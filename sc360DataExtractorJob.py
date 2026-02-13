from __future__ import annotations

import io
import json
import logging
import os
import re
import time
import zipfile
from datetime import datetime, timedelta, timezone
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, Optional, Tuple, List, Any
from urllib.parse import quote

import boto3
import psycopg2
from botocore.exceptions import BotoCoreError, ClientError

# -------------------------
# Logging
# -------------------------
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# -------------------------
# Utility: load per-environment JSON env vars and resolve values
# -------------------------
def _load_json_env_var(key: str) -> Any:
    raw = os.environ.get(key)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return raw


def _normalize_env_folder(env_name: str) -> str:
    if not env_name:
        return "Dev"
    n = env_name.lower()
    return "DEV" if n == "dev" else n.capitalize()


def resolve_env_for_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simple scalar-only resolver for single-env Lambdas.

    Expected environment variables (scalars):
      - DEFAULT_SENDER (e.g. "SC360 File Extractor <noreply@...>")
      - DEFAULT_S3_PATH (either "s3://bucket/path/" OR a bare bucket name like "sc360-data-extraction-bucket")
      - REDSHIFT_SECRET_NAME (e.g. "redshift_secrets_with_kms")
      - RETENTION_BUCKET (either "s3://bucket/prefix/" OR bare bucket name)

    Behavior:
      - If DEFAULT_S3_PATH is a bare bucket name it becomes "s3://{bucket}/{EnvFolder}/"
      - If RETENTION_BUCKET is a bare bucket name, retention prefix becomes "{EnvFolder}/"
      - env_folder derives from event["environment"] or ENVIRONMENT env var (defaults to "dev")
    """
    env_name = event.get("environment") or os.environ.get("ENVIRONMENT")
    env_folder = _normalize_env_folder(env_name)

    # Scalars only (no JSON parsing)
    default_sender = os.environ.get("DEFAULT_SENDER", "SC360 File Extractor <noreply@notification.sc360-DEV.arubanetworks.com>")
    raw_s3 = os.environ.get("DEFAULT_S3_PATH", "") or ""
    default_s3_path = ""

    if raw_s3:
        s = raw_s3.strip()
        if s.startswith("s3://"):
            default_s3_path = s if s.endswith("/") else s + "/"
        elif re.fullmatch(r"[a-z0-9.\-]+", s, flags=re.IGNORECASE):
            # bare bucket name -> build s3 path with env folder
            default_s3_path = f"s3://{s}/{env_folder}/"
        else:
            default_s3_path = s if s.endswith("/") else s + "/"

    redshift_secret = os.environ.get("REDSHIFT_SECRET_NAME", "redshift_secrets_with_kms")

    raw_ret_bucket = os.environ.get("RETENTION_BUCKET", "")
    retention_bucket = None
    retention_prefix = None
    if raw_ret_bucket:
        s = raw_ret_bucket.strip()
        if s.startswith("s3://"):
            try:
                b, p = parse_s3_path(s)
                retention_bucket = b
                retention_prefix = p
            except Exception:
                retention_bucket = s
                retention_prefix = None
        elif "/" in s:
            # treat "bucket/prefix" -> bucket and prefix
            parts = s.split("/", 1)
            retention_bucket = parts[0]
            retention_prefix = (parts[1] + ("" if parts[1].endswith("/") else "/")) if len(parts) > 1 else f"{env_folder}/"
        else:
            # bare bucket name
            retention_bucket = s
            retention_prefix = f"{env_folder}/"

    resolved = {
        "environment": env_name,
        "env_folder": env_folder,
        "DEFAULT_SENDER": default_sender,
        "DEFAULT_S3_PATH": default_s3_path,
        "REDSHIFT_SECRET_NAME": redshift_secret,
        "RETENTION_BUCKET": retention_bucket,
        "RETENTION_PREFIX": retention_prefix,
    }

    # safe debug print (do not print secrets)
    print("Variables:", {k: v for k, v in resolved.items() if k != "REDSHIFT_SECRET_NAME"})
    return resolved

# -------------------------
# Configuration defaults (non-mapped fallbacks)
# -------------------------
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
DEFAULT_RECIPIENT_ENV = os.environ.get("DEFAULT_RECIPIENT", "lokeswara.reddy-ext@hpe.com")
DEFAULT_IAM_ROLE_ARN_ENV = os.environ.get("DEFAULT_IAM_ROLE_ARN", "arn:aws:iam::105186932710:role/SC360-Redshift-S3-Role")
MAX_EMAIL_ATTACHMENT_SIZE_BYTES = int(os.environ.get("MAX_EMAIL_ATTACHMENT_SIZE_BYTES", 10 * 1024 * 1024))
RETENTION_PREFIX_ENV = os.environ.get("RETENTION_PREFIX")  # optional
RETENTION_DAYS_ENV = int(os.environ.get("RETENTION_DAYS", 30))

# -------------------------
# Helpers (S3, Redshift, email, merging, etc.)
# -------------------------


def parse_s3_path(s3_path: str) -> Tuple[str, str]:
    if not s3_path:
        raise ValueError("s3_path is empty")
    m = re.match(r"^s3://([^/]+)/(.*)$", s3_path)
    if not m:
        raise ValueError(f"Invalid S3 path: {s3_path}")
    bucket = m.group(1)
    prefix = m.group(2)
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"
    return bucket, prefix


def choose_incremented_prefix_if_missing(s3_client: "boto3.client", provided_path: Optional[str], default_s3_path: str) -> Tuple[str, str]:
    """
    Return (bucket, prefix) for the UNLOAD target.

    Behavior:
    - If provided_path is given, parse and return it.
    - Otherwise, parse and return default_s3_path exactly as provided (no auto-increment).
      This ensures files always land under the configured folder (e.g. DEV/).
    """
    if provided_path:
        return parse_s3_path(provided_path)

    if not default_s3_path:
        raise ValueError("No s3_path provided and DEFAULT_S3_PATH is not set")

    bucket, base_prefix = parse_s3_path(default_s3_path)
    logger.info("Using DEFAULT_S3_PATH without increment: s3://%s/%s", bucket, base_prefix)
    return bucket, base_prefix

def build_unload_sql(query: str, s3_bucket: str, s3_prefix: str, object_basename_no_ext: str,
                     file_format: str, iam_role_arn: str) -> str:
    if not query or not query.strip().lower().startswith("select"):
        raise ValueError("query must be a SELECT statement")
    fmt = file_format.strip().upper()
    if fmt not in ("CSV", "PARQUET"):
        raise ValueError("Unsupported file_format: %s" % file_format)

    target = f"s3://{s3_bucket}/{s3_prefix}{object_basename_no_ext}"
    escaped_query = query.replace("'", "''")
    if fmt == "CSV":
        format_clause = "FORMAT AS CSV\nHEADER\nDELIMITER ','"
    else:
        format_clause = "FORMAT AS PARQUET"

    unload_sql = (
        "UNLOAD ('{query}')\n"
        "TO '{target}'\n"
        "IAM_ROLE '{iam_role}'\n"
        f"{format_clause}\n"
        "PARALLEL OFF\n"
        "ALLOWOVERWRITE;"
    ).format(query=escaped_query, target=target, iam_role=iam_role_arn)
    logger.debug("Built UNLOAD SQL: %s", unload_sql.replace("\n", " | "))
    return unload_sql


def get_db_connection(secret_name: str) -> "psycopg2.extensions.connection":
    try:
        sm = boto3.client("secretsmanager", region_name=AWS_REGION)
        resp = sm.get_secret_value(SecretId=secret_name)
        secret_str = resp.get("SecretString", "")
        if not secret_str:
            raise RuntimeError("SecretString empty")
        try:
            creds = json.loads(secret_str)
        except json.JSONDecodeError:
            creds = eval(secret_str)  # fallback
        dbname = creds.get("redshift_database") or creds.get("database") or creds.get("engine")
        host = creds.get("redshift_host") or creds.get("host")
        port = creds.get("redshift_port") or creds.get("port")
        user = creds.get("redshift_username") or creds.get("username")
        password = creds.get("redshift_password") or creds.get("password")
        if not all((dbname, host, port, user, password)):
            raise RuntimeError("Missing DB fields in secret")
        conn_string = f"dbname='{dbname}' port='{port}' user='{user}' password='{password}' host='{host}'"
        conn = psycopg2.connect(conn_string)
        logger.info("Connected to DB host=%s db=%s", host, dbname)
        return conn
    except (ClientError, BotoCoreError):
        logger.exception("Secrets Manager error")
        raise RuntimeError("Secrets retrieval error")
    except psycopg2.Error:
        logger.exception("DB connection error")
        raise RuntimeError("Database connection error")


def run_unload_statement(connection: "psycopg2.extensions.connection", unload_sql: str) -> None:
    try:
        with connection:
            with connection.cursor() as cur:
                logger.info("Executing UNLOAD")
                logger.debug("UNLOAD SQL: %s", unload_sql)
                cur.execute(unload_sql)
                logger.info("UNLOAD executed")
    except psycopg2.Error:
        logger.exception("UNLOAD execution failed")
        raise RuntimeError("UNLOAD execution error")


def _read_manifest_and_get_keys(s3_client: "boto3.client", bucket: str, manifest_key: str) -> List[str]:
    try:
        resp = s3_client.get_object(Bucket=bucket, Key=manifest_key)
        mj = json.loads(resp["Body"].read())
        entries = mj.get("entries") or mj.get("files") or []
        keys: List[str] = []
        for e in entries:
            if isinstance(e, dict):
                url = e.get("url") or e.get("uri")
            else:
                url = e if isinstance(e, str) else None
            if url and url.startswith("s3://"):
                parts = url[5:].split("/", 1)
                if len(parts) == 2:
                    keys.append(parts[1])
        return keys
    except Exception:
        logger.exception("Failed to read manifest %s", manifest_key)
        return []


def list_unload_part_keys(s3_client: "boto3.client", bucket: str, prefix: str,
                          object_basename_no_ext: str) -> List[Tuple[str, int]]:
    expected_prefix = f"{prefix}{object_basename_no_ext}"
    manifest_key = f"{expected_prefix}.manifest"
    try:
        s3_client.head_object(Bucket=bucket, Key=manifest_key)
        logger.info("Manifest found at %s; reading entries", manifest_key)
        manifest_keys = _read_manifest_and_get_keys(s3_client, bucket, manifest_key)
        results: List[Tuple[str, int]] = []
        for k in manifest_keys:
            try:
                h = s3_client.head_object(Bucket=bucket, Key=k)
                size = int(h.get("ContentLength", 0))
                if size > 0:
                    results.append((k, size))
            except Exception:
                logger.warning("Manifest referenced key not accessible: %s", k, exc_info=True)
        return results
    except ClientError:
        continuation = None
        parts: List[Tuple[str, int]] = []
        try:
            while True:
                kwargs = {"Bucket": bucket, "Prefix": expected_prefix}
                if continuation:
                    kwargs["ContinuationToken"] = continuation
                resp = s3_client.list_objects_v2(**kwargs)
                for obj in resp.get("Contents", []) or []:
                    key = obj.get("Key")
                    size = int(obj.get("Size", 0))
                    if key and size > 0 and key.startswith(expected_prefix):
                        parts.append((key, size))
                if resp.get("IsTruncated"):
                    continuation = resp.get("NextContinuationToken")
                else:
                    break
        except (ClientError, BotoCoreError):
            logger.exception("S3 listing error")
            raise RuntimeError("S3 listing error")
        parts.sort(key=lambda x: x[0])
        return parts


def merge_csv_parts(s3_client: "boto3.client", bucket: str, keys: List[str]) -> bytes:
    if not keys:
        return b""
    lines_acc: List[str] = []
    first = True
    for key in keys:
        logger.debug("Downloading part for merge: s3://%s/%s", bucket, key)
        resp = s3_client.get_object(Bucket=bucket, Key=key)
        raw = resp["Body"].read()
        text = raw.decode("utf-8", errors="replace")
        part_lines = text.splitlines()
        if first:
            lines_acc.extend(part_lines)
            first = False
        else:
            if part_lines:
                lines_acc.extend(part_lines[1:])
    merged_text = "\n".join(lines_acc) + "\n"
    return merged_text.encode("utf-8")


def upload_merged_bytes(s3_client: "boto3.client", bucket: str, dest_key: str, data: bytes) -> None:
    logger.info("Uploading merged result to s3://%s/%s (size=%s)", bucket, dest_key, len(data))
    s3_client.put_object(Bucket=bucket, Key=dest_key, Body=data)


def _rank_candidates(candidates: List[dict], canonical_ext: str) -> List[dict]:
    def score(c: dict) -> int:
        k = c["Key"]
        if k.endswith(canonical_ext):
            return 0
        if canonical_ext in k:
            return 1
        return 2
    return sorted(candidates, key=score)


def find_unload_object_key(s3_client: "boto3.client", bucket: str, prefix: str,
                           object_basename_no_ext: str, file_format: str, timeout_seconds: int = 300) -> Tuple[str, int]:
    expected_prefix = f"{prefix}{object_basename_no_ext}"
    canonical_ext = ".csv" if file_format.strip().upper() == "CSV" else ".parquet"
    deadline = time.time() + timeout_seconds
    logger.debug("Looking for objects starting with: %s", expected_prefix)
    try:
        while time.time() < deadline:
            continuation_token = None
            candidates: List[dict] = []
            while True:
                list_kwargs = {"Bucket": bucket, "Prefix": expected_prefix}
                if continuation_token:
                    list_kwargs["ContinuationToken"] = continuation_token
                resp = s3_client.list_objects_v2(**list_kwargs)
                for obj in resp.get("Contents", []) or []:
                    key = obj.get("Key")
                    size = obj.get("Size", 0)
                    if key and size and key.startswith(expected_prefix):
                        candidates.append({"Key": key, "Size": size})
                if resp.get("IsTruncated"):
                    continuation_token = resp.get("NextContinuationToken")
                else:
                    break

            if candidates:
                ranked = _rank_candidates(candidates, canonical_ext)
                chosen = ranked[0]
                logger.info("Selected representative unload file: s3://%s/%s (size=%s)", bucket, chosen["Key"], chosen["Size"])
                return chosen["Key"], int(chosen["Size"])

            # manifest fallback
            manifest_key = f"{expected_prefix}.manifest"
            try:
                s3_client.head_object(Bucket=bucket, Key=manifest_key)
                logger.info("Found manifest at s3://%s/%s", bucket, manifest_key)
                keys = _read_manifest_and_get_keys(s3_client, bucket, manifest_key)
                for k in keys:
                    try:
                        h = s3_client.head_object(Bucket=bucket, Key=k)
                        if h.get("ContentLength", 0) > 0:
                            return k, int(h.get("ContentLength", 0))
                    except Exception:
                        logger.warning("Manifest referenced key not available: %s", k, exc_info=True)
            except ClientError:
                pass

            logger.debug("No unload output yet; sleeping 3s")
            time.sleep(3)
        raise TimeoutError(f"Timed out waiting for UNLOAD output at s3://{bucket}/{expected_prefix} after {timeout_seconds}s")
    except (ClientError, BotoCoreError):
        logger.exception("S3 error while searching for unload file")
        raise RuntimeError("S3 error")


def get_s3_object_bytes(s3_client: "boto3.client", bucket: str, key: str) -> bytes:
    try:
        resp = s3_client.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()
    except (ClientError, BotoCoreError):
        logger.exception("Failed to download s3://%s/%s", bucket, key)
        raise RuntimeError("S3 download error")


def create_zip_bytes(filename_in_zip: str, file_bytes: bytes) -> Tuple[str, bytes]:
    zip_name = f"{filename_in_zip}.zip"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(filename_in_zip, file_bytes)
    buf.seek(0)
    return zip_name, buf.read()


def s3_console_link(bucket: str, key: str) -> str:
    """
    Build a console link that points to the folder/key prefix. The prefix is URL-encoded
    to avoid email-gateway mangling (UrlDefense/Proofpoint and similar).
    We use the /s3/buckets/<bucket> view with a prefix query so the console shows the folder.
    """
    # ensure no leading slash
    if key.startswith("/"):
        key = key[1:]
    # encode the prefix so slashes become %2F etc and email gateways won't mangle them
    encoded = quote(key, safe="")
    return f"https://s3.console.aws.amazon.com/s3/buckets/{bucket}?region={AWS_REGION}&prefix={encoded}&showversions=false"


def _parse_recipients(raw_default: str, event_additional: Optional[str]) -> List[str]:
    result: List[str] = []
    if raw_default:
        for p in raw_default.split(","):
            p = p.strip()
            if p:
                result.append(p)
    if event_additional:
        for p in event_additional.split(","):
            p = p.strip()
            if p:
                result.append(p)
    # unique preserve order
    seen = set()
    dedup = []
    for r in result:
        if r not in seen:
            seen.add(r)
            dedup.append(r)
    return dedup


def send_email_v2(sender: str, recipients: List[str], subject: str, body_text: str, body_html: Optional[str] = None) -> None:
    client = boto3.client("sesv2", region_name=AWS_REGION)
    try:
        content = {"Simple": {"Subject": {"Data": subject}, "Body": {"Text": {"Data": body_text}}}}
        if body_html:
            content["Simple"]["Body"]["Html"] = {"Data": body_html}
        client.send_email(FromEmailAddress=sender, Destination={"ToAddresses": recipients}, Content=content)
        logger.info("SESv2 sent email to %s", recipients)
    except (ClientError, BotoCoreError):
        logger.exception("SESv2 send_email failed")
        raise RuntimeError("Email send failed (SESv2)")


def send_email_with_attachment_via_raw(sender: str, recipients: List[str], subject: str, body_text: str,
                                       body_html: Optional[str], attachment_name: str, attachment_bytes: bytes) -> None:
    ses = boto3.client("ses", region_name=AWS_REGION)
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(body_text, "plain"))
    if body_html:
        alt.attach(MIMEText(body_html, "html"))
    msg.attach(alt)

    part = MIMEApplication(attachment_bytes)
    part.add_header("Content-Disposition", "attachment", filename=attachment_name)
    msg.attach(part)

    try:
        ses.send_raw_email(Source=sender, Destinations=recipients, RawMessage={"Data": msg.as_string()})
        logger.info("SES raw email (with attachment) sent to %s", recipients)
    except (ClientError, BotoCoreError):
        logger.exception("SES send_raw_email failed")
        raise RuntimeError("Email send failed (SES raw)")


def delete_objects_older_than(s3_client: "boto3.client", bucket: str, prefix: Optional[str], days: int) -> Dict:
    from datetime import datetime, timezone, timedelta
    from botocore.exceptions import ClientError

    if prefix is not None:
        prefix = str(prefix).strip()
        if (prefix.startswith('"') and prefix.endswith('"')) or (prefix.startswith("'") and prefix.endswith("'")):
            prefix = prefix[1:-1].strip()
        if prefix.lower() in ("", "none", "null"):
            prefix = None

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    logger.info("Deleting objects older than %s under s3://%s/%s", cutoff.isoformat(), bucket, prefix or "")

    paginator = s3_client.get_paginator("list_objects_v2")
    page_kwargs = {"Bucket": bucket}
    if prefix:
        page_kwargs["Prefix"] = prefix

    scanned = 0
    candidates: List[Dict[str, int]] = []
    bytes_total = 0

    try:
        for page in paginator.paginate(**page_kwargs):
            for obj in page.get("Contents", []) or []:
                scanned += 1
                key = obj.get("Key")
                lm = obj.get("LastModified")
                size = int(obj.get("Size", 0))
                if not lm:
                    continue
                if lm < cutoff:
                    candidates.append({"Key": key, "Size": size})
                    bytes_total += size
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        msg = exc.response.get("Error", {}).get("Message", str(exc))
        raise RuntimeError(f"Failed listing objects in s3://{bucket}/{prefix or ''}: {code} - {msg}") from exc

    if not candidates:
        logger.info("No objects older than cutoff found (scanned=%d).", scanned)
        return {
            "status": "NO_CANDIDATES",
            "scanned": scanned,
            "candidates": 0,
            "deleted_count": 0,
            "bytes_freed": 0,
        }

    deleted_count = 0
    bytes_freed = 0
    try:
        for i in range(0, len(candidates), 1000):
            batch = candidates[i : i + 1000]
            delete_payload = {"Objects": [{"Key": c["Key"]} for c in batch], "Quiet": False}
            resp = s3_client.delete_objects(Bucket=bucket, Delete=delete_payload)
            deleted = resp.get("Deleted", []) or []
            errors = resp.get("Errors", []) or []
            deleted_count += len(deleted)
            bytes_freed += sum(c["Size"] for c in batch)
            if errors:
                raise RuntimeError({"message": "Errors returned from delete_objects", "errors": errors, "batch": [c["Key"] for c in batch]})
            logger.info("Deleted batch: requested=%d deleted=%d", len(batch), len(deleted))
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        msg = exc.response.get("Error", {}).get("Message", str(exc))
        raise RuntimeError(f"S3 delete_objects failed in s3://{bucket}/{prefix or ''}: {code} - {msg}") from exc

    logger.info("Deletion completed: scanned=%d candidates=%d deleted=%d bytes_freed=%d",
                scanned, len(candidates), deleted_count, bytes_freed)

    return {
        "status": "DELETED",
        "scanned": scanned,
        "candidates": len(candidates),
        "deleted_count": deleted_count,
        "bytes_freed": bytes_freed,
    }


# -------------------------
# Lambda entrypoint
# -------------------------


def lambda_handler(event: Dict, context) -> Dict:
    try:
        query = event.get("query")
        if not query:
            raise ValueError("'query' is required in the event")

        file_format = str(event.get("file_format", "CSV")).strip().upper()
        if file_format not in ("CSV", "PARQUET"):
            raise ValueError("file_format must be 'CSV' or 'PARQUET'")

        # Resolve env config
        env_cfg = resolve_env_for_event(event)
        default_sender = env_cfg.get("DEFAULT_SENDER")
        default_s3_path = env_cfg.get("DEFAULT_S3_PATH")
        default_redshift_secret = env_cfg.get("REDSHIFT_SECRET_NAME")
        default_retention_bucket = env_cfg.get("RETENTION_BUCKET")
        default_retention_prefix = env_cfg.get("RETENTION_PREFIX")
        env_folder = env_cfg.get("env_folder")

        # Always use configured DEFAULT_S3_PATH; ignore any s3_path in the event
        provided_s3_path = None  # intentionally ignore event s3_path per new requirement

        # recipients: parse default and append any comma-separated recipients passed in event key "recipient_emails"
        event_recipients_raw = event.get("recipient_emails") or event.get("additional_recipients") or event.get("recipient_email")
        recipients_list = _parse_recipients(DEFAULT_RECIPIENT_ENV, event_recipients_raw)
        if not recipients_list:
            raise ValueError("No recipient email addresses configured or provided")

        # other overrides
        redshift_secret_name = event.get("redshift_secret_name") or default_redshift_secret or os.environ.get("REDSHIFT_SECRET_NAME", "redshift_secrets_with_kms")
        sender = event.get("sender") or default_sender
        iam_role = event.get("iam_role_arn") or DEFAULT_IAM_ROLE_ARN_ENV

        need_email_attachment = str(event.get("need_email_attachment", "N")).strip().upper()
        timeout_seconds = int(event.get("s3_wait_timeout_seconds", 300))

        # Retention config: if resolve_env_for_event provided only a bucket base, use its prefix; else fallbacks
        retention_bucket = default_retention_bucket or os.environ.get("RETENTION_BUCKET")
        retention_prefix = default_retention_prefix or RETENTION_PREFIX_ENV or f"{env_folder}/"
        retention_days = int(event.get("retention_days") or RETENTION_DAYS_ENV)

        s3_client = boto3.client("s3", region_name=AWS_REGION)

        # Use default_s3_path (constructed per env). We no longer accept event s3_path.
        bucket, base_prefix = choose_incremented_prefix_if_missing(s3_client, provided_s3_path, default_s3_path)

        # Use UTC timestamp for folder names and object basename (yr=/month=/day= format)
        now = datetime.utcnow()
        year = now.strftime("%Y")        # "2026"
        month = now.strftime("%m")       # "01"
        day = now.strftime("%d")         # "08"

        m = re.search(r"from\s+([^\s;]+)", query, flags=re.IGNORECASE)
        table_name = m.group(1).split(".")[-1] if m else "extract"

        # Add a per-run subfolder (tablename_HHMMSS) so multiple runs don't collide and duplicates are separated
        object_subfolder = f"{table_name}_{now.strftime('%H%M%S')}"
        # Files land under <base_prefix>/<table_name>/yr=YYYY/month=MM/day=DD/<table_name_HHMMSS>/
        # (base_prefix is expected to already end with a slash)
        target_prefix = f"{base_prefix}{table_name}/yr={year}/month={month}/day={day}/{object_subfolder}/"
        object_basename_no_ext = f"{table_name}_{now.strftime('%Y%m%dT%H%M%SZ')}"

        logger.info("Will UNLOAD to s3://%s/%s (table folder: %s) using secret: %s", bucket, target_prefix, table_name, redshift_secret_name)

        unload_sql = build_unload_sql(query=query, s3_bucket=bucket, s3_prefix=target_prefix,
                                      object_basename_no_ext=object_basename_no_ext, file_format=file_format,
                                      iam_role_arn=iam_role)

        # Run UNLOAD
        conn = get_db_connection(redshift_secret_name)
        try:
            conn.autocommit = True
            run_unload_statement(conn, unload_sql)
        finally:
            try:
                conn.close()
            except Exception:
                logger.warning("Error closing DB connection", exc_info=True)

        # Collect parts
        parts = list_unload_part_keys(s3_client, bucket, target_prefix, object_basename_no_ext)
        if not parts:
            raise RuntimeError("No unload output found in S3")

        total_size = sum(sz for (_k, sz) in parts)
        keys = [k for (k, _sz) in parts]
        logger.info("Found %d part(s) for unload. Total size: %s bytes", len(parts), total_size)

        env_name = event.get("environment") or os.environ.get("ENVIRONMENT")
        subject = f"[{env_name.upper()}]: SC360 File Extractor: {table_name} ({file_format})"
        timestamp_str = now.strftime("%Y%m%dT%H%M%SZ")
        common_lines = [
            f"Your requested extract has completed.",
            f"Query target/table: {table_name}",
            f"Format: {file_format}",
            f"Timestamp (UTC): {timestamp_str}",
            "",
        ]

        # Instead of listing individual file keys, report the subfolder path only so duplicates are separated by run folder.
        folder_console = s3_console_link(bucket, target_prefix)
        folder_text = f"s3://{bucket}/{target_prefix}"
        links_html: List[str] = [f'<li><a href="{folder_console}">{target_prefix}</a></li>']
        links_text: List[str] = [folder_text]

        # CSV multi-part merge logic and rest of behavior unchanged (but email/reporting will reference the folder only)
        normalized_key = f"{target_prefix}{object_basename_no_ext}.csv"
        rep_key = parts[0][0]
        rep_size = parts[0][1]

        # If multiple CSV parts and small enough, merge into a single CSV inside the same run subfolder and update representative key.
        if file_format == "CSV" and len(parts) > 1:
            if total_size <= MAX_EMAIL_ATTACHMENT_SIZE_BYTES:
                logger.info("Merging %d CSV parts (total %s bytes) into single file", len(parts), total_size)
                merged_bytes = merge_csv_parts(s3_client, bucket, keys)
                upload_merged_bytes(s3_client, bucket, normalized_key, merged_bytes)
                rep_key = normalized_key
                total_size = len(merged_bytes)
                # still keep folder-only links in email
            else:
                size_mb = total_size / (1024 * 1024)
                body_text_lines = common_lines + [
                    f"Found {len(parts)} part files for this extract. Total size: {size_mb:.2f} MB",
                    "",
                    f"The combined file is larger than the email attachment threshold ({MAX_EMAIL_ATTACHMENT_SIZE_BYTES / (1024*1024):.0f} MB).",
                    "Please download the files from the S3 folder below:",
                    "",
                ] + links_text
                body_text = "\n".join(body_text_lines)
                body_html = "<p>" + "</p><p>".join(common_lines) + "</p>"
                body_html += "<p>The combined file is larger than the email attachment threshold. Files are in:</p><ul>"
                body_html += "".join(links_html)
                body_html += "</ul>"
                # Single email containing the folder link
                send_email_v2(sender, recipients_list, subject, body_text, body_html)

                if retention_bucket:
                    try:
                        cleanup_result = delete_objects_older_than(s3_client, retention_bucket, retention_prefix, retention_days)
                        logger.info("Retention cleanup after notification: %s", cleanup_result)
                    except Exception:
                        logger.exception("Retention cleanup failed after sending links")

                return {
                    "status": "SUCCESS",
                    "s3_path": folder_text,
                    "file_size_bytes": total_size,
                    "emailed": True,
                    "attachment": None,
                    "note": "Multiple part files; too large to merge/attach. S3 folder link provided."
                }

        # If single CSV part but not ending with .csv, try to normalize key (still inside same subfolder)
        if file_format == "CSV" and len(parts) == 1:
            rep_key = parts[0][0]
            if not rep_key.endswith(".csv"):
                try:
                    logger.info("Normalizing CSV key to end with .csv: %s -> %s", rep_key, normalized_key)
                    copy_source = {"Bucket": bucket, "Key": rep_key}
                    s3_client.copy_object(Bucket=bucket, CopySource=copy_source, Key=normalized_key)
                    s3_client.delete_object(Bucket=bucket, Key=rep_key)
                    rep_key = normalized_key
                    head = s3_client.head_object(Bucket=bucket, Key=rep_key)
                    total_size = int(head.get("ContentLength", 0))
                    logger.info("Normalized key created: s3://%s/%s size=%s", bucket, rep_key, total_size)
                except Exception:
                    logger.warning("Failed to normalize CSV key, continuing with original key", exc_info=True)

        # If the final single-file size is too large to attach, send a single email with folder link
        if total_size > MAX_EMAIL_ATTACHMENT_SIZE_BYTES:
            size_mb = total_size / (1024 * 1024)
            body_text_lines = common_lines + [
                f"File size: {size_mb:.2f} MB",
                "",
                f"The file exceeds the maximum email attachment threshold ({MAX_EMAIL_ATTACHMENT_SIZE_BYTES / (1024*1024):.0f} MB).",
                "Please download the file(s) from the S3 folder below:",
                "",
            ] + links_text
            body_text = "\n".join(body_text_lines)
            body_html = "<p>" + "</p><p>".join(common_lines) + "</p>"
            body_html += "<p>The file exceeds the maximum email attachment threshold. Download from:</p><ul>"
            body_html += "".join(links_html)
            body_html += "</ul>"
            send_email_v2(sender, recipients_list, subject, body_text, body_html)

            if retention_bucket:
                try:
                    cleanup_result = delete_objects_older_than(s3_client, retention_bucket, retention_prefix, retention_days)
                    logger.info("Retention cleanup after notification: %s", cleanup_result)
                except Exception:
                    logger.exception("Retention cleanup failed after sending links")

            return {
                "status": "SUCCESS",
                "s3_path": folder_text,
                "file_size_bytes": total_size,
                "emailed": True,
                "attachment": None,
                "note": "File too large for email; S3 folder link provided."
            }

        # Small-enough file: optionally attach and send single email; still reference folder in body
        body_text = "\n".join(common_lines + [f"File size: {total_size / (1024*1024):.2f} MB", ""] + links_text)
        body_html = "<p>" + "</p><p>".join(common_lines) + "</p>"
        body_html += "<p>Download folder:</p><ul>" + "".join(links_html) + "</ul>"

        if need_email_attachment == "Y":
            file_bytes = get_s3_object_bytes(s3_client, bucket, rep_key)
            ext = ".csv" if file_format == "CSV" else ".parquet"
            filename_in_zip = f"{table_name}{ext}"
            zip_name, zip_bytes = create_zip_bytes(filename_in_zip, file_bytes)
            send_email_with_attachment_via_raw(sender, recipients_list, subject, body_text, body_html, zip_name, zip_bytes)
            if retention_bucket:
                try:
                    cleanup_result = delete_objects_older_than(s3_client, retention_bucket, retention_prefix, retention_days)
                    logger.info("Retention cleanup after attachment send: %s", cleanup_result)
                except Exception:
                    logger.exception("Retention cleanup failed after send")
            return {
                "status": "SUCCESS",
                "s3_path": folder_text,
                "file_size_bytes": total_size,
                "emailed": True,
                "attachment": zip_name
            }

        # Send notification (single email) referencing the run subfolder
        send_email_v2(sender, recipients_list, subject, body_text, body_html)
        if retention_bucket:
            try:
                cleanup_result = delete_objects_older_than(s3_client, retention_bucket, retention_prefix, retention_days)
                logger.info("Retention cleanup after notification: %s", cleanup_result)
            except Exception:
                logger.exception("Retention cleanup failed after send")
        return {
            "status": "SUCCESS",
            "s3_path": folder_text,
            "file_size_bytes": total_size,
            "emailed": True,
            "attachment": None
        }

    except (ValueError, RuntimeError, TimeoutError) as exc:
        logger.exception("Job failed")
        return {"status": "FAILED", "reason": str(exc)}
    except Exception:
        logger.exception("Unexpected error")
        return {"status": "FAILED", "reason": "Unexpected error occurred"}