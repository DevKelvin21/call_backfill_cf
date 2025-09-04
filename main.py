# main.py
import os
import io
import csv
import json
import re
import hashlib
import logging
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from google.cloud import storage, bigquery

# ---------- Env ----------
BUCKET_PREFIX = os.getenv("GCS_INPUT_PREFIX", "call-exports/")  # folder within the bucket to process
SOURCE_TZ = os.getenv("SOURCE_TZ", "UTC-7")                     # fallback tz if row offset not present
TIME_TOL_SEC = int(os.getenv("TIME_TOL_SEC", "60"))

STAGING_TABLE  = os.getenv("STAGING_BQ_TABLE",  "dev-at-cf.at_dials_stage.at_dials_stage")
CLEAN_TABLE    = os.getenv("CLEAN_BQ_TABLE",    "dev-at-cf.at_dials.at_dials_cleaned")
EXISTING_TABLE = os.getenv("EXISTING_BQ_TABLE", "dev-at-cf.at_dials.at_dials_vici_cf_bq")

TMP_PREFIX = os.getenv("GCS_OUTPUT_PREFIX", "tmp/normalized/")  # where NDJSON lands

# ---------- Clients & Logger ----------
storage_client = storage.Client()
bq = bigquery.Client()
log = logging.getLogger("call-dedup-loader")

# ---------- Lead ID header candidates ----------
LEAD_KEYS = ["lead_id", "vendor_lead_code", "LeadID"]

# ---------- Date formats ----------
DT_FORMATS = [
    "%Y-%m-%d %H:%M:%S",  # 2025-08-23 15:08:58
    "%Y-%m-%d %H:%M",
    "%m/%d/%Y %H:%M:%S",  # 09/02/2025 11:02:00
    "%m/%d/%Y %H:%M",
    "%m/%d/%y %H:%M:%S",  # 9/2/25 11:02:00
    "%m/%d/%y %H:%M",     # 9/2/25 11:02   <-- your sample
    "%m/%d/%y %I:%M %p",  # 9/2/25 11:02 AM
    "%m/%d/%Y %I:%M %p",  # 09/02/2025 11:02 AM
]

UTC = timezone.utc

# ---------- TZ helpers ----------
def resolve_tz(tz_str: str):
    """
    Returns a tzinfo from an env string.
    Supports named zones (e.g., 'America/Denver') and fixed offsets like 'UTC-7'.
    """
    s = (tz_str or "").strip()
    if s.upper().startswith("UTC") and any(sign in s for sign in ("+", "-")):
        try:
            hours = int(s.replace("UTC", ""))
            return timezone(timedelta(hours=hours))
        except Exception:
            return UTC
    try:
        return ZoneInfo(s)
    except Exception:
        return UTC

SRC_TZ_OBJ = resolve_tz(SOURCE_TZ)

def tz_from_row_offset(offset_str: str):
    """gmt_offset_now like '-4' -> UTC-4 tz; returns None if invalid."""
    try:
        hours = int(str(offset_str).strip())
        return timezone(timedelta(hours=hours))
    except Exception:
        return None

# ---------- Header & field helpers ----------
def normalize_headers(fieldnames):
    """
    Map {clean_lower_name -> original_header}. Cleans BOM and NBSP.
    """
    def clean(h):
        if h is None:
            return ""
        return str(h).replace("\ufeff", "").replace("\xa0", " ").strip().lower()
    return { clean(h): h for h in (fieldnames or []) }

def get_val(row: dict, header_map: dict, *candidates):
    """
    Fetch first present value among candidate header names (case/space/BOM-insensitive).
    """
    for cand in candidates:
        key = str(cand).replace("\ufeff", "").replace("\xa0", " ").strip().lower()
        orig = header_map.get(key)
        if orig is not None:
            val = row.get(orig)
            if val is not None:
                return val
    return None

def safe_str(x):
    return None if x is None else (str(x).strip() or None)

def normalize_phone(val):
    if val is None:
        return None
    digits = re.sub(r"\D+", "", str(val))
    return digits or None

def pick_lead_id(row: dict, header_map: dict):
    for k in LEAD_KEYS:
        v = get_val(row, header_map, k)
        if v:
            return str(v).strip()
    return safe_str(get_val(row, header_map, "vendor_lead_code", "lead_id"))

# ---------- Date parsing ----------
def parse_date_local(s: str, tzinfo_override=None):
    """
    Returns (original_text, utc_datetime) or None if not parseable.
    Uses tzinfo_override if provided, else SOURCE_TZ env.
    """
    if s is None:
        return None
    txt = str(s).strip()
    if not txt:
        return None

    for fmt in DT_FORMATS:
        try:
            dt_naive = datetime.strptime(txt, fmt)
            tzinfo = tzinfo_override or SRC_TZ_OBJ
            dt_local = dt_naive.replace(tzinfo=tzinfo)
            return txt, dt_local.astimezone(UTC)
        except ValueError:
            print(f"Date parse failed for '{txt}' with format '{fmt}'")
            continue
    return None

# ---------- BigQuery helpers ----------
def load_to_staging_from_ndjson(gcs_uri: str):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    bq.load_table_from_uri(gcs_uri, STAGING_TABLE, job_config=job_config).result()

def run_merge(time_tol_sec: int):
    sql_path = os.path.join(os.path.dirname(__file__), "merge.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    job_config = bigquery.QueryJobConfig(
        use_legacy_sql=False,  # <-- critical
        query_parameters=[ bigquery.ScalarQueryParameter("time_tol", "INT64", time_tol_sec) ],
    )
    bq.query(sql, job_config=job_config).result()


# ---------- Entry point ----------
def main(event, context):
    """
    Trigger: GCS object.finalize on bucket (e.g., gs://call-exports)
    Processes only objects whose name starts with BUCKET_PREFIX (default: 'call-exports/').
    """
    bucket_name = event["bucket"]
    object_name = event["name"]

    # Filter by prefix to avoid processing unrelated uploads
    if not object_name.startswith(BUCKET_PREFIX):
        return

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    source_file = f"gs://{bucket_name}/{object_name}"

    # BOM-aware read to strip \ufeff from the first header
    content = blob.download_as_text(encoding="utf-8-sig")
    reader = csv.DictReader(io.StringIO(content), skipinitialspace=True)

    header_map = normalize_headers(reader.fieldnames)

    tmp_key = f"{TMP_PREFIX}{object_name}.ndjson"
    tmp_blob = bucket.blob(tmp_key)
    buf = io.StringIO()

    total = 0
    good = 0
    bad = 0

    for row in reader:
        total += 1

        # Row-specific timezone if provided by the export
        row_tz = tz_from_row_offset(get_val(row, header_map, "gmt_offset_now"))
        dt_parsed = parse_date_local(get_val(row, header_map, "call_date"), tzinfo_override=row_tz)
        if not dt_parsed:
            bad += 1
            # Optional: uncomment for diagnostics
            # print(f"Bad row (date parse failed): {row}")
            continue

        src_txt, dt_utc = dt_parsed

        phone = normalize_phone(get_val(row, header_map, "phone_number_dialed", "phone_number"))
        lead_id = pick_lead_id(row, header_map)

        mapped = {
            "Date": dt_utc.isoformat(),  # RFC3339, BigQuery JSON load compatible
            "FirstName": safe_str(get_val(row, header_map, "first_name")),
            "LastName": safe_str(get_val(row, header_map, "last_name")),
            "Address": safe_str(get_val(row, header_map, "address1")),
            "CallNotes": safe_str(get_val(row, header_map, "call_notes")),
            # Business rule: TalkTime stores campaign_id in your current table
            "TalkTime": safe_str(get_val(row, header_map, "campaign_id")),
            "SiteName": None,
            "Phone": phone,
            "Email": (safe_str(get_val(row, header_map, "email")) or None),
            "LeadID": (lead_id if lead_id is None else (str(lead_id).strip() or None)),
            "ListDescription": safe_str(get_val(row, header_map, "list_description")),
            "ListID": (safe_str(get_val(row, header_map, "list_id"))),
            "Disposition": safe_str(get_val(row, header_map, "status")),
            "TermReason": None,
            "SubscriberID": None,
            "Source": None,
            "LeadType": None,
            "SourceLocalTime": src_txt,
            "SourceTimezone": (str(row_tz) if row_tz else SOURCE_TZ),
            "SourceFile": source_file,
        }

        # DedupKey / RowHash (time bucket to minute)
        key_parts = [
            mapped.get("Phone") or "",
            mapped.get("LeadID") or "",
            mapped.get("ListID") or "",
            mapped.get("Disposition") or "",
            dt_utc.strftime("%Y-%m-%d %H:%M"),
        ]
        dedup = "|".join(key_parts)
        mapped["DedupKey"] = dedup
        mapped["RowHash"] = hashlib.sha256(dedup.encode()).hexdigest()
        mapped["_raw"] = json.dumps(row, ensure_ascii=False)

        buf.write(json.dumps(mapped) + "\n")
        good += 1

        # Optional one-time diag
        # if total == 1:
        #     print(f"[Diag] first row date='{src_txt}', tz='{row_tz}', utc='{dt_utc.isoformat()}'")

    # If nothing good parsed, still move the file and write audit
    if good > 0:
        tmp_blob.upload_from_string(buf.getvalue(), content_type="application/x-ndjson")
        ndjson_uri = f"gs://{bucket_name}/{tmp_key}"
        load_to_staging_from_ndjson(ndjson_uri)
        run_merge(TIME_TOL_SEC)

    # Audit (inserted/skipped can be added later using MERGE stats if needed)
    bq.query(
        """
        INSERT `dev-at-cf.at_dials._ingestion_audit`
        (SourceFile, RowsTotal, RowsInserted, RowsSkippedExisting, RowsErrored, Notes)
        VALUES (@sf, @rt, NULL, NULL, @err, @note)
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("sf", "STRING", source_file),
                bigquery.ScalarQueryParameter("rt", "INT64", total),
                bigquery.ScalarQueryParameter("err", "INT64", bad),
                bigquery.ScalarQueryParameter("note", "STRING", f"Processed good={good}, bad={bad}"),
            ]
        ),
    ).result()

    # Move original file to processed/
    processed_key = f"processed/{object_name.rsplit('/',1)[-1]}"
    bucket.copy_blob(blob, bucket, processed_key)
    blob.delete()
