# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00000000-0000-0000-0000-000000000000",
# META       "default_lakehouse_name": "YourLakehouse",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "00000000-0000-0000-0000-000000000000"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Cell 1: Parameters (tagged as parameters cell for Fabric Jobs API injection)

# PARAMETERS CELL ********************

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

tables_json = "[]"
results_path = "Files/pr_validation/results/data_presence_results_manual.json"
mode = "soft"
run_id = "manual"
retention_months = "3"
# Optional: only needed for the abfss write fallback below. Leave blank to disable it.
lakehouse_workspace_id = ""
lakehouse_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 2: Imports and helpers

# CELL ********************

import json
import re
from datetime import datetime, timezone, timedelta
from pyspark.sql import functions as F

def log(level, msg):
    """Print a log message with level prefix. ASCII-safe."""
    print("[{}] {}".format(level, msg))

def get_table_spark_name(table_name):
    """
    Convert registry table name to a Spark-readable reference.

    - 'silver.dim_customer' -> 'silver.dim_customer' (already valid Spark SQL)
    - 'gold.daily_sales_summary' -> 'gold.daily_sales_summary'
    - 'Files/...' paths are not Spark SQL tables; handled separately.
    """
    return table_name

def is_files_path(table_name):
    """Check if table_name refers to a Files/* path (not a Delta managed table)."""
    return table_name.startswith("Files/")


def get_results_directory(path_value):
    """Return the parent directory for a results file path."""
    normalized = path_value.rstrip("/")
    if "/" not in normalized:
        return "."
    return normalized.rsplit("/", 1)[0]


def cleanup_old_result_files(results_dir, retention_months_value):
    """Delete old run result files using the timestamp encoded in the file name."""
    try:
        retention_int = int(retention_months_value)
    except Exception:
        retention_int = 3

    if retention_int <= 0:
        log("INFO", "Retention cleanup disabled (retention_months={})".format(retention_months_value))
        return

    pattern = re.compile(r"^data_presence_results_(\d{8}T\d{6}Z)_[A-Za-z0-9-]+\.json$")
    cutoff = datetime.now(timezone.utc) - timedelta(days=30 * retention_int)

    try:
        entries = mssparkutils.fs.ls(results_dir)
    except Exception as e:
        log("WARN", "Could not list results directory '{}': {}".format(results_dir, str(e)[:200]))
        return

    deleted = 0
    for entry in entries:
        try:
            name = entry.name if hasattr(entry, "name") else entry.get("name", "")
            path = entry.path if hasattr(entry, "path") else entry.get("path", results_dir + "/" + name)
            is_dir = entry.isDir if hasattr(entry, "isDir") else entry.get("isDir", False)

            if is_dir:
                continue

            match = pattern.match(name)
            if not match:
                continue

            file_ts = datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
            if file_ts < cutoff:
                mssparkutils.fs.rm(path, True)
                deleted += 1
        except Exception as e:
            log("WARN", "Cleanup skipped for one entry in '{}': {}".format(results_dir, str(e)[:120]))

    log("INFO", "Cleanup complete in {} - deleted {} file(s) older than {} month(s).".format(
        results_dir,
        deleted,
        retention_int,
    ))


def add_business_days(base_date, offset_days):
    """Add/subtract business days (Mon-Fri) from a date."""
    if offset_days == 0:
        return base_date
    step = 1 if offset_days > 0 else -1
    remaining = abs(offset_days)
    current = base_date
    while remaining > 0:
        current = current + timedelta(days=step)
        if current.weekday() < 5:
            remaining -= 1
    return current


def load_files_dataframe(table_path):
    """Load Files/* path with fallback formats and path variants."""
    path_candidates = [table_path]
    if not table_path.startswith("/"):
        path_candidates.append("/" + table_path)

    format_candidates = [
        ("delta", {}),
        ("parquet", {}),
        ("csv", {"header": "true", "inferSchema": "true"}),
    ]

    last_error = None
    for path in path_candidates:
        for fmt, opts in format_candidates:
            try:
                reader = spark.read.format(fmt)
                for key, val in opts.items():
                    reader = reader.option(key, val)
                df = reader.load(path)
                return df, fmt, path
            except Exception as e:
                last_error = e

    raise Exception("Could not load Files path '{}': {}".format(table_path, str(last_error)[:200]))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 3: Check runner - row count, freshness, schema, custom checks

# CELL ********************

log("START", "Data Presence Validation - run_id={}".format(run_id))
log("INFO", "Mode: {} | Tables payload length: {} chars".format(mode, len(tables_json)))

tables = json.loads(tables_json)
log("INFO", "Tables to validate: {}".format(len(tables)))

results = {
    "run_id": run_id,
    "mode": mode,
    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "tables": {},
    "summary": {"total": 0, "passed": 0, "failed": 0, "skipped": 0, "error": 0},
}

for table_entry in tables:
    table_name = table_entry.get("table_name", "")
    min_rows = table_entry.get("min_rows", 0)
    row_count_mode = table_entry.get("row_count_mode", None)
    min_rows_date_check = table_entry.get("min_rows_date_check", None)  # None = use latest available date
    freshness_hours = table_entry.get("freshness_hours", 0)
    freshness_column = table_entry.get("freshness_column", "")
    required_columns = table_entry.get("required_columns", [])
    not_null_columns = table_entry.get("not_null_columns", [])
    custom_checks = table_entry.get("checks", [])

    log("INFO", "--- Validating: {} ---".format(table_name))
    table_result = {"checks": [], "passed": True}
    results["summary"]["total"] += 1

    source_type = "files" if is_files_path(table_name) else "table"

    # --- Check: Source exists and readable ---
    try:
        if source_type == "files":
            df, file_format, loaded_path = load_files_dataframe(table_name)
            log("INFO", "Loaded Files path using format={} path={}".format(file_format, loaded_path))
        else:
            df = spark.table(get_table_spark_name(table_name))
    except Exception as e:
        log("FAIL", "Source not found or not accessible: {} - {}".format(table_name, str(e)[:200]))
        table_result["checks"].append({
            "name": "source_exists",
            "status": "error",
            "message": "Source not accessible: {}".format(str(e)[:200]),
        })
        table_result["passed"] = False
        results["summary"]["error"] += 1
        results["tables"][table_name] = table_result
        continue

    # --- Check 1: Row count ---
    # row_count_mode behavior:
    # - business_offset      -> freshness_column + min_rows_date_check
    # - latest_available_date -> freshness_column + MAX(date)
    # - total                -> full table/file row count
    # Backward compatibility (when row_count_mode not configured):
    # - freshness_column + min_rows_date_check set    => business_offset
    # - freshness_column + min_rows_date_check absent => latest_available_date
    # - no freshness_column                            => total
    try:
        if row_count_mode is None:
            if freshness_column and min_rows_date_check is not None:
                row_count_mode = "business_offset"
            elif freshness_column:
                row_count_mode = "latest_available_date"
            else:
                row_count_mode = "total"

        if row_count_mode == "business_offset":
            if freshness_column and min_rows_date_check is not None:
                target_date = add_business_days(datetime.now(timezone.utc).date(), min_rows_date_check)
                row_count = df.filter(
                    F.to_date(F.col(freshness_column)) == F.lit(target_date.isoformat())
                ).count()
                count_scope = "business_date_offset({})={}".format(min_rows_date_check, target_date.isoformat())
            else:
                log("WARN", "Row count mode business_offset invalid for {} - falling back to total".format(table_name))
                row_count = df.count()
                count_scope = "total_fallback_from_business_offset"
        elif row_count_mode == "latest_available_date":
            if freshness_column:
                max_date = df.select(F.max(F.to_date(F.col(freshness_column))).alias("max_d")).collect()[0]["max_d"]
                if max_date is not None:
                    row_count = df.filter(
                        F.to_date(F.col(freshness_column)) == F.lit(str(max_date))
                    ).count()
                    count_scope = "latest_available_date={}".format(max_date)
                else:
                    row_count = 0
                    count_scope = "latest_available_date=NULL"
            else:
                log("WARN", "Row count mode latest_available_date invalid for {} - falling back to total".format(table_name))
                row_count = df.count()
                count_scope = "total_fallback_from_latest_available_date"
        else:
            row_count = df.count()
            count_scope = "total"

        passed = row_count >= min_rows
        status = "passed" if passed else "failed"
        log("PASS" if passed else "FAIL",
            "Row count ({}): {} (min_rows={}) -> {}".format(count_scope, row_count, min_rows, status))
        table_result["checks"].append({
            "name": "row_count",
            "status": status,
            "actual": row_count,
            "expected_min": min_rows,
            "scope": count_scope,
        })
        if not passed:
            table_result["passed"] = False
    except Exception as e:
        log("FAIL", "Row count check error for {}: {}".format(table_name, str(e)[:200]))
        table_result["checks"].append({
            "name": "row_count",
            "status": "error",
            "message": str(e)[:200],
        })
        table_result["passed"] = False

    # --- Check 2: Freshness ---
    if freshness_hours > 0 and freshness_column:
        try:
            max_ts_row = df.select(F.max(F.col(freshness_column)).alias("max_ts")).collect()[0]
            max_ts = max_ts_row["max_ts"]

            if max_ts is None:
                log("FAIL", "Freshness: column '{}' is all NULL".format(freshness_column))
                table_result["checks"].append({
                    "name": "freshness",
                    "status": "failed",
                    "message": "Freshness column '{}' is all NULL.".format(freshness_column),
                })
                table_result["passed"] = False
            else:
                import datetime as dt
                now_utc = datetime.now(timezone.utc)

                # Handle date-only vs timestamp columns
                if isinstance(max_ts, dt.date) and not isinstance(max_ts, dt.datetime):
                    # Pure date: compare in days, convert to hours
                    today_utc = now_utc.date()
                    age_hours = (today_utc - max_ts).days * 24.0
                elif hasattr(max_ts, "timestamp"):
                    # pyspark Timestamp / python datetime
                    if max_ts.tzinfo is None:
                        max_ts_utc = max_ts.replace(tzinfo=timezone.utc)
                    else:
                        max_ts_utc = max_ts
                    age_hours = (now_utc - max_ts_utc).total_seconds() / 3600.0
                else:
                    # String fallback - try datetime first, then date
                    ts_str = str(max_ts)[:19]
                    try:
                        max_ts_parsed = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                        age_hours = (now_utc - max_ts_parsed).total_seconds() / 3600.0
                    except ValueError:
                        max_date_parsed = datetime.strptime(ts_str[:10], "%Y-%m-%d").date()
                        age_hours = (now_utc.date() - max_date_parsed).days * 24.0

                passed = age_hours <= freshness_hours
                status = "passed" if passed else "failed"
                log("PASS" if passed else "FAIL",
                    "Freshness: max({})={}, age={:.1f}h (limit={}h) -> {}".format(
                        freshness_column, max_ts, age_hours, freshness_hours, status))
                table_result["checks"].append({
                    "name": "freshness",
                    "status": status,
                    "freshness_column": freshness_column,
                    "max_value": str(max_ts),
                    "age_hours": round(age_hours, 1),
                    "limit_hours": freshness_hours,
                })
                if not passed:
                    table_result["passed"] = False
        except Exception as e:
            log("FAIL", "Freshness check error for {}: {}".format(table_name, str(e)[:200]))
            table_result["checks"].append({
                "name": "freshness",
                "status": "error",
                "message": str(e)[:200],
            })
            table_result["passed"] = False
    elif freshness_hours > 0 and not freshness_column:
        log("SKIP", "Freshness check skipped - no freshness_column defined for {}".format(table_name))
        table_result["checks"].append({
            "name": "freshness",
            "status": "skipped",
            "message": "No freshness_column defined in table registry.",
        })
    else:
        # freshness_hours == 0: explicitly disabled in table registry (freshness_hours commented out)
        log("SKIP", "Freshness check disabled for {} (freshness_hours not set)".format(table_name))
        table_result["checks"].append({
            "name": "freshness",
            "status": "skipped",
            "message": "Freshness check disabled - freshness_hours not configured in table registry.",
        })

    # --- Check 3: Schema (required columns) ---
    if required_columns:
        try:
            actual_columns = [c.lower() for c in df.columns]
            missing = [c for c in required_columns if c.lower() not in actual_columns]
            passed = len(missing) == 0
            status = "passed" if passed else "failed"
            log("PASS" if passed else "FAIL",
                "Schema: {} required columns, {} missing -> {}".format(
                    len(required_columns), len(missing), status))
            if missing:
                log("FAIL", "Missing columns: {}".format(missing))
            table_result["checks"].append({
                "name": "schema",
                "status": status,
                "required": len(required_columns),
                "missing": missing,
            })
            if not passed:
                table_result["passed"] = False
        except Exception as e:
            log("FAIL", "Schema check error for {}: {}".format(table_name, str(e)[:200]))
            table_result["checks"].append({
                "name": "schema",
                "status": "error",
                "message": str(e)[:200],
            })
            table_result["passed"] = False

    # --- Check 4: Not-null columns ---
    if not_null_columns:
        if source_type == "files":
            log("SKIP", "Not-null column checks skipped for Files path {}".format(table_name))
            for col in not_null_columns:
                table_result["checks"].append({
                    "name": "not_null_{}".format(col),
                    "status": "skipped",
                    "message": "Not-null checks are supported for table sources only.",
                })
        else:
            for col in not_null_columns:
                try:
                    null_count = df.filter(F.col(col).isNull()).count()
                    passed = (null_count == 0)
                    status = "passed" if passed else "failed"
                    log("PASS" if passed else "FAIL",
                        "Not-null '{}': null_count={} -> {}".format(col, null_count, status))
                    table_result["checks"].append({
                        "name": "not_null_{}".format(col),
                        "status": status,
                        "column": col,
                        "null_count": null_count,
                    })
                    if not passed:
                        table_result["passed"] = False
                except Exception as e:
                    log("FAIL", "Not-null check error for column '{}': {}".format(col, str(e)[:200]))
                    table_result["checks"].append({
                        "name": "not_null_{}".format(col),
                        "status": "error",
                        "message": str(e)[:200],
                    })
                    table_result["passed"] = False

    # --- Check 5: Custom checks ---
    for check in custom_checks:
        check_name = check.get("name", "unnamed")
        check_sql = check.get("sql", "")
        if not check_sql:
            continue
        try:
            if source_type == "files":
                log("SKIP", "Custom check '{}' skipped for Files path {}".format(check_name, table_name))
                table_result["checks"].append({
                    "name": "custom_{}".format(check_name),
                    "status": "skipped",
                    "message": "Custom SQL checks are supported for table sources only.",
                })
                continue

            # Substitute {table} placeholder with actual table name
            resolved_sql = check_sql.replace("{table}", get_table_spark_name(table_name))
            result_val = spark.sql(resolved_sql).collect()[0][0]

            check_passed = True
            expected_desc = ""
            if "expect_eq" in check:
                expected = check["expect_eq"]
                check_passed = (result_val == expected)
                expected_desc = "== {}".format(expected)
            elif "expect_lt" in check:
                expected = check["expect_lt"]
                check_passed = (float(result_val) < float(expected))
                expected_desc = "< {}".format(expected)
            elif "expect_gt" in check:
                expected = check["expect_gt"]
                check_passed = (float(result_val) > float(expected))
                expected_desc = "> {}".format(expected)

            status = "passed" if check_passed else "failed"
            log("PASS" if check_passed else "FAIL",
                "Custom '{}': actual={}, expected {} -> {}".format(
                    check_name, result_val, expected_desc, status))
            table_result["checks"].append({
                "name": "custom_{}".format(check_name),
                "status": status,
                "sql": resolved_sql,
                "actual": str(result_val),
                "expected": expected_desc,
            })
            if not check_passed:
                table_result["passed"] = False
        except Exception as e:
            log("FAIL", "Custom check '{}' error: {}".format(check_name, str(e)[:200]))
            table_result["checks"].append({
                "name": "custom_{}".format(check_name),
                "status": "error",
                "message": str(e)[:200],
            })
            table_result["passed"] = False

    # --- Summarize table result ---
    if table_result["passed"]:
        results["summary"]["passed"] += 1
        log("PASS", "Table {} - ALL CHECKS PASSED".format(table_name))
    else:
        results["summary"]["failed"] += 1
        log("FAIL", "Table {} - ONE OR MORE CHECKS FAILED".format(table_name))

    results["tables"][table_name] = table_result

log("INFO", "Validation complete. Summary: {}".format(json.dumps(results["summary"])))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 4: Write results JSON to Lakehouse Files

# CELL ********************

results_json = json.dumps(results, indent=2, default=str)
log("INFO", "Writing results to: {}".format(results_path))
results_dir = get_results_directory(results_path)

# Ensure the parent directory exists (mssparkutils.fs.put does not create it)
try:
    mssparkutils.fs.mkdirs(results_dir)
    log("INFO", "Ensured results directory exists: {}".format(results_dir))
except Exception as e:
    log("WARN", "mkdirs('{}') failed (may already exist): {}".format(results_dir, str(e)[:100]))

# Use the Fabric mssparkutils to write to the Lakehouse Files section
try:
    mssparkutils.fs.put(results_path, results_json, True)
    log("PASS", "Results written successfully ({} bytes)".format(len(results_json)))
except Exception as e:
    # Fallback: write via HDFS-compatible path
    log("WARN", "mssparkutils.fs.put failed ({}), trying HDFS path".format(str(e)[:100]))
    try:
        if lakehouse_workspace_id and lakehouse_id:
            hdfs_path = "abfss://{}@onelake.dfs.fabric.microsoft.com/{}/{}".format(
                lakehouse_workspace_id,
                lakehouse_id,
                results_path,
            )
            dbutils_rdd = spark.sparkContext.parallelize([results_json])
            dbutils_rdd.coalesce(1).saveAsTextFile(hdfs_path + "_tmp")
            log("WARN", "Results written via HDFS fallback (may need manual rename)")
        else:
            log("FAIL", "HDFS fallback skipped: set lakehouse_workspace_id and lakehouse_id parameters to enable it.")
    except Exception as e2:
        log("FAIL", "Could not write results: {}".format(str(e2)[:200]))

cleanup_old_result_files(results_dir, retention_months)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 5: Pass/fail gate

# CELL ********************

summary = results["summary"]
log("INFO", "=== FINAL SUMMARY ===")
log("INFO", "Total: {}, Passed: {}, Failed: {}, Skipped: {}, Error: {}".format(
    summary["total"], summary["passed"], summary["failed"],
    summary["skipped"], summary["error"]))

if mode == "hard" and (summary["failed"] > 0 or summary["error"] > 0):
    msg = "Data presence validation FAILED in hard mode: {} failed, {} error out of {} tables.".format(
        summary["failed"], summary["error"], summary["total"])
    log("FAIL", msg)
    raise Exception(msg)
elif summary["failed"] > 0 or summary["error"] > 0:
    log("WARN", "Data presence issues detected (soft mode - not blocking).")
else:
    log("PASS", "All data presence checks passed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
