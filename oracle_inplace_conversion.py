#!/usr/bin/env python3
"""Safeguarded Oracle POS/ATM value conversion while retaining VARCHAR2 columns.

The conversion maps describe VALUE transformations only. Converted date/time/amount
values are written back into existing columns as VARCHAR2 strings.
"""
from datetime import datetime
from pathlib import Path
import re

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Explicit column_name -> conversion operation maps. EXIT_TIME, RE_ENTRY_TIME, and B24_ENTRY_TIME are intentionally not processed.
POS_CONVERSION_MAP = {
    "TRN_BGN_DATE": "DATE",
    "POST_DATE": "DATE",
    "AINT_SETL_DATE": "DATE",
    "IINT_SETL_DATE": "DATE",
    "TRN_BGN_TIME": "TIMESTAMP",
    "AMT_1": "AMOUNT",
    "AMT_2": "AMOUNT",
}

ATM_CONVERSION_MAP = {
    "DATE_TRN_BGN": "DATE",
    "POST_DATE": "DATE",
    "ACQ_ICHG_SETL_DATE": "DATE",
    "ISS_ICHG_SETL_DATE": "DATE",
    "TRN_ENTY_TIME": "TIMESTAMP",
    "AMT_1": "AMOUNT",
    "AMT_2": "AMOUNT",
    "AMT_3": "AMOUNT",
}

DATE_OUTPUT_FORMAT = "dd_MM-yyyy"
TIMESTAMP_OUTPUT_FORMAT = "dd_MM-yyyy HH:mm:ss"
DATE_INPUT_PIVOT = 70  # YY 00-69 -> 2000-2069; YY 70-99 -> 1970-1999


def load_properties(path):
    props = {}
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"Properties file not found: {p.resolve()}")
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith(("#", "!")) and "=" in line:
            k, v = line.split("=", 1)
            props[k.strip()] = v.strip()
    return props


def required(props, key):
    value = props.get(key, "").strip()
    upper_value = value.upper()
    if not value or any(token in upper_value for token in (
        "CHANGE_ME", "ORACLE_HOST", "ORACLE_SERVICE", "ORACLE_USERNAME", "ORACLE_PASSWORD"
    )):
        raise ValueError(f"Set a real value for '{key}' in application.properties.")
    return value


def bool_value(props, key, default=False):
    value = props.get(key, str(default)).strip().lower()
    if value not in {"true", "false", "yes", "no", "1", "0"}:
        raise ValueError(f"{key} must be true or false.")
    return value in {"true", "yes", "1"}


def ident(name):
    """Validate and quote an Oracle table/column identifier."""
    parts = name.split(".")
    if len(parts) not in (1, 2) or any(
        not re.fullmatch(r"[A-Za-z][A-Za-z0-9_$#]*", p) for p in parts
    ):
        raise ValueError(f"Invalid Oracle identifier: {name}")
    return ".".join(f'"{p.upper()}"' for p in parts)


def split_name(table):
    ident(table)
    parts = table.upper().split(".")
    return (parts[0], parts[1]) if len(parts) == 2 else (None, parts[0])


def qualified_name(schema, name):
    return f"{schema}.{name}" if schema else name


def generated_name(table, suffix):
    schema, name = split_name(table)
    generated = name[:30 - len(suffix)] + suffix
    return qualified_name(schema, generated)


def stage_name(table):
    return generated_name(table, "_CVT_TMP")


def backup_name(table, yyyymmdd):
    return generated_name(table, f"_BAK_{yyyymmdd}")


def backup_temp_name(table):
    return generated_name(table, "_BAK_TMP")


def swap_name(table):
    return generated_name(table, "_SWAP_OLD")


def jdbc_read(spark, props, table, fetchsize):
    return (
        spark.read.format("jdbc")
        .option("url", required(props, "oracle.url"))
        .option("dbtable", table)
        .option("user", required(props, "oracle.username"))
        .option("password", required(props, "oracle.password"))
        .option("driver", "oracle.jdbc.OracleDriver")
        .option("fetchsize", str(fetchsize))
        .load()
    )


def count_rows(spark, props, table, fetchsize):
    query = f"(SELECT COUNT(*) AS C FROM {ident(table)}) Q"
    row = (
        spark.read.format("jdbc")
        .option("url", required(props, "oracle.url"))
        .option("dbtable", query)
        .option("user", required(props, "oracle.username"))
        .option("password", required(props, "oracle.password"))
        .option("driver", "oracle.jdbc.OracleDriver")
        .option("fetchsize", str(fetchsize))
        .load()
        .first()
    )
    return int(row["C"])


def execute(conn, sql):
    stmt = conn.createStatement()
    try:
        stmt.execute(sql)
    finally:
        stmt.close()


def table_exists(conn, table):
    schema, name = split_name(table)
    if schema:
        ps = conn.prepareStatement(
            "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER=? AND TABLE_NAME=?"
        )
        ps.setString(1, schema)
        ps.setString(2, name)
    else:
        ps = conn.prepareStatement(
            "SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME=?"
        )
        ps.setString(1, name)
    try:
        rs = ps.executeQuery()
        try:
            rs.next()
            return rs.getInt(1) > 0
        finally:
            rs.close()
    finally:
        ps.close()


def connection(spark, props):
    spark._jvm.java.lang.Class.forName("oracle.jdbc.OracleDriver")
    return spark._jvm.java.sql.DriverManager.getConnection(
        required(props, "oracle.url"),
        required(props, "oracle.username"),
        required(props, "oracle.password"),
    )


def oracle_column_types(conn, table):
    """Return ordered column metadata, including type/length/precision/scale."""
    schema, name = split_name(table)
    if schema:
        ps = conn.prepareStatement(
            "SELECT COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, DATA_LENGTH, "
            "DATA_PRECISION, DATA_SCALE, COLUMN_ID "
            "FROM ALL_TAB_COLUMNS WHERE OWNER=? AND TABLE_NAME=? ORDER BY COLUMN_ID"
        )
        ps.setString(1, schema)
        ps.setString(2, name)
    else:
        ps = conn.prepareStatement(
            "SELECT COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, DATA_LENGTH, "
            "DATA_PRECISION, DATA_SCALE, COLUMN_ID "
            "FROM USER_TAB_COLUMNS WHERE TABLE_NAME=? ORDER BY COLUMN_ID"
        )
        ps.setString(1, name)
    result = {}
    try:
        rs = ps.executeQuery()
        try:
            while rs.next():
                result[rs.getString(1).upper()] = (
                    rs.getString(2).upper(),
                    int(rs.getInt(3)),
                    int(rs.getInt(4)),
                    (int(rs.getInt(5)) if rs.getObject(5) is not None else None),
                    (int(rs.getInt(6)) if rs.getObject(6) is not None else None),
                    int(rs.getInt(7)),
                )
        finally:
            rs.close()
    finally:
        ps.close()
    return result


def assert_mapped_columns_exist(conn, table, conversion_map):
    """Do not reject a table just because earlier processing changed mapped types.
    The staging table is explicitly rebuilt with VARCHAR2 for every mapped field.
    """
    metadata = oracle_column_types(conn, table)
    missing = sorted(set(conversion_map) - set(metadata))
    if missing:
        raise ValueError(f"{table}: required mapped columns missing: {missing}")

def ensure_no_name_collision(conn, table_names):
    collisions = [name for name in table_names if table_exists(conn, name)]
    if collisions:
        raise RuntimeError(
            "The following work table(s) already exist; inspect them before rerunning: "
            + ", ".join(collisions)
        )


def backup_old_name(table):
    return generated_name(table, "_BAK_OLD")


def refresh_daily_backup(spark, props, conn, table, fetchsize, expected_count, backup_date):
    """Create a temporary backup, verify row count/schema, then safely replace today's backup."""
    daily = backup_name(table, backup_date)
    temp = backup_temp_name(table)
    old = backup_old_name(table)

    if table_exists(conn, temp):
        raise RuntimeError(
            f"Temporary backup {temp} already exists. Inspect it manually before retrying; "
            "the script will not delete a potentially recoverable copy."
        )
    if table_exists(conn, old):
        raise RuntimeError(
            f"Backup replacement recovery table {old} already exists. Inspect it manually "
            "before retrying; no target changes have been made."
        )

    source_schema = oracle_column_types(conn, table)
    execute(conn, f"CREATE TABLE {ident(temp)} AS SELECT * FROM {ident(table)}")
    temp_count = count_rows(spark, props, temp, fetchsize)
    temp_schema = oracle_column_types(conn, temp)
    if temp_count != expected_count:
        raise RuntimeError(
            f"Temporary backup row count {temp_count} != source count {expected_count}. "
            f"Target untouched; inspect {temp}."
        )
    if temp_schema != source_schema:
        raise RuntimeError(
            f"Temporary backup schema does not match source. Target untouched; inspect {temp}. "
            f"Source columns={len(source_schema)}, backup columns={len(temp_schema)}."
        )

    daily_exists = table_exists(conn, daily)
    _, daily_unqualified = split_name(daily)
    _, temp_unqualified = split_name(temp)
    _, old_unqualified = split_name(old)

    if daily_exists:
        execute(conn, f"ALTER TABLE {ident(daily)} RENAME TO {ident(old_unqualified)}")
    try:
        execute(conn, f"ALTER TABLE {ident(temp)} RENAME TO {ident(daily_unqualified)}")
    except Exception:
        if daily_exists and table_exists(conn, old):
            execute(conn, f"ALTER TABLE {ident(old)} RENAME TO {ident(daily_unqualified)}")
        raise

    verified_count = count_rows(spark, props, daily, fetchsize)
    verified_schema = oracle_column_types(conn, daily)
    if verified_count != expected_count or verified_schema != source_schema:
        # Preserve the questionable new copy and restore the previous daily backup if one existed.
        if daily_exists and table_exists(conn, old):
            questionable = generated_name(table, "_BAK_BAD")
            if not table_exists(conn, questionable):
                execute(conn, f"ALTER TABLE {ident(daily)} RENAME TO {ident(split_name(questionable)[1])}")
                execute(conn, f"ALTER TABLE {ident(old)} RENAME TO {ident(daily_unqualified)}")
        raise RuntimeError(
            f"Daily backup {daily} failed post-rename verification "
            f"(rows={verified_count}, expected={expected_count}, schema_match={verified_schema == source_schema}). "
            "Target table has not been modified; inspect backup artifacts."
        )

    if daily_exists and table_exists(conn, old):
        execute(conn, f"DROP TABLE {ident(old)} PURGE")
    print(f"VERIFIED BACKUP: {daily}; rows={verified_count}; schema matches source")
    return daily

def text_col(col):
    """Cast a source column to text so Spark DATE/NUMBER fields can be normalized too."""
    return F.trim(F.col(col).cast("string"))


def parse_yyMMdd(col, null_sentinels):
    """Parse YYMMDD, YYYYMMDD, ISO date text, or JDBC DATE/TIMESTAMP string values."""
    raw = text_col(col)
    is_null = raw.isNull() | (raw == "") | raw.isin(*null_sentinels)

    yy = F.substring(raw, 1, 2)
    rest = F.substring(raw, 3, 4)
    century = F.when(yy.cast("int") < DATE_INPUT_PIVOT, F.lit("20")).otherwise(F.lit("19"))
    full_yymmdd = F.concat(century, yy, rest)

    parsed_yymmdd = F.to_date(full_yymmdd, "yyyyMMdd")
    parsed_yyyymmdd = F.to_date(raw, "yyyyMMdd")
    parsed_iso_date = F.to_date(F.substring(raw, 1, 10), "yyyy-MM-dd")

    parsed = (
        F.when(raw.rlike(r"^\d{6}$"), parsed_yymmdd)
        .when(raw.rlike(r"^\d{8}$"), parsed_yyyymmdd)
        .when(raw.rlike(r"^\d{4}-\d{2}-\d{2}($|[ T].*)"), parsed_iso_date)
        .otherwise(F.lit(None).cast("date"))
    )
    return F.when(is_null, F.lit(None).cast("date")).otherwise(parsed)

def validate_source(df, conversion_map, timestamp_date_map, null_sentinels):
    fields = {field.name.upper(): field.dataType for field in df.schema.fields}
    missing = sorted(set(conversion_map) - set(fields))
    if missing:
        raise ValueError(f"Missing mapped source columns: {missing}")

    checks = []
    for col, operation in conversion_map.items():
        raw = text_col(col)
        nonblank = raw.isNotNull() & (raw != "")
        if operation == "DATE":
            parsed = parse_yyMMdd(col, null_sentinels)
            accepted_encoding = (
                raw.rlike(r"^\d{6}$")
                | raw.rlike(r"^\d{8}$")
                | raw.rlike(r"^\d{4}-\d{2}-\d{2}($|[ T].*)")
            )
            invalid = (
                nonblank & (~raw.isin(*null_sentinels)) & (~accepted_encoding | parsed.isNull())
            )
            checks.append(F.sum(F.when(invalid, 1).otherwise(0)).alias(f"BAD_{col}"))

        elif operation == "TIMESTAMP":
            date_col = timestamp_date_map[col]
            if date_col.upper() not in fields:
                raise ValueError(f"{col}: configured date column {date_col} is missing.")
            hour = F.substring(raw, 1, 2).cast("int")
            minute = F.substring(raw, 3, 2).cast("int")
            second = F.substring(raw, 5, 2).cast("int")
            invalid_time = nonblank & (
                ~raw.rlike(r"^\d{8}$")
                | hour.isNull() | minute.isNull() | second.isNull()
                | (~hour.between(0, 23)) | (~minute.between(0, 59)) | (~second.between(0, 59))
            )
            parsed_date = parse_yyMMdd(date_col, null_sentinels)
            invalid_date_link = nonblank & parsed_date.isNull()
            checks.append(F.sum(F.when(invalid_time, 1).otherwise(0)).alias(f"BAD_{col}"))
            checks.append(F.sum(F.when(invalid_date_link, 1).otherwise(0)).alias(f"NO_DATE_{col}"))

        elif operation == "AMOUNT":
            # If a NUMBER column is exposed as 50000.00, allow its harmless .00 scale
            # and still interpret 50000 as raw minor units. Non-zero decimal source values fail.
            normalized = F.regexp_replace(raw, r"\.0+$", "")
            checks.append(
                F.sum(F.when(nonblank & ~normalized.rlike(r"^-?\d+$"), 1).otherwise(0))
                .alias(f"BAD_{col}")
            )
            checks.append(
                F.sum(F.when(nonblank & normalized.cast("decimal(19,0)").isNull(), 1).otherwise(0))
                .alias(f"OVERFLOW_{col}")
            )

        elif operation == "TRUNCATE_LAST_2":
            invalid = nonblank & (
                (~raw.rlike(r"^\d{3,19}$")) | (~raw.endswith("00"))
            )
            checks.append(F.sum(F.when(invalid, 1).otherwise(0)).alias(f"BAD_{col}"))
        else:
            raise ValueError(f"Unsupported conversion operation {operation!r} for {col}")

    bad = df.agg(*checks).first().asDict()
    bad = {key: int(value) for key, value in bad.items() if value and value > 0}
    if bad:
        raise ValueError(
            f"Source validation failed; target untouched: {bad}. "
            "For time-field failures, reload the fresh raw source because a prior DATE/TIMESTAMP "
            "cast may have destroyed the original HHMMSSxx value."
        )


def transform(df, conversion_map, timestamp_date_map, null_sentinels):
    # Trim all string values but preserve nulls.
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            name = field.name
            df = df.withColumn(
                name,
                F.when(F.col(name).isNull(), F.lit(None).cast("string"))
                 .otherwise(F.trim(F.col(name))),
            )

    # Timestamp output must be built while the paired source date still has its raw representation.
    for col, operation in conversion_map.items():
        if operation != "TIMESTAMP":
            continue
        date_col = timestamp_date_map[col]
        parsed_date = parse_yyMMdd(date_col, null_sentinels)
        raw_time = text_col(col)
        hh = F.substring(raw_time, 1, 2)
        mi = F.substring(raw_time, 3, 2)
        ss = F.substring(raw_time, 5, 2)
        time_text = F.concat(hh, F.lit(":"), mi, F.lit(":"), ss)
        iso_date = F.date_format(parsed_date, "yyyy-MM-dd")
        parsed_timestamp = F.to_timestamp(
            F.concat(iso_date, F.lit(" "), time_text), "yyyy-MM-dd HH:mm:ss"
        )
        formatted_timestamp = F.date_format(parsed_timestamp, TIMESTAMP_OUTPUT_FORMAT)
        df = df.withColumn(
            col,
            F.when(raw_time.isNull() | (raw_time == ""), F.lit(None).cast("string"))
             .otherwise(formatted_timestamp),
        )

    for col, operation in conversion_map.items():
        if operation == "DATE":
            parsed = parse_yyMMdd(col, null_sentinels)
            df = df.withColumn(
                col,
                F.when(parsed.isNull(), F.lit(None).cast("string"))
                 .otherwise(F.date_format(parsed, DATE_OUTPUT_FORMAT)),
            )
        elif operation == "AMOUNT":
            raw = text_col(col)
            normalized = F.regexp_replace(raw, r"\.0+$", "")
            amount = F.when(
                raw.isNull() | (raw == ""),
                F.lit(None).cast("decimal(21,2)"),
            ).otherwise(
                (normalized.cast("decimal(19,0)") / F.lit(100)).cast("decimal(21,2)")
            )
            # Explicit decimal format avoids scientific notation and guarantees two decimals.
            df = df.withColumn(
                col,
                F.when(raw.isNull() | (raw == ""), F.lit(None).cast("string"))
                 .otherwise(amount.cast("string")),
            )
        elif operation == "TRUNCATE_LAST_2":
            raw = text_col(col)
            truncated = F.expr(f"substring(CAST(`{col}` AS STRING), 1, length(CAST(`{col}` AS STRING)) - 2)")
            df = df.withColumn(
                col,
                F.when(raw.isNull() | (raw == ""), F.lit(None).cast("string"))
                 .otherwise(truncated),
            )
    return df

def expected_lengths(conversion_map):
    expected = {}
    for col, operation in conversion_map.items():
        if operation == "DATE":
            expected[col] = 10
        elif operation == "TIMESTAMP":
            expected[col] = 19
        elif operation == "AMOUNT":
            expected[col] = 21
        elif operation == "TRUNCATE_LAST_2":
            expected[col] = 19
    return expected


def assert_stage_schema(conn, stage, conversion_map):
    metadata = oracle_column_types(conn, stage)
    expected = expected_lengths(conversion_map)
    errors = {}
    for col, length in expected.items():
        if col not in metadata:
            errors[col] = "missing"
        elif metadata[col][0] != "VARCHAR2" or metadata[col][1] < length:
            errors[col] = f"found {metadata[col]}, expected VARCHAR2 length >= {length}"
    if errors:
        raise RuntimeError(f"Staging schema validation failed: {errors}")

def validate_converted_values(df, conversion_map):
    checks = []
    for col, operation in conversion_map.items():
        raw = F.col(col)
        nonblank = raw.isNotNull() & (raw != "")
        if operation == "DATE":
            invalid = nonblank & (~raw.rlike(r"^\d{2}_\d{2}-\d{4}$"))
        elif operation == "TIMESTAMP":
            invalid = nonblank & (~raw.rlike(r"^\d{2}_\d{2}-\d{4} \d{2}:\d{2}:\d{2}$"))
        elif operation == "AMOUNT":
            invalid = nonblank & (~raw.rlike(r"^-?\d+\.\d{2}$"))
        elif operation == "TRUNCATE_LAST_2":
            invalid = nonblank & ((F.length(raw) > 17) | (~raw.rlike(r"^\d{1,17}$")))
        else:
            continue
        checks.append(F.sum(F.when(invalid, 1).otherwise(0)).alias(f"BAD_{col}"))
    bad = df.agg(*checks).first().asDict()
    bad = {key: int(value) for key, value in bad.items() if value and value > 0}
    if bad:
        raise ValueError(f"Converted-value validation failed: {bad}")


def run_job(job_name, table, conversion_map, timestamp_date_map, props_path):
    from pyspark.sql import SparkSession

    props = load_properties(props_path)
    dry_run = bool_value(props, "job.dry_run", True)
    backup_verified_override = bool_value(props, "backup.verified", False)
    spark = SparkSession.builder.appName(job_name).getOrCreate()
    spark.sparkContext.setLogLevel(props.get("spark.log.level", "WARN"))
    conn = None
    try:
        fetchsize = int(props.get("jdbc.fetchsize", "1000"))
        source = jdbc_read(spark, props, table, fetchsize)
        source_count = count_rows(spark, props, table, fetchsize)
        print(f"[{job_name}] table={table}; rows={source_count}; dry_run={dry_run}")
        print("Mapped output columns are written as VARCHAR2; source JDBC types may be DATE/NUMBER.")
        print("Run only against freshly reloaded raw values. A prior cast of a time field may be irreversible.")

        conn = connection(spark, props)
        assert_mapped_columns_exist(conn, table, conversion_map)
        validate_source(source, conversion_map, timestamp_date_map, {"000000"})

        converted = transform(source, conversion_map, timestamp_date_map, {"000000"})
        validate_converted_values(converted, conversion_map)

        if dry_run:
            cols = list(conversion_map)
            converted.select(*cols).show(20, truncate=False)
            print("DRY RUN COMPLETE: no Oracle tables were changed and no backup was created.")
            return

        # Refuse stale work tables rather than deleting a potentially useful recovery artifact.
        stage = stage_name(table)
        swap_old = swap_name(table)
        temp_backup = backup_temp_name(table)
        backup_date = datetime.now().strftime("%Y%m%d")
        ensure_no_name_collision(conn, [stage, swap_old, temp_backup, backup_old_name(table)])

        # Mandatory backup occurs before staging DDL or target data/schema changes.
        refreshed_backup = refresh_daily_backup(
            spark, props, conn, table, fetchsize, source_count, backup_date
        )
        if backup_verified_override:
            print("NOTE: backup.verified=true is informational; runtime backup checks remain mandatory.")

        # Stage starts as a CTAS clone so unconverted columns retain their Oracle data types.
        execute(conn, f"CREATE TABLE {ident(stage)} AS SELECT * FROM {ident(table)} WHERE 1=0")
        for col, length in expected_lengths(conversion_map).items():
            execute(
                conn,
                f"ALTER TABLE {ident(stage)} MODIFY ({ident(col)} VARCHAR2({length} CHAR))",
            )
        # Preserve the original INS_DATE default where this column exists.
        stage_columns = oracle_column_types(conn, stage)
        if "INS_DATE" in stage_columns and stage_columns["INS_DATE"][0] == "DATE":
            execute(conn, f"ALTER TABLE {ident(stage)} MODIFY (\"INS_DATE\" DATE DEFAULT SYSDATE)")
        assert_stage_schema(conn, stage, conversion_map)
        conn.close()
        conn = None

        (
            converted.write.format("jdbc")
            .option("url", required(props, "oracle.url"))
            .option("dbtable", stage)
            .option("user", required(props, "oracle.username"))
            .option("password", required(props, "oracle.password"))
            .option("driver", "oracle.jdbc.OracleDriver")
            .option("batchsize", props.get("jdbc.batchsize", "1000"))
            .mode("append")
            .save()
        )

        stage_count = count_rows(spark, props, stage, fetchsize)
        if stage_count != source_count:
            raise RuntimeError(
                f"Stage count {stage_count} != source count {source_count}; "
                f"target untouched. Inspect stage {stage} and backup {refreshed_backup}."
            )

        staged_df = jdbc_read(spark, props, stage, fetchsize)
        validate_converted_values(staged_df, conversion_map)
        conn = connection(spark, props)
        assert_stage_schema(conn, stage, conversion_map)
        conn.close()
        conn = None

        # Swap only after backup and stage checks pass. Preserve the old target temporarily
        # until the new target passes its post-swap row-count check.
        conn = connection(spark, props)
        execute(conn, f"ALTER TABLE {ident(table)} RENAME TO {ident(split_name(swap_old)[1])}")
        try:
            execute(conn, f"ALTER TABLE {ident(stage)} RENAME TO {ident(split_name(table)[1])}")
        except Exception:
            execute(conn, f"ALTER TABLE {ident(swap_old)} RENAME TO {ident(split_name(table)[1])}")
            raise
        conn.close()
        conn = None

        final_count = count_rows(spark, props, table, fetchsize)
        schema_error = None
        conn = connection(spark, props)
        try:
            try:
                assert_stage_schema(conn, table, conversion_map)
            except Exception as exc:
                schema_error = str(exc)
        finally:
            conn.close()
            conn = None

        if final_count != source_count or schema_error:
            conn = connection(spark, props)
            try:
                # Roll back the table swap. Keep the converted table under the stage name for diagnosis.
                execute(conn, f"ALTER TABLE {ident(table)} RENAME TO {ident(split_name(stage)[1])}")
                execute(conn, f"ALTER TABLE {ident(swap_old)} RENAME TO {ident(split_name(table)[1])}")
            finally:
                conn.close()
                conn = None
            raise RuntimeError(
                f"Post-swap verification failed (rows={final_count}, expected={source_count}, "
                f"schema_error={schema_error}); original target restored. "
                f"Converted table retained as {stage}; daily backup: {refreshed_backup}."
            )

        conn = connection(spark, props)
        execute(conn, f"DROP TABLE {ident(swap_old)} PURGE")
        conn.close()
        conn = None
        print(
            f"SUCCESS: {table} converted in place; rows={final_count}; "
            f"daily backup={refreshed_backup}"
        )
    finally:
        if conn is not None:
            conn.close()
        spark.stop()
