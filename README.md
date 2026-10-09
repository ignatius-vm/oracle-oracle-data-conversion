# Oracle POS/ATM VARCHAR2 Conversion: Backup

## Safety-gate properties

The relevant settings in `application.properties` are:

```properties
# --- safety gate ---
# Dry run:
job.dry_run=true
backup.verified=false

# Prod Run:
job.dry_run=false
backup.verified=true
```

### Dry run

```properties
backup.verified=false
job.dry_run=true
```

With `job.dry_run=true`, the script:

1. Reads the configured target table.
2. Validates that the mapped columns exist.
3. Validates source date, time, and amount values.
4. Transforms the mapped values in Spark.
5. Validates the transformed values.
6. Displays sample converted values.
7. Exits without creating a backup or changing Oracle tables.

`backup.verified=false`  during dry-run checks because no production backup/swap is performed in that run. The script's runtime backup checks are mandatory for an actual production run; the `backup.verified` property does not replace those checks.

### Production run

```properties
job.dry_run=false
backup.verified=true
```

`job.dry_run=false` enables the script's backup, staging, and table-swap workflow.

`backup.verified=true` does **not** skip or replace the runtime backup verification. The script still checks the backup it creates during the production run.

## 3. Production backup process

When `job.dry_run=false`, the script follows this safety sequence:

1. **Read and validate source.** It reads the target table and records the source row count. Source validation and converted-value validation must pass before the backup process begins. If validation fails, the script raises an error and leaves the target table untouched.
2. **Check for work-table name collisions.** It refuses to proceed if relevant temporary/staging/recovery tables already exist. Inspect any existing work tables manually; the script deliberately does not automatically delete potentially recoverable artifacts.
3. **Create a temporary backup.** It creates a copy of the target table named with the `_BAK_TMP` suffix using `CREATE TABLE ... AS SELECT *`.
4. **Verify the temporary backup.** It compares the temporary backup's row count with the source row count and compares its column metadata/schema with the source. If either check fails, it stops and preserves the temporary backup for investigation.
5. **Refresh the dated backup.** The final backup name follows this pattern:
   - POS: `INSUSER.ETL_PTLFX_01_BAK_YYYYMMDD`
   - ATM: `INSUSER.ETL_TLFX_01_BAK_YYYYMMDD`

   `YYYYMMDD` is the run date. If a backup for that date already exists, the script first renames the existing dated backup to a temporary old-backup name, renames the verified temporary copy to the dated backup name, then verifies the renamed backup again. It only drops the old backup after the replacement backup passes verification. If the replacement fails verification, the script attempts to restore the previous dated backup and preserves questionable artifacts for inspection.
6. **Create the staging table.** It clones the target table structure without rows, then changes the mapped output columns to `VARCHAR2` with the required lengths. It verifies the staging schema before writing transformed rows.
7. **Write and validate staging data.** Spark writes the transformed dataset to the staging table. The script verifies the staged row count against the original source count, rereads the staged data, validates converted values, and checks the staging schema.
8. **Swap tables.** Only after the backup and staging checks pass, the script renames the original target table to a temporary old-table name and renames the staging table to the target table's original name.
9. **Verify the replacement.** It checks the final row count and the mapped columns' `VARCHAR2` schema. If the checks fail, it attempts to restore the original table name and retains the converted table under the staging name for diagnosis.
10. **Remove the old table after success.** Only after the new target passes the post-swap checks does the script drop the temporary old table. The dated backup remains available.

## 4. Backup names and examples

The date suffix is generated from the run date (`YYYYMMDD`).

| Object | POS example | ATM example |
|---|---|---|
| Target | `ETL_PTLFX_01` | `ETL_TLFX_01` |
| Temporary backup | `ETL_PTLFX_01_BAK_TMP` | `ETL_TLFX_01_BAK_TMP` |
| Dated backup | `ETL_PTLFX_01_BAK_20261009` | `ETL_TLFX_01_BAK_20261009` |
| Old dated backup during refresh | `ETL_PTLFX_01_BAK_OLD` | `ETL_TLFX_01_BAK_OLD` |
| Staging table | `ETL_PTLFX_01_CVT_TMP` | `ETL_TLFX_01_CVT_TMP` |
| Original table during swap | `ETL_PTLFX_01_SWAP_OLD` | `ETL_TLFX_01_SWAP_OLD` |

The examples use 9 October 2026 as an illustration; the actual suffix is generated at runtime.

Oracle identifiers are limited to 30 characters in the helper's generated-name logic. The script truncates the base table name as needed when constructing suffixes.

## 5. Run procedure

### Step A — Confirm the source

- Confirm the target contains freshly reloaded raw values.
- Confirm no earlier conversion run has irreversibly changed encoded time values.
- Confirm the configured table and Oracle connection settings are correct.
- Confirm the active POS/ATM conversion map contains only the intended columns.

### Step B — Dry run

Set:

```properties
backup.verified=false
job.dry_run=true
```

Run the relevant Spark job. Review:
- the reported table name and row count;
- any source validation errors;
- the displayed sample converted values;
- negative amounts and their signs;
- the required date/time output formats.

A failed dry run should be investigated before production. Source-validation failure means the script exits before backup creation or target replacement.

### Step C — Production run

Only after the dry run succeeds and its output is reviewed, set:

```properties
job.dry_run=false
backup.verified=true
```

Run the job and monitor each reported stage. Do not interrupt the process during backup refresh or table swap unless necessary to stop a harmful operation.

### Step D — Post-run verification

Verify:
- the target row count matches the pre-run count;
- mapped columns have the intended `VARCHAR2` types and lengths;
- representative converted dates, timestamps, and amounts are correct;
- negative amounts retain their sign;
- the dated backup exists and has the expected row count and schema.

Keep the dated backup until the converted target has been independently verified and approved.

## 6. Recovery artifacts and failed runs

The script intentionally stops if it finds certain work tables from a prior run. Do not delete these automatically: they may contain recoverable data.

Possible artifacts include:
- `_BAK_TMP`: temporary backup;
- `_BAK_OLD`: previous dated backup retained during backup refresh;
- `_CVT_TMP`: staging or failed converted table;
- `_SWAP_OLD`: original target retained during a table swap;
- `_BAK_BAD`: questionable backup preserved after verification failure.

Inspect the artifact, row count, schema, and run logs before deciding whether to rename, preserve, or remove it. If a run fails after the backup was created, the dated backup may be available, but the exact recovery action depends on the point of failure.

## 7. What `backup.verified=true` does not mean

Do not interpret this setting as:
- proof that an operator manually checked a backup beforehand;
- permission to skip runtime backup creation;
- permission to skip row-count or schema checks;
- a guarantee that every Oracle object dependency is restored by a table rename.

In the current helper, this property only causes an informational message when true. Runtime backup creation and verification are performed independently when `job.dry_run=false`.

## 8. Operational cautions

- Take account of Oracle DDL semantics: Oracle DDL statements such as `CREATE TABLE`, `ALTER TABLE`, and `DROP TABLE` implicitly commit. The workflow uses verification and recovery names rather than relying on a single rollback transaction.
- A table rename may affect grants, synonyms, triggers, indexes, constraints, dependencies, or other objects depending on how they are defined. Review those dependencies in the target environment before production use.
- Run with an account that has the required privileges to read, create, alter, rename, and drop the relevant tables.
- Do not run both POS and ATM conversions concurrently unless the operational plan explicitly allows it.
- Store the dated backup for the required retention period and verify it can be queried before relying on it for recovery.
