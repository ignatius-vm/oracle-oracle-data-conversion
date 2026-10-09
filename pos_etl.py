#!/usr/bin/env python3
"""POS conversion for INSUSER.ETL_PTLFX_01.

POS amount fields AMT_1, AMT_2 are divided by 100 and formatted
with exactly two decimal places. Mapped output columns remain VARCHAR2.
"""
import sys
from oracle_inplace_conversion import POS_CONVERSION_MAP, load_properties, required, run_job

if __name__ == "__main__":
    properties = sys.argv[1] if len(sys.argv) > 1 else "application.properties"
    props = load_properties(properties)
    table = required(props, "pos.table")
    if table.upper() != "INSUSER.ETL_PTLFX_01":
        raise ValueError("pos.table must be INSUSER.ETL_PTLFX_01")
    run_job(
        "POS Oracle VARCHAR2 Conversion",
        table,
        POS_CONVERSION_MAP,
        {"TRN_BGN_TIME": "TRN_BGN_DATE"},
        properties,
    )
