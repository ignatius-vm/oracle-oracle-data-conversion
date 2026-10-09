#!/usr/bin/env python3
"""ATM conversion for INSUSER.ETL_TLFX_01.

ATM amount fields AMT_1 and AMT_2, AMT_3 are divided by 100 and formatted with
exactly two decimal places.
Mapped output columns remain VARCHAR2.
"""
import sys
from oracle_inplace_conversion import ATM_CONVERSION_MAP, load_properties, required, run_job

if __name__ == "__main__":
    properties = sys.argv[1] if len(sys.argv) > 1 else "application.properties"
    props = load_properties(properties)
    table = required(props, "atm.table")
    if table.upper() != "INSUSER.ETL_TLFX_01":
        raise ValueError("atm.table must be INSUSER.ETL_TLFX_01")
    run_job(
        "ATM Oracle VARCHAR2 Conversion",
        table,
        ATM_CONVERSION_MAP,
        {"TRN_ENTY_TIME": "DATE_TRN_BGN"},
        properties,
    )
