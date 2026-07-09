"""Pipeline for exporting a text file from MS SQL to MVS via FTPS.

Steps:
- read MS SQL table (Id, RecordLine)
- sort by Id
- write all RecordLine values to a single text file encoded as cp1047
- encrypt the file with a public key
- upload encrypted file to MVS via FTPS
"""
from typing import Dict, Any, Iterable
from pyspark.sql import SparkSession
from . import io_util
from . import export_util  # type: ignore[attr-defined]


def run_mssql_to_mvs_export(
    spark: SparkSession,
    mssql_config: Dict[str, Any],
    table: str,
    plain_text_path: str,
    encrypted_path: str,
    ftps_config: Dict[str, Any],
    encryption_config: Dict[str, Any],
) -> Dict[str, Any]:
    """End-to-end export from MS SQL to MVS.

    Returns a small status dict for logging/tests.
    """
    df = io_util.read_mssql_table_native(spark, mssql_config, table)

    # Normalize column names (case-insensitive match for Id/RecordLine)
    cols_lower = {c.lower(): c for c in df.columns}
    id_col = cols_lower.get("id")
    line_col = cols_lower.get("recordline")
    if not id_col or not line_col:
        raise ValueError(f"Expected Id and RecordLine columns in table {table}, got {df.columns}")

    df_sorted = df.select(id_col, line_col).orderBy(id_col)

    # Collect RecordLine values lazily
    recordlines: Iterable[str] = (r[line_col] for r in df_sorted.toLocalIterator())

    export_util.write_recordlines_cp1047(recordlines, plain_text_path)

    public_key_path = encryption_config["public_key_path"]
    enc_type = encryption_config.get("type", "pgp")
    if enc_type == "pgp":
        export_util.encrypt_file_with_pgp(plain_text_path, encrypted_path, public_key_path)
    elif enc_type == "openssl":
        export_util.encrypt_file_with_openssl(plain_text_path, encrypted_path, public_key_path)
    else:
        raise ValueError(f"Unsupported encryption type: {enc_type}")

    # FTPS upload
    export_util.ftps_upload_file(
        host=ftps_config["host"],
        port=ftps_config.get("port", 21),
        username=ftps_config["username"],
        password=ftps_config["password"],
        local_path=encrypted_path,
        remote_path=ftps_config["remote_path"],
        use_tls_explicit=ftps_config.get("use_tls_explicit", True),
        passive=ftps_config.get("passive", True),
    )

    return {
        "rows": df_sorted.count(),
        "plain_text_path": plain_text_path,
        "encrypted_path": encrypted_path,
        "remote_path": ftps_config["remote_path"],
    }
