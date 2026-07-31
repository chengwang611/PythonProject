# Databricks notebook source
# ---------------------------------------------------------------------------
# hello_world.py — Spark smoke test
# ---------------------------------------------------------------------------
# Minimal hello-world PySpark script that can be run directly from a
# Databricks Repo as a notebook (or locally with PySpark).
#
# It exercises the shared Spark session helper from
#   src/bank_etl/utils/spark_utils.py -> get_spark_session()
# and then runs a tiny createDataFrame -> show -> SQL demo to confirm the
# cluster is healthy.
#
# How to run on Databricks:
#   1. Add this repo to a Databricks workspace (Repos -> Add Repo).
#   2. Open  src/bank_etl/test/hello_world.py  in the repo.
#   3. Attach a running cluster and click "Run".
# ---------------------------------------------------------------------------


import os
import sys
from pathlib import Path



def _bootstrap_src_path() -> Path:
    """Add <repo-root>/src to sys.path so `import bank_etl` resolves.

    The package lives in a src-layout (src/bank_etl/...), and Databricks
    does not add `src/` to sys.path automatically when running a repo
    notebook. This helper supports two execution modes:

    * Local script / pytest  -> derived from __file__ (parents[2] is src/).
    * Databricks Repo notebook -> derived from dbutils notebookPath
      (e.g. /Repos/<user>/<repo>/src/bank_etl/test/hello_world).
    """
    src_dir = None

    # Mode 1: __file__ is available (local runs).
    try:
        this_file = Path(__file__).resolve()
        # .../src/bank_etl/test/hello_world.py -> parents[2] == <repo>/src
        src_dir = this_file.parents[2]
    except NameError:
        pass

    # Mode 2: running as a Databricks Repo notebook (no __file__).
    if src_dir is None:
        try:
            notebook_path = (
                dbutils.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .notebookPath()
                .get()
            )
            # Strip any run-instance suffix (e.g. ".../hello_world.py#12345").
            notebook_path = notebook_path.split("#")[0]
            parts = list(Path(notebook_path).parts)
            idx = parts.index("src")
            repo_root = os.path.join(*parts[:idx])
            src_dir = Path(repo_root) / "src"
        except Exception as exc:  # noqa: BLE001 - best-effort bootstrap
            print(f"[hello_world] Could not derive repo src path from dbutils: {exc}")

    if src_dir is not None and str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))

    return src_dir



SRC_DIR = _bootstrap_src_path()

# Shared Spark session builder from the ETL package.
from bank_etl.utils.spark_utils import get_spark_session  # noqa: E402

spark = get_spark_session(app_name="bank-etl-hello-world")

print("=" * 70)
print("Hello, Databricks! Spark is up and running.")
print(f"  Repo src path : {SRC_DIR}")
print(f"  Spark version : {spark.version}")
print(f"  App name      : {spark.conf.get('spark.app.name')}")
print(f"  Master        : {spark.sparkContext.master}")
print("=" * 70)

# --- Minimal PySpark demo ---------------------------------------------------
rows = [
    ("Hello", "World", 1),
    ("Hello", "Databricks", 2),
    ("Hello", "PySpark", 3),
]
df = spark.createDataFrame(rows, ["greeting", "target", "id"])
df.show(truncate=False)

df.createOrReplaceTempView("hello_world")

spark.sql(
    "SELECT greeting, count(*) AS cnt, collect_list(target) AS targets "
    "FROM hello_world GROUP BY greeting"
).show(truncate=False)

print(f"Rows processed: {df.count()}")
print("Hello world smoke test completed successfully.")
