"""Local Phase 1 smoke test: Spark session + Splink Spark backend import.

Does not load the company dataset or run entity matching.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path


def _ensure_java_home() -> None:
    if os.environ.get("JAVA_HOME"):
        return
    java = shutil.which("java")
    if java is None:
        raise RuntimeError("Java is required for local PySpark. Install a JDK and retry.")
    os.environ["JAVA_HOME"] = str(Path(java).resolve().parent.parent)


def main() -> int:
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    _ensure_java_home()

    from pyspark.sql import SparkSession
    from splink.backends.spark import SparkAPI, similarity_jar_location

    jar_path = similarity_jar_location()
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("ir-spark-smoke")
        .config("spark.jars", jar_path)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    try:
        count = spark.range(10).count()
        if count != 10:
            raise RuntimeError(f"spark.range(10).count() returned {count}, expected 10")
        db_api = SparkAPI(spark_session=spark, break_lineage_method="persist")
        print("Spark smoke: spark.range(10).count() == 10")
        print(f"Splink SparkAPI initialized: {type(db_api).__name__}")
        print(f"Similarity JAR: {jar_path}")
        print("Phase 1 local check passed.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
