import os
from pyspark.sql import DataFrame


def write_redshift_table(df: DataFrame, schema: str, table: str) -> None:
    """Write a Spark DataFrame to Redshift using JDBC configuration from environment."""
    jdbc_url = os.environ["REDSHIFT_JDBC_URL"]
    user = os.environ["REDSHIFT_USER"]
    password = os.environ["REDSHIFT_PASSWORD"]
    driver = os.environ.get("REDSHIFT_DRIVER", "com.amazon.redshift.jdbc42.Driver")

    (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"{schema}.{table}")
        .option("user", user)
        .option("password", password)
        .option("driver", driver)
        .mode("overwrite")
        .save()
    )
