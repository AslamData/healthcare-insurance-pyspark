from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_dataframe(df: DataFrame) -> DataFrame:
    """Normalize column names, trim strings, and drop exact duplicate rows."""
    for old_name in df.columns:
        new_name = (
            old_name.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )
        if new_name != old_name:
            df = df.withColumnRenamed(old_name, new_name)

    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            df = df.withColumn(field.name, F.trim(F.col(field.name)))

    return df.dropDuplicates()


def fill_descriptive_nulls(
    df: DataFrame,
    columns: list[str],
    placeholder: str = "NA",
) -> DataFrame:
    """Fill nulls only in descriptive text columns, not IDs or measures."""
    replacements = {name: placeholder for name in columns if name in df.columns}
    return df.fillna(replacements)
