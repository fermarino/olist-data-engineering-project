from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame

def read_data(glueContext, database, table_name):
    """Função para ler dados do Glue Data Catalog."""
    return glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table_name
    ).toDF()

def write_data(df: DataFrame, path: str, partitionKeys=None):
    """Escreve DataFrame no S3 no formato Parquet."""
    if partitionKeys:
        df.write.partitionBy(*partitionKeys).mode("overwrite").parquet(path)
    else:
        df.write.mode("overwrite").parquet(path)
