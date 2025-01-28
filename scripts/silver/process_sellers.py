from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Importa funções compartilhadas
from utils.shared_functions import read_data, write_data

def map_region(state_column):
    return (
        when(state_column.isin("AC", "AP", "AM", "PA", "RO", "RR", "TO"), "Norte")
        .when(state_column.isin("AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"), "Nordeste")
        .when(state_column.isin("DF", "GO", "MS", "MT"), "Centro-Oeste")
        .when(state_column.isin("ES", "MG", "RJ", "SP"), "Sudeste")
        .when(state_column.isin("PR", "RS", "SC"), "Sul")
        .otherwise("Unknown")
    )

def process_sellers(glueContext):
    # Lê dados da camada Bronze
    df_sellers = read_data(glueContext, "olist_project_data_bronze", "bronze_olist_sellers_dataset_csv")

    # Tratamentos e enriquecimentos
    df_sellers_cleaned = df_sellers.dropDuplicates(["seller_id"]).fillna({
        "seller_city": "unknown",
        "seller_state": "unknown"
    }).drop("seller_zip_code_prefix").withColumnRenamed("seller_city", "city") \
                                     .withColumnRenamed("seller_state", "state")

    df_sellers_cleaned = df_sellers_cleaned.withColumn("region", map_region(col("state")))

    # Escreve na camada Silver
    output_path = "s3://olist-data-project/silver/parquet/olist_sellers_cleaned"
    write_data(df_sellers_cleaned, output_path, partitionKeys=["region", "state"])

# Configuração inicial do Glue Context
glueContext = GlueContext(SparkSession.builder.getOrCreate())
process_sellers(glueContext)
