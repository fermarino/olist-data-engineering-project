import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## Parâmetros do Glue Job (como o nome do job)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

## Inicializa o ambiente do Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

## Inicializa o job para rastreamento no Glue
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuração do nome do banco de dados em que as tabelas estão registradas no Glue Catalog
bronze_database = "olist_project_data_bronze"

# Caminho para salvar os arquivos no formato Parquet no S3
bronze_parquet_path = "s3://olist-data-project/bronze/parquet/"

# Lista de tabelas e os nomes esperados de saída
datasets = [
    {"table_name": "bronze_olist_customers_dataset_csv", "output_path": f"{bronze_parquet_path}customers/"},
    {"table_name": "bronze_olist_geolocation_dataset_csv", "output_path": f"{bronze_parquet_path}geolocation/"},
    {"table_name": "bronze_olist_order_items_dataset_csv", "output_path": f"{bronze_parquet_path}order_items/"},
    {"table_name": "bronze_olist_order_payments_dataset_csv", "output_path": f"{bronze_parquet_path}order_payments/"},
    {"table_name": "bronze_olist_order_reviews_dataset_csv", "output_path": f"{bronze_parquet_path}order_reviews/"},
    {"table_name": "bronze_olist_orders_dataset_csv", "output_path": f"{bronze_parquet_path}orders/"},
    {"table_name": "bronze_olist_products_dataset_csv", "output_path": f"{bronze_parquet_path}products/"},
    {"table_name": "bronze_olist_sellers_dataset_csv", "output_path": f"{bronze_parquet_path}sellers/"},
    {"table_name": "bronze_product_category_name_translation_csv", "output_path": f"{bronze_parquet_path}product_category_translation/"}
]

# Função que processa os dados de cada tabela do Glue Catalog
def process_bronze_table(dataset):
    table_name = dataset["table_name"]
    output_path = dataset["output_path"]

    print(f"Carregando dados da tabela `{table_name}` do Glue Catalog...")

    # Carrega os dados como um DynamicFrame a partir do Glue Catalog
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=bronze_database,
        table_name=table_name
    )

    # Converte o DynamicFrame para um Spark DataFrame para maior flexibilidade
    data_frame = dynamic_frame.toDF()

    # Validação: Conta o total de linhas carregadas
    total_linhas = data_frame.count()
    print(f"Tabela `{table_name}` carregada com sucesso. Total de linhas: {total_linhas}")

    # Salva os dados no formato Parquet no S3
    print(f"Salvando tabela `{table_name}` no formato Parquet em: {output_path}")
    data_frame.write.format("parquet").mode("overwrite").save(output_path)

    print(f"Tabela `{table_name}` salva com sucesso no formato Parquet!")

# Loop para processar todas as tabelas listadas no `datasets`
for dataset in datasets:
    process_bronze_table(dataset)

# Finaliza o Glue Job
job.commit()
print("### Job concluído com sucesso! Dados da camada Bronze processados e salvos no formato Parquet. ###")