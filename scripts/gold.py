# Pipeline Gold: Ingestão e Agregação
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, current_timestamp
from pyspark.sql.types import IntegerType
from delta import *
import logging
import pyspark
import func as dq

# Configura logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configura Spark
builder = pyspark.sql.SparkSession.builder.appName("Ingestao - Gold") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:3.0")

try:
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
except Exception as e:
    logging.error(f"Erro ao iniciar SparkSession: {e}")
    raise


# Criação da tabela Gold se não existir
spark.sql("""
    CREATE TABLE IF NOT EXISTS gold_brewery_list (
        state STRING,
        brewery_type STRING,
        brewery_quantity INT,
        dt_ingestion timestamp
    ) USING DELTA
    LOCATION '/datalake/gold/breweries/num_breweries_per_state'
""")

# Lê os dados do Silver
try:
    df_silver = spark.read.format("delta").load('/datalake/silver/breweries/')
    logging.info("Dados do Silver carregados com sucesso.")
except Exception as e:
    logging.error(f"Erro ao carregar os dados do Silver: {e}")
    raise

# Agrega os dados
try:
    df_agg = df_silver.groupBy(col("state"), col("brewery_type")) \
        .agg(count("*").alias("brewery_quantity")) \
        .withColumn("brewery_quantity", col("brewery_quantity").cast(IntegerType())) \
        .withColumn("dt_ingestion", current_timestamp())
    logging.info("Dados agregados com sucesso.")
except Exception as e:
    logging.error(f"Erro ao agregar os dados: {e}")
    raise

# Validação básica
try:
    unique_rows = dq.check_only_unique_rows(df_agg, ["state", "brewery_type"])
    dataframe_with_rows = dq.check_empty_df(df_silver)
    df_with_null_values = dq.check_null_values(df_agg, ['state', 'brewery_type', 'brewery_quantity'])

    if not dataframe_with_rows:
        logging.error("Erro: Dataframe está vazio.")
    if not unique_rows:
        logging.error("Erro: Dados duplicados encontrados.")
    if df_with_null_values:
        logging.error("Erro: Existem valores nulos nos dados.")

    if not dataframe_with_rows or not unique_rows or df_with_null_values:
        raise Exception("Erros encontrados durante as validações.")

    logging.info("Validações de qualidade feitas com sucesso.")
except Exception as e:
    logging.error(f"Erro nas validações: {e}")
    raise

# Salva os dados na camada Gold
try:
    df_agg.write.format("delta").mode("overwrite") \
        .save('/datalake/gold/breweries/num_breweries_per_state')
    logging.info("Dados salvos na camada Gold com sucesso.")
except Exception as e:
    logging.error(f"Erro ao salvar os dados na camada Gold: {e}")
    raise
 