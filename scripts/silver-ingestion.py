# Importação de bibliotecas para manipulação de dados, strings e logging
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, upper, udf, current_timestamp
import re
import unicodedata
from delta import *
import pyspark
from datetime import datetime
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Função para configurar o contexto do Spark
def configurar_contexto_spark():
    try:
        builder = pyspark.sql.SparkSession.builder.appName("Ingestao - Silver") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        return configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as e:
        logging.error(f"Erro ao configurar Spark. Detalhes: {str(e)}")
        raise

# Função para limpar strings de caracteres malformados
def limpar_string(entrada:str) -> str:
    '''
    Limpa dados malformados
    '''
    if entrada is not None:
        nfkd_form = unicodedata.normalize('NFKD', entrada)
        string_limpa = u"".join([c for c in nfkd_form if not unicodedata.combining(c)])
        # Substitui caracteres especiais por '_'
        return re.sub(r'[^a-zA-Z0-9 ]', '_', string_limpa)
    return entrada

if __name__ == '__main__':
    # Configura contexto Spark
    spark = configurar_contexto_spark()

    # Registra a UDF para limpeza de strings
    limpar_string_udf = udf(limpar_string, StringType())

    # Cria tabela Delta na camada Silver, se ainda não existir
    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver_brewery_list (
            id STRING,
            name STRING,
            brewery_type STRING,
            street STRING,
            address_2 STRING,
            address_3 STRING,
            city STRING,
            postal_code STRING,
            country STRING,
            longitude STRING,
            latitude STRING,
            phone STRING,
            website_url STRING,
            state STRING,
            dt_ingestion timestamp
        ) 
        USING DELTA
        PARTITIONED BY (state)
        LOCATION '/datalake/silver/breweries/'
    """)

    # Define o caminho dos arquivos na camada bronze
    caminho_bronze = '/datalake/bronze/breweries/'
    hoje = datetime.now()
    ano, mes, dia = str(hoje.year), str(hoje.month).zfill(2), str(hoje.day).zfill(2)
    caminho_arquivo_bronze = f"{caminho_bronze}{ano}/{mes}/{dia}/*.json"

    # Leitura dos dados da camada Bronze
    try:
        df = spark.read.json(caminho_arquivo_bronze, multiLine=True)
        logging.info(f"Número de linhas nos dados brutos: {df.count()}")
    except Exception as e:
        logging.error(f"Erro ao ler dados brutos. Caminho: {caminho_arquivo_bronze}. Detalhes: {str(e)}")
        raise

    # Limpa coluna 'state' removendo caracteres especiais
    try:
        df_limpo = df.withColumn('state', limpar_string_udf(col('state')))
    except Exception as e:
        logging.error(f"Erro ao usar UDF 'limpar_string_udf'. Detalhes: {str(e)}")
        raise

    # Remove linhas com valores nulos em colunas importantes
    try:
        df_filtrado = df_limpo.filter(
            (col("state").isNotNull()) & (col("state") != '') &
            (col("id").isNotNull()) & (col("id") != '') &
            (col("brewery_type").isNotNull()) & (col("brewery_type") != '')
        )
    except Exception as e:
        logging.error("Erro ao remover valores nulos dos dados brutos. Detalhes: {str(e)}")
        raise

    # Transforma valores da coluna 'state' para maiúsculo
    estado_normalizado = df_filtrado.withColumn("state", upper(col("state")))

    # Seleciona colunas finais e adiciona coluna de timestamp
    try:
        df_final = estado_normalizado.select(
            "id",
            "name",
            "brewery_type",
            "street",
            "address_2",
            "address_3",
            "city",
            "postal_code",
            "country",
            "longitude",
            "latitude",
            "phone",
            "website_url",
            "state"
        ).withColumn("dt_ingestion", current_timestamp())
    except Exception as e:
        logging.error(f"Erro ao selecionar colunas no DataFrame final. Detalhes: {str(e)}")
        raise

    # Escreve os dados transformados na camada Silver
    try:
        df_final.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("state") \
            .saveAsTable('silver_brewery_list')
        logging.info("Dados salvos com sucesso na camada Silver.")
    except Exception as e:
        logging.error(f"Erro ao salvar dados na camada Silver. Detalhes: {str(e)}")
        raise
