# Script Unificado para Ingestão e Validação
import logging
import requests
import json
import os
from datetime import datetime
from delta import *
import pyspark
import func as dq

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# CONFIGURAÇÃO DO SPARK
try:
    spark_builder = pyspark.sql.SparkSession.builder \
        .appName("Ingestao e Validacao - Raw") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark_session = configure_spark_with_delta_pip(spark_builder).getOrCreate()
except Exception as spark_error:
    logging.error(f"Erro ao configurar o Spark: {str(spark_error)}")
    raise


# ENDPOINTS E VARIÁVEIS
API_ENDPOINT = "https://api.openbrewerydb.org/breweries"
META_ENDPOINT = "https://api.openbrewerydb.org/v1/breweries/meta"
DATA_PATH = "/datalake/bronze/breweries/"
EXPECTED_COLUMNS = [
    "id", "name", "brewery_type", "address_2", "address_3", "city", 
    "postal_code", "country", "longitude", "latitude", "phone", 
    "website_url", "state", "street"
]


# FUNÇÃO PARA CRIAR E ESCREVER ARQUIVOS JSON
def save_to_json(data, base_dir, file_name):
    try:
        today = datetime.now()
        folder_path = os.path.join(base_dir, str(today.year), str(today.month).zfill(2), str(today.day).zfill(2))
        os.makedirs(folder_path, exist_ok=True)
        
        file_path = os.path.join(folder_path, f"{file_name}.json")
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
        
        logging.info(f"Arquivo salvo: {file_path}")
    except Exception as write_error:
        logging.error(f"Erro ao salvar o arquivo: {str(write_error)}")
        raise


# COLETA DE DADOS DA API
def fetch_api_data():
    try:
        response = requests.get(META_ENDPOINT)
        response_data = response.json()
        total_items = int(response_data.get("total", 0))
        logging.info(f"Total de itens disponíveis: {total_items}")
        
        collected = 0
        page = 1
        while collected < total_items:
            page_url = f"{API_ENDPOINT}?page={page}&per_page=200"
            page_response = requests.get(page_url)
            if page_response.status_code == 200:
                save_to_json(page_response.json(), DATA_PATH, f"pagina_breweries_{page}")
                collected += len(page_response.json())
                page += 1
            else:
                logging.error(f"Erro na API: {page_url} retornou status {page_response.status_code}")
                raise Exception("Falha ao fazer request para a API.")
    except Exception as api_error:
        logging.error(f"Erro ao buscar dados: {str(api_error)}")
        raise


# VALIDAÇÃO DE DADOS
def validate_data():
    try:
        today = datetime.now()
        file_path = f"{DATA_PATH}{today.year}/{str(today.month).zfill(2)}/{str(today.day).zfill(2)}/*.json"
        data_frame = spark_session.read.json(file_path, multiLine=True)
        
        validation_results = {
            "empty_check": dq.check_empty_df(data_frame),
            "missing_columns": dq.check_missing_columns(EXPECTED_COLUMNS, data_frame.columns)
        }
        
        if all(validation_results.values()):
            logging.info("Validação feita com sucesso!")
        else:
            logging.error("Validação falhou, alguns problemas foram encontrados.")
            raise Exception("Problemas nos dados detectados durante a validação.")
    except Exception as validation_error:
        logging.error(f"Erro na validação: {str(validation_error)}")
        raise


# EXECUÇÃO DO PROCESSO
if __name__ == "__main__":
    fetch_api_data()
    validate_data()
