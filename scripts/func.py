import logging
from pyspark.sql.functions import count, col, isnan, when

# Verifica se o dataframe está vazio
def check_empty_df(df) -> None:
    # Retorna falso se vazio, senão True
    if df.rdd.isEmpty():
        logging.warning("Dataframe vazio.")  # Aviso que é vazio
        return False
    return True


# Verifica se faltam colunas esperadas
def check_missing_columns(columns_expected: list, columns: list) -> bool:
    '''
    Verifica se todas as colunas esperadas estão presentes no DF.
    columns_expected: lista de colunas esperadas
    columns: lista real
    '''
    missing_columns = [col for col in columns_expected if col not in columns]

    if missing_columns:
        logging.warning(f"Colunas ausentes: {missing_columns}")  # Se faltar, loga e retorna falso
        return False
    return True


# Valida linhas duplicadas
def check_only_unique_rows(df, columns: list) -> bool:
    '''
    Verifica duplicatas. Gera erro se tiver duplicadas.
    '''
    # Agrupa por colunas e checa se tem mais de 1
    df_duplicate_rows = df.groupBy(columns).agg(count('*').alias('quantity')).filter(col('quantity') > 1)
    if check_empty_df(df_duplicate_rows):  # Usa função de DF vazio
        logging.error(f"Linhas duplicadas encontradas: {columns}")  # Duplicadas detectadas
        return False
    else:
        logging.info(f"Sem duplicatas nas colunas: {columns}")  # Tudo certo
        return True


# Checa valores nulos
def check_null_values(df, columns: list) -> bool:
    '''
    Confere se tem nulos nas colunas.
    df: dataframe
    columns: lista de colunas
    '''
    try:
        exprs = [count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in columns]
        null_counts = df.select(exprs).collect()[0]

        # Retorna se tem nulos
        return any(null_count > 0 for null_count in null_counts)
    except Exception as e:
        logging.error(f"Erro verificando valores nulos: {str(e)}")  # Erro genérico
        raise

# Fim do código
