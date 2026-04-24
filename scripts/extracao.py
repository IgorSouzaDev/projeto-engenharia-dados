import requests
import os
from dotenv import load_dotenv
from pyspark.sql  import functions as F
from pyspark.sql import SparkSession  
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType  


load_dotenv()
spark = SparkSession.builder.appName('extracao').getOrCreate()

url = os.getenv("url")


def extracao():
    try:
        if url:
            response = requests.get(url)
            if response.status_code == 200:
                dados = response.json()
                print("✅ Extração bem sucedida!")
                return dados
            else:
                print(f"Erro ao acessar a URL: {response.status_code}")
        else:
            print("Erro")
    except Exception as e:
        print(f"Erro ao acessar a URL: {e}")
    


def ingestion_raw(df):
    try:

        df=spark.read.json(spark.sparkContext.parallelize([dados]))
        df.write\
        .mode("overwrite")\
        .parquet("raw_data/dados_raw.parquet")
        print("✅ Ingestão bem sucedida!")
    except Exception as e:
        print(f"Erro ao salvar os dados: {e}")


dados = extracao()
ingestion_raw(dados)

