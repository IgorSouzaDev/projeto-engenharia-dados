import requests
import os
from dotenv import load_dotenv
from pyspark.sql  import functions as F
from pyspark.sql import SparkSession  
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType  
import json

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
    

extracao()    
dados = extracao()
df=spark.read.json(spark.sparkContext.parallelize([dados]))