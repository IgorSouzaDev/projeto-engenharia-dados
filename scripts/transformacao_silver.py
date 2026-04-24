from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType  




def transformacao_silver(): 
    try:
        spark=(
            SparkSession.builder
            .master("local")
            .appName("Kaglle")
            .getOrCreate()
        )

        
        schema_rating= StructType([
            StructField("count", IntegerType(),True),
            StructField("rate", DoubleType(), True)
        ])

        schema= StructType([
                StructField("category", StringType(), True),
                StructField("description", StringType(), True),
                StructField("id", IntegerType(), True),
                StructField("image", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("rating", schema_rating, True),
                StructField("title", StringType(), True)

            ])

        df = spark.read.parquet("raw_data/dados.parquet",schema=schema)

        print(df.printSchema())
        df = df.withColumn("avaliacoes", F.col("rating.count")) \
                    .withColumn("rate", F.col("rating.rate")) \
                    .drop("rating")

        df.write\
        .mode("overwrite")\
        .parquet("silver_data/dados_silver.parquet")
        print("✅ Transformação para Silver bem sucedida!")
    except Exception as e:
        print(f"Erro na transformação para Silver: {e}")


transformacao_silver()
