FROM apache/airflow:3.2.1

USER root
# Instala o Java (JRE) necessário para o PySpark
RUN apt-get update \
  && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
# Instala as bibliotecas Python
RUN pip install --no-cache-dir pyspark requests python-dotenv
