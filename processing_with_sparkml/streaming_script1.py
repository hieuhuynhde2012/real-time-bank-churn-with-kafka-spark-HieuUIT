import sys
import os
import json
import numpy as np
import pandas as pd
from kafka import KafkaProducer
import pg8000
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel

# Cấu hình môi trường PySpark
os.environ["PYSPARK_PYTHON"] = "c:/Users/PC/Desktop/Do_an_Big_data/PythonCodes/henv/Scripts/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["PYSPARK_PYTHON"]

# Khởi tạo SparkSession
def init_spark():
    return SparkSession.builder \
        .master("local") \
        .appName("ChurnPredictionStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

# Định nghĩa schema cho dữ liệu đầu vào
schema = StructType([
    StructField("CustomerId", IntegerType(), True),
    StructField("Surname", StringType(), True),
    StructField("CreditScore", IntegerType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Tenure", IntegerType(), True),
    StructField("Balance", DoubleType(), True),
    StructField("NumOfProducts", IntegerType(), True),
    StructField("HasCrCard", IntegerType(), True),
    StructField("IsActiveMember", IntegerType(), True),
    StructField("EstimatedSalary", DoubleType(), True),
    StructField("BalanceSalary", DoubleType(), True),
    StructField("TenureAge", DoubleType(), True),
    StructField("ScoreAge", DoubleType(), True),
    StructField("tenure_age", DoubleType(), True),
    StructField("tenure_salary", DoubleType(), True),
    StructField("score_age", DoubleType(), True),
    StructField("score_salary", DoubleType(), True),
    StructField("newAge", IntegerType(), True),
    StructField("newCreditScore", IntegerType(), True),
    StructField("AgeScore", IntegerType(), True),
    StructField("BalanceScore", IntegerType(), True),
    StructField("SalaryScore", IntegerType(), True),
    StructField("newEstimatedSalary", DoubleType(), True),
    StructField("score_balance", DoubleType(), True),
    StructField("age_balance", DoubleType(), True),
    StructField("balance_salary", DoubleType(), True),
    StructField("age_hascrcard", DoubleType(), True),
    StructField("product_utilization_rate_by_year", DoubleType(), True),
    StructField("product_utilization_rate_by_salary", DoubleType(), True),
    StructField("countries_monthly_average_salaries", DoubleType(), True),
    StructField("Germany", BooleanType(), True),
    StructField("Spain", BooleanType(), True),
    StructField("Female", BooleanType(), True),
    StructField("Male", BooleanType(), True)
])

# Class để load mô hình Random Forest
class ModelLoader:
    MODEL_PATH = "C:/Users/PC/Desktop/Do_an_Big_data/PythonCodes/work/models/random_forest_on_sparkml"
    
    def __init__(self, spark):
        self.model = RandomForestClassificationModel.load(self.MODEL_PATH)

# Cấu hình Kafka Producer
def init_kafka_producer():
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )

# Cấu hình PostgreSQL
PG_CONFIG = {
    "host": "localhost",
    "user": "postgres",
    "password": "1234",
    "database": "churn_prediction",
    "port": 5432
}
TABLE_NAME = "churn_prediction"

from pyspark.ml.feature import StandardScaler

# Hàm xử lý batch dữ liệu streaming
def process_batch(spark, model_loader, producer):
    # Đọc dữ liệu từ Kafka
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "producerrf") \
        .load()

    # Parse dữ liệu JSON theo schema
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as raw_data") \
        .select(from_json(col("raw_data"), schema).alias("data")).select("data.*")

    # Hàm xử lý từng batch
    def process(batch_df, batch_id):
        try:
            if batch_df.count() == 0:
                print(f"Batch {batch_id}: No data")
                return

            # Chuyển đổi các cột boolean thành integer
            boolean_cols = ["Germany", "Spain", "Female", "Male"]
            for col_name in boolean_cols:
                batch_df = batch_df.withColumn(col_name, col(col_name).cast("int"))

            # Chọn các cột feature (loại bỏ CustomerId và Surname)
            feature_cols = [c for c in batch_df.columns if c not in ["CustomerId", "Surname"]]
            df_filled = batch_df.fillna(0)

            # Tạo vector features
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
            df_features = assembler.transform(df_filled)

            # Chuẩn hóa dữ liệu để tạo cột "scaled_features"
            scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
            scaler_model = scaler.fit(df_features)
            df_scaled = scaler_model.transform(df_features)

            # Dự đoán bằng mô hình Random Forest
            df_result = model_loader.model.transform(df_scaled) \
                .withColumn("ChurnPrediction", col("prediction")) \
                .drop("features", "scaled_features", "rawPrediction", "probability", "prediction")

            # Gửi kết quả đi
            send_to_kafka_and_db(df_result, batch_id, producer)
            
            print(f"Batch {batch_id} processed successfully")
            df_result.show(5, truncate=False)

        except Exception as e:
            print(f"Error in batch {batch_id}: {str(e)}")

    return parsed_df.writeStream \
        .foreachBatch(process) \
        .outputMode("append") \
        .start()

# Hàm gửi kết quả đến Kafka và PostgreSQL
def send_to_kafka_and_db(df, batch_id, producer):
    try:
        conn = pg8000.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        # Lấy danh sách cột từ schema và thêm cột ChurnPrediction
        columns = [field.name.lower() for field in schema.fields] + ["churnprediction"]
        
        # Chuyển DataFrame thành danh sách các bản ghi
        records = df.collect()

        for row in records:
            # Chuyển row thành dictionary với tên cột in thường
            message = {col.lower(): row[col] for col in row.asDict().keys()}
            
            # Gửi đến Kafka
            producer.send("predictionrf", value=message)
            
            # Chuẩn bị giá trị cho PostgreSQL
            values = tuple(message.get(col, None) for col in columns)
            
            # Debug: In giá trị trước khi gửi vào PostgreSQL
            print(f"Batch {batch_id} - Values to PostgreSQL: {values}")
            
            # Tạo câu lệnh SQL
            placeholders = ','.join(['%s'] * len(columns))
            sql = f"INSERT INTO {TABLE_NAME} ({','.join(columns)}) VALUES ({placeholders})"
            cursor.execute(sql, values)

        conn.commit()
        print(f"Batch {batch_id} saved to Kafka and PostgreSQL")
        
    except Exception as e:
        print(f"Error saving batch {batch_id}: {str(e)}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# Hàm main
def main():
    # Khởi tạo Spark và các component
    spark = init_spark()
    model_loader = ModelLoader(spark)
    producer = init_kafka_producer()
    
    # Bắt đầu xử lý stream
    query = process_batch(spark, model_loader, producer)
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
    finally:
        producer.close()
        spark.stop()

if __name__ == "__main__":
    main()