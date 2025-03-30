import sys
import os
import json
import numpy as np
import pandas as pd
import joblib
from kafka import KafkaProducer
import pg8000
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import DenseVector

# Cấu hình môi trường
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

# Định nghĩa schema
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

# Load mô hình và scaler
class ModelLoader:
    MODEL_PATH = "C:/Users/PC/Desktop/Do_an_Big_data/PythonCodes/work/models/svm/svm_model.pkl"
    SCALER_PATH = "C:/Users/PC/Desktop/Do_an_Big_data/PythonCodes/work/models/svm/sc.pkl"
    
    def __init__(self):
        self.model = joblib.load(self.MODEL_PATH)
        self.scaler = joblib.load(self.SCALER_PATH)

# Kafka Producer configuration
def init_kafka_producer():
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',  # Đảm bảo message được gửi thành công
        retries=3    # Thử lại khi gửi thất bại
    )

# PostgreSQL configuration (loại bỏ 'table' khỏi kết nối)
PG_CONFIG = {
    "host": "localhost",
    "user": "postgres",
    "password": "1234",
    "database": "churn_prediction",
    "port": 5432
}
TABLE_NAME = "churn_predictions"

# Định nghĩa UDF dự đoán
def create_predict_udf(model):
    def predict(features):
        features = DenseVector(features)
        return float(model.predict([features])[0])
    return udf(predict, DoubleType())

# Định nghĩa hàm chuẩn hóa
def scale_features_udf(scaler):
    def scale(features):
        features_array = np.array(features).reshape(1, -1)
        scaled = scaler.transform(features_array)
        return scaled[0].tolist()
    return udf(scale, ArrayType(DoubleType()))

def process_batch(model_loader, producer):
    spark = init_spark()
    predict_udf = create_predict_udf(model_loader.model)
    scale_udf = scale_features_udf(model_loader.scaler)
    
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "producer_svm") \
        .load()

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as raw_data") \
        .select(from_json(col("raw_data"), schema).alias("data")).select("data.*")

    def process(batch_df, batch_id):
        try:
            if batch_df.count() == 0:
                print(f"Batch {batch_id}: No data")
                return

            # Chuẩn bị dữ liệu
            boolean_cols = ["Germany", "Spain", "Female", "Male"]
            for col_name in boolean_cols:
                batch_df = batch_df.withColumn(col_name, col(col_name).cast("int"))

            feature_cols = [c for c in batch_df.columns if c not in ["CustomerId", "Surname"]]
            df_filled = batch_df.fillna(0)

            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
            
            # Pipeline xử lý
            df_features = assembler.transform(df_filled)
            df_scaled = df_features.withColumn("scaled_features", scale_udf(col("features")))
            df_result = df_scaled.withColumn("ChurnPrediction", predict_udf(col("scaled_features"))) \
                               .drop("features", "scaled_features")

            # Gửi kết quả
            send_to_kafka_and_db(df_result, batch_id, producer)
            
            print(f"Batch {batch_id} processed successfully")
            df_result.show(5, truncate=False)

        except Exception as e:
            print(f"Error in batch {batch_id}: {str(e)}")

    return parsed_df.writeStream \
        .foreachBatch(process) \
        .outputMode("append") \
        .start()

def send_to_kafka_and_db(df, batch_id, producer):
    try:
        # Kết nối PostgreSQL (không có 'table' trong connect)
        conn = pg8000.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        # Danh sách cột khớp với schema và thêm ChurnPrediction
        columns = [field.name.lower() for field in schema.fields] + ["churnprediction"]
        records = df.collect()

        for row in records:
            message = row.asDict()
            
            # Gửi đến Kafka
            producer.send("prediction_svm", value=message)
            
            # Chèn vào PostgreSQL
            values = tuple(message.get(col.capitalize(), None) for col in columns)
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
            
def main():
    model_loader = ModelLoader()
    producer = init_kafka_producer()
    
    query = process_batch(model_loader, producer)
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
    finally:
        producer.close()

if __name__ == "__main__":
    main()