import pyspark
from pyspark.sql import SparkSession

# Kiểm tra và thiết lập phiên bản Scala và Spark
scala_version = '2.12'
spark_version = '3.2.1'

# Gói cần thiết cho Spark Kafka
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.0.0'  # Thay bằng phiên bản Kafka client tương thích
]

spark = SparkSession.builder \
    .master("local") \
    .appName("ChurnPredictionStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
    .config("spark.driver.extraJavaOptions", "-Dlog4j.debug=true") \
    .config("spark.executor.extraJavaOptions", "-Dlog4j.debug=true") \
    .getOrCreate()


# Hiển thị thông tin SparkSession
print(spark)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler
from kafka import KafkaProducer
import json

# Định nghĩa schema của dữ liệu nhận từ Kafka
schema = StructType([
    StructField("CustomerId", IntegerType(), True),  # Giữ lại CustomerId để nhận diện khách hàng
    StructField("CreditScore", IntegerType(), True),
    StructField("Geography", IntegerType(), True),  # Đã được mã hóa (France:0, Spain:1, Germany:2)
    StructField("Gender", IntegerType(), True),  # Đã được mã hóa (Male:0, Female:1)
    StructField("Age", IntegerType(), True),
    StructField("Tenure", IntegerType(), True),
    StructField("Balance", DoubleType(), True),
    StructField("NumOfProducts", IntegerType(), True),
    StructField("HasCrCard", IntegerType(), True),
    StructField("IsActiveMember", IntegerType(), True),
    StructField("EstimatedSalary", DoubleType(), True)
])

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Giải mã dữ liệu JSON từ Kafka
df_parsed = df_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Load mô hình đã huấn luyện từ thư mục 'output/bestmodel'
model = LogisticRegressionModel.load("C:/Users/PC/Desktop/Do_an_Big_data/PythonCodes/output/bestModel")

# Chuyển đổi dữ liệu đầu vào thành vector feature
feature_columns = ["CreditScore", "Geography", "Gender", "Age", "Tenure", 
                   "Balance", "NumOfProducts", "HasCrCard", "IsActiveMember", "EstimatedSalary"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
df_features = assembler.transform(df_parsed)

# Dự đoán churn (0: giữ chân, 1: rời bỏ)
df_predictions = model.transform(df_features)
# Chọn tất cả đặc trưng đầu vào cùng với kết quả dự đoán
df_results = df_predictions.select(
    "CustomerId", "CreditScore", "Geography", "Gender", "Age", "Tenure", 
    "Balance", "NumOfProducts", "HasCrCard", "IsActiveMember", "EstimatedSalary",
    col("prediction").alias("ChurnPrediction")
)

# Gửi kết quả dự đoán về Kafka topic 'churn_predictions'
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Xử lý từng batch và gửi dữ liệu về Kafka
def send_to_kafka(batch_df, batch_id):
    # Hiển thị batch DataFrame trên Jupyter Notebook
    print(f"Batch {batch_id}:")
    batch_df.show(truncate=False)  # Hiển thị toàn bộ dữ liệu
    
    # Chuyển DataFrame thành danh sách từ điển
    records = batch_df.collect()
    for row in records:
        message = {
            "CustomerId": row.CustomerId,
            "CreditScore": row.CreditScore,
            "Geography": row.Geography,
            "Gender": row.Gender,
            "Age": row.Age,
            "Tenure": row.Tenure,
            "Balance": row.Balance,
            "NumOfProducts": row.NumOfProducts,
            "HasCrCard": row.HasCrCard,
            "IsActiveMember": row.IsActiveMember,
            "EstimatedSalary": row.EstimatedSalary,
            "ChurnPrediction": int(row.ChurnPrediction)
        }
        producer.send("churn_predictions", value=message)
        print(f"Sent to Kafka: {message}")

# Ghi kết quả dự đoán ra Kafka và hiển thị lên console
query = df_results.writeStream \
    .foreachBatch(send_to_kafka) \
    .outputMode("append") \
    .start()

query.awaitTermination()
