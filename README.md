# Real Time Bank Churn With Kafka Spark

<p align="center">
  <img src="customer_churn_prediction.png" alt="Customer Churn Prediction">
</p>

## Table of Contents
- [Introduction](#introduction)
- [Features](#features)
- [Technologies Used](#technologies-used)
- [Setup Instructions](#setup-instructions)
- [Data Pipeline](#data-pipeline)
- [Usage](#usage)
- [Visualizations](#visualizations)
- [Conclusion](#conclusion)
- [Future Direction](#future-direction)
- [Contact](#contact)

## Introduction
This project focuses on real-time customer churn prediction in the banking sector. By leveraging Apache Kafka for data streaming, Apache Spark for real-time processing, and a trained machine learning model, we aim to predict and analyze customer churn behavior dynamically. The results are visualized using Streamlit for interactive reporting and insights.

## Features
- **Real-time Data Streaming**: Simulate and stream customer transactions using Kafka.  
- **Efficient Processing**: Utilize Apache Spark for real-time data processing.  
- **Churn Prediction Model**: Implement a trained ML model to predict customer churn.  
- **Interactive Dashboard**: Use Streamlit and Plotly for dynamic data visualization.  

## Technologies Used
- **Kafka**: For real-time data streaming.  
- **Apache Spark**: For real-time data processing and machine learning inference.  
- **Machine Learning Model**: Pre-trained model for churn prediction.  
- **Streamlit**: For interactive visualization and reporting.  

## Setup Instructions


### Clone the Repository
`git clone https://github.com/hieuchelsea20121997/real-time-bank-churn-with-kafka-spark-HieuUIT.git`

## Setup Instructions
- **Start the Environmentn**:
### Start the Environment
1. **Create and activate a virtual environment**  
   ```sh
   python -m venv venv
   source venv/bin/activate  # On macOS/Linux
   venv\Scripts\activate     # On Windows
2. **Install dependencies from requirements.txt**
    ```sh
    pip install -r requirements.txt
3. **Start Kafka**
- **Navigate to the Kafka directory:**: 
    ```sh
    cd path/to/your/kafka/location
- **Start Zookeeper:**: 
    ```sh
    bin\windows\zookeeper-server-start.bat config\zookeeper.properties
- **Start Kafka broker:**: 
    ```sh
    bin\windows\kafka-server-start.bat config\server.properties
- **Create required Kafka topics::**: 
    ```sh
    bin\windows\kafka-topics.bat --create --topic transactions --bootstrap-server localhost:9092
    bin\windows\kafka-topics.bat --create --topic churn-predictions --bootstrap-server localhost:9092
- **Model Setup**:
The trained model is available in the output directory. If you don't want to train a new model, you can skip to the next step.
Modify predict.py if you want to train a new model.
- **Kafka Producer Configuration**:
- Open kafka-spark-prediction folder
- Edit producer.py to set up the Kafka data producer.
    ```sh
    cd kafka-spark-prediction
    - **Navigate to the Kafka directory:**: 
- Run file Kafka_Producer.ipynb to see Kafka Producer Result
- Run file Kafka_Consumer.ipynb to see Kafka Producer Result
- **Streamlit Configuration**:
- Open visualize folder
- Modify app.py to integrate model inference and display real-time predictions.
    ```sh
    cd visualize
    streamlint run app.py

## Data Pipeline

<p align="center">
  <img src="pipeline.jpg" >
</p>

- Kafka: Streams simulated bank transaction data.
- Apache Spark: Processes data in real time and makes churn predictions.
- Machine Learning Model: Predicts churn likelihood.
- Streamlit Dashboard: Displays predictions and analytics.

## Usage
- Query Apache Spark Streaming Data
- Open Apache Spark UI to monitor streaming jobs.
- Monitor Kafka Messages
- Use Kafka CLI to check message flow.
- Open Streamlit Dashboard
- Visit http://localhost:8501/ in your browser to view real-time analytics.
-Visualizations
- (Add details about visualizations if needed.)

## Conclusion
- This project provides a robust real-time churn prediction system for banking services.
- By leveraging streaming data processing and machine learning, it helps banks identify at-risk customers early, enabling proactive retention strategies.

## Future Direction
- Enhance Model Accuracy: Improve churn prediction with more advanced techniques.
- Scalability: Optimize the pipeline for larger datasets.
- Alert System: Implement notifications for high-risk customers.
- Additional Visualizations: Expand reporting insights.
## Contact
- For any questions or feedback, please reach out to trunghieu201297@gmail.com.

