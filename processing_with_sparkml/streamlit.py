import streamlit as st
import pandas as pd
import plotly.express as px
from kafka import KafkaConsumer
import json
import subprocess
import time
import psutil

# Đường dẫn đến Python trong môi trường ảo henv
PYTHON_EXECUTABLE = "C:/Users/PC/Desktop/Do_an_Big_data/PythonCodes/henv/Scripts/python.exe"

# Set up the main interface
st.set_page_config(layout="wide", page_title="Churn Prediction Dashboard")
st.markdown(
    "<h1 style='text-align: center;'>📊 PHÂN TÍCH KHÁCH HÀNG RỜI BỎ NGÂN HÀNG THEO THỜI GIAN THỰC</h1>", 
    unsafe_allow_html=True
)

# Sidebar Menu
st.sidebar.title("📊 CHURN PREDICTION")
st.sidebar.markdown("<hr style='border:1px solid gray'>", unsafe_allow_html=True)

menu = st.sidebar.radio("Chọn", ["Trang chủ", "Thông tin các thành viên thực hiện"])

st.sidebar.markdown("<br>", unsafe_allow_html=True)  # Add space

# Control Streaming in Sidebar
with st.sidebar.container():
    st.markdown("### 🔄 Điều khiển Streaming")

    if st.button("🚀 Start Streaming"):
        if not st.session_state.get("streaming_active", False):
            st.session_state["streaming_active"] = True
            process = subprocess.Popen([PYTHON_EXECUTABLE, "streaming_script1.py"])
            st.session_state["streaming_pid"] = process.pid  # Lưu PID của tiến trình

    st.markdown("<br>", unsafe_allow_html=True)  # Space between buttons

    if st.button("🛑 Stop Streaming"):
        if st.session_state.get("streaming_active", False):
            st.session_state["streaming_active"] = False
            pid = st.session_state.get("streaming_pid")
            if pid:
                try:
                    parent = psutil.Process(pid)
                    for child in parent.children(recursive=True):  # Dừng các tiến trình con
                        child.terminate()
                    parent.terminate()  # Dừng tiến trình chính
                except psutil.NoSuchProcess:
                    pass

# Display member information if "Thông tin các thành viên thực hiện" is selected
if menu == "Thông tin các thành viên thực hiện":
    st.empty()
    st.markdown("<hr style='border:1px solid gray'>", unsafe_allow_html=True)
    
    st.markdown("<h3 style='text-align: center;'>Môn học: Xử lý Dữ liệu Lớn</h3>", unsafe_allow_html=True)
    st.markdown("<h3 style='text-align: center;'>Giảng viên hướng dẫn: TS. Đỗ Trọng Hợp</h3>", unsafe_allow_html=True)
    st.markdown("<h3 style='text-align: center;'>Thực hiện: Ngoc Ngan Team</h3>", unsafe_allow_html=True)

    st.markdown("<hr style='border:1px solid gray'>", unsafe_allow_html=True)
    
    st.markdown("<h2 style='text-align: center;'>👨‍💻 Thông tin các thành viên thực hiện</h2>", unsafe_allow_html=True)
    
    members = [
        {"name": "Huỳnh Trung Hiếu", "id": "22540006"},
        {"name": "Nguyễn Tấn Đạt", "id": "22540003"},
        {"name": "Phan Tấn Cảnh", "id": "22540002"},
    ]
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"<h3 style='text-align: center;'>{members[0]['name']}</h3>", unsafe_allow_html=True)
        st.markdown(f"<p style='text-align: center;'><strong>🆔 {members[0]['id']}</strong></p>", unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"<h3 style='text-align: center;'>{members[1]['name']}</h3>", unsafe_allow_html=True)
        st.markdown(f"<p style='text-align: center;'><strong>🆔 {members[1]['id']}</strong></p>", unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"<h3 style='text-align: center;'>{members[2]['name']}</h3>", unsafe_allow_html=True)
        st.markdown(f"<p style='text-align: center;'><strong>🆔 {members[2]['id']}</strong></p>", unsafe_allow_html=True)
    
    st.stop()


# Kafka Config
TOPIC_NAME = "prediction_rf_sparkml"
KAFKA_SERVER = "localhost:9092"

# Initialize session_state
if "data" not in st.session_state:
    st.session_state["data"] = pd.DataFrame()
if "streaming_active" not in st.session_state:
    st.session_state["streaming_active"] = False
if "update_count" not in st.session_state:  # Biến đếm số lần cập nhật
    st.session_state["update_count"] = 0

# Function to read initial data from Kafka
def load_initial_data():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000
    )
    records = [message.value for message in consumer]
    return pd.DataFrame(records) if records else pd.DataFrame()

# Load initial data if empty
if st.session_state["data"].empty:
    st.session_state["data"] = load_initial_data()

# Tạo vùng chứa để cập nhật biểu đồ
chart_placeholder = st.empty()

def update_data():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True
    )

    while st.session_state["streaming_active"]:
        for message in consumer:
            new_data = pd.DataFrame([message.value])
            st.session_state["data"] = pd.concat([st.session_state["data"], new_data], ignore_index=True)
            
            st.session_state["update_count"] += 1  # Cập nhật số lần vẽ

            with chart_placeholder.container():
                display_charts()  # Cập nhật biểu đồ trong vùng chứa
            
            time.sleep(1)  # Giảm tải CPU

# Display charts
def display_charts():
    df = st.session_state["data"]
    if not df.empty:
        # Extract churn counts
        churn_counts = df["ChurnPrediction"].value_counts()
        
        # Pie chart for churn prediction with matching colors
        fig_pie = px.pie(
            names=["Không Rời Bỏ", "Rời Bỏ"],
            values=[churn_counts.get(0, 0), churn_counts.get(1, 0)],
            title="📌 Tỷ lệ khách hàng rời bỏ",
            color=["Không Rời Bỏ", "Rời Bỏ"],  # Định nghĩa màu theo nhãn
            color_discrete_map={"Không Rời Bỏ": "blue", "Rời Bỏ": "red"}  # Giữ màu giống histogram
        )


        # Group data by ChurnPrediction
        # No need for Geography anymore
        # Bar chart for ChurnPrediction by Gender
        gender_churn = df.groupby("Female")["ChurnPrediction"].mean().reset_index()
        gender_churn["Gender"] = gender_churn["Female"].map({1: "Nữ", 0: "Nam"})

        fig_gender_churn = px.bar(
            gender_churn,
            x="Gender",
            y="ChurnPrediction",
            title="📊 Tỷ lệ rời bỏ theo giới tính",
            color="Gender")


        # Histogram for CreditScore with ChurnPrediction labels
        # Tạo cột mới với nhãn thay vì số 0/1
        df["Churn_Label"] = df["ChurnPrediction"].map({0: "Không Rời Bỏ", 1: "Rời Bỏ"})

        # Histogram for CreditScore with labeled ChurnPrediction
        fig_credit = px.histogram(
            df, x="CreditScore", nbins=30, title="📈 Phân bố điểm tín dụng", 
            color="Churn_Label",  # Dùng nhãn thay vì số 0/1
            color_discrete_map={"Không Rời Bỏ": "blue", "Rời Bỏ": "red"}
        )

        fig_credit.update_layout(
            legend_title_text="Churn Prediction",
            legend_traceorder="normal"
        )



        
        # Line chart for Age vs ChurnPrediction
        age_churn = df.groupby("Age")["ChurnPrediction"].mean().reset_index()
        fig_age = px.line(age_churn, x="Age", y="ChurnPrediction", title="📊 Tỷ lệ rời bỏ theo độ tuổi")
        
        # Bar chart for NumOfProducts vs ChurnPrediction
        fig_products = px.bar(df, x="NumOfProducts", color="ChurnPrediction", barmode="group", title="📊 Ảnh hưởng của số lượng sản phẩm đến rời bỏ")
        
        # Display charts in two columns
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(fig_pie, use_container_width=True, key=f"fig_pie_{st.session_state['update_count']}")
        with col2:
            st.plotly_chart(fig_gender_churn, use_container_width=True, key=f"fig_gender_churn_{st.session_state['update_count']}")
        col3, col4 = st.columns(2)
        with col3:
            st.plotly_chart(fig_credit, use_container_width=True, key=f"fig_credit_{st.session_state['update_count']}")
        with col4:
            st.plotly_chart(fig_age, use_container_width=True, key=f"fig_age_{st.session_state['update_count']}")

        # Display statistics
        st.write("### 📝 Thống kê khách hàng có nguy cơ rời bỏ")
        st.dataframe(df[df["ChurnPrediction"] == 1])

        st.write("### 📊 Thống kê số lượng prediction")
        st.dataframe(churn_counts.to_frame("Số lượng").reset_index().rename(columns={"index": "Prediction"}))

# Display initial charts
with chart_placeholder.container():
    display_charts()

# Stream new data if streaming is active
if st.session_state["streaming_active"]:
    update_data()


