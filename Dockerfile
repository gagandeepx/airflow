# FROM apache/airflow:2.3.3

# # Install additional dependencies
# RUN pip install --no-cache-dir apache-airflow-providers-databricks==1.0.1

# # Set environment variables
# ENV AIRFLOW_CONN_DATABRICKS_DEFAULT=databricks://@host-url?token=your-pat-token

# # Copy your DAGs and configurations
# COPY dags /app/dags
# COPY airflow.cfg /app/config

# # Expose ports for Airflow web server and scheduler
# EXPOSE 8080 8793

# # Run Airflow web server and scheduler
# CMD ["airflow", "webserver", "--port", "8080"]

FROM apache/airflow:2.3.3

ADD requirements.txt .

RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt 