FROM apache/airflow:2.0.0
COPY requirements.txt /tmp
RUN python3 -m pip install -r /tmp/requirements.txt
RUN mkdir /home/airflow/.aws/ 
USER root
RUN mkdir -p /usr/local/airflow
RUN ln -s  /opt/airflow/dags /usr/local/airflow/dags 
RUN chown -R airflow:airflow /usr/local/airflow 
USER airflow 
COPY credentials /home/airflow/.aws/
