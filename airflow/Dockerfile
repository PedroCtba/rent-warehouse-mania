FROM apache/airflow:2.7.3
COPY requirements.txt /requirements.txt
RUN python -m pip install pip==23.2.1
RUN pip install --no-cache-dir --user -r /requirements.txt