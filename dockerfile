FROM apache/airflow:latest
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
RUN python -m spacy download en_core_web_sm