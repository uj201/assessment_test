FROM python:3.11.9
# FROM apache/airflow:2.5.1-python3.11.9

RUN pip install apache-airflow==2.5.1
RUN pip install requests beautifulsoup4 scrapy pandas textblob lxml pandasql tabulate

COPY dags/ /dags/
COPY dags/scripts/ /scripts/

# CMD ["airflow", "scheduler"]