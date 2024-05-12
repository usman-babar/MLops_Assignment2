from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from bs4 import BeautifulSoup
import csv
import os
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mlops_A2_dag',
    default_args=default_args,
    description='This is a DAG for MLOps Assignment02',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 5, 10),
    catchup=False
)

# Task 1 - Data Extraction
def extract_data():
    sources = ['https://www.dawn.com/', 'https://www.bbc.com/']
    all_links = []
    for source in sources:
        reqs = requests.get(source)
        soup = BeautifulSoup(reqs.text, 'html.parser')
        links = soup.find_all('a', href=True)
        for link in links:
            link_url = link['href']
            if link_url.startswith('http'):
                all_links.append(link_url)
            else:
                if source.endswith('/'):
                    all_links.append(source + link_url)
                else:
                    all_links.append(source + '/' + link_url)
    
    extracted_data = []
    for link in all_links:
        try:
            req = requests.get(link)
            req.raise_for_status()
            link_soup = BeautifulSoup(req.text, 'html.parser')
            title = link_soup.title.string.strip() if link_soup.title else ""
            description = link_soup.find('meta', property='og:description')
            description = description['content'].strip() if description and 'content' in description.attrs else ""
            extracted_data.append({'title': title, 'description': description, 'link': link})
        except (requests.RequestException, KeyError, AttributeError) as e:
            print(f"Error occurred while fetching or parsing data for link: {link}. Error: {e}")
    
    with open('data\extracted_data.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Title', 'Description', 'Link']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for data in extracted_data:
            writer.writerow({'Title': data['title'], 'Description': data['description'], 'Link': data['link']})

    return extracted_data

extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

# Task 2 - Preprocessing of Data
def preprocess_data():
    extracted_data = []
    with open('data\extracted_data.csv', 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            extracted_data.append({'title': row['Title'], 'description': row['Description'], 'link': row['Link']})

    unique_data = [dict(t) for t in {tuple(d.items()) for d in extracted_data}]

    preprocessed_data = []
    for data in unique_data:
        if data['title'] and data['description']:
            if data['title'].encode('ascii', 'ignore').decode() == data['title'] and \
               data['description'].encode('ascii', 'ignore').decode() == data['description']:
                preprocessed_data.append(data)
            else:
                print(f"Ignoring non-English entry: {data}")
        else:
            data['description'] = None
            preprocessed_data.append(data)

    with open('data\preprocessed_data.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Title', 'Description', 'Link']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for data in preprocessed_data:
            writer.writerow({'Title': data['title'], 'Description': data['description'], 'Link': data['link']})

    return preprocessed_data

preprocess_data_task = PythonOperator(
    task_id='preprocess_data',
    python_callable=preprocess_data,
    dag=dag
)

# Task 3 - DVC Implementation
def setup_dvc():
    os.system('git init')
    os.system('dvc init')
    os.system('dvc remote add -d gdrive gdrive://1aOUOyuPdZ6NMONKP_3B6lz4F1yji1mN2')

def add_data_to_dvc():
    os.system('dvc add data\preprocessed_data.csv')
    os.system('git add data\preprocessed_data.csv.dvc data\.gitignore')
    os.system('git commit -m "First time commit"')
    os.system('dvc commit')

def push_to_remote():
    os.system('dvc push')

def integrate_with_git():
    os.system('git add .dvc/')
    os.system('git commit -m "Add DVC metafiles"')
    subprocess.run(['git', 'remote', 'add', 'origin', 'https://github.com/usman-babar/MLops_Assignment2.git'])
    subprocess.run(['git', 'branch', '-M', 'main'])
    subprocess.run(['git', 'push', '-u', 'origin', 'main'])

setup_dvc_task = PythonOperator(
    task_id='setup_dvc',
    python_callable=setup_dvc,
    dag=dag
)

add_data_to_dvc_task = PythonOperator(
    task_id='add_data_to_dvc',
    python_callable=add_data_to_dvc,
    dag=dag
)

push_to_remote_task = PythonOperator(
    task_id='push_to_remote',
    python_callable=push_to_remote,
    dag=dag
)

integrate_with_git_task = PythonOperator(
    task_id='integrate_with_git',
    python_callable=integrate_with_git,
    dag=dag
)

# Task Dependencies
extract_data_task >> preprocess_data_task >> setup_dvc_task >> add_data_to_dvc_task >> push_to_remote_task >> integrate_with_git_task

## command to set the admin for airflow 
# docker-compose run airflow-worker airflow users create  --role Admin --username <your-username> --email <your-email> --firstname <your-firstname> --lastname <your-lastname> --password <your-password>