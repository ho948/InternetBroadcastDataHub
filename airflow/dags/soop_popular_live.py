from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import logging
import csv
import requests

CUR_PATH = os.path.dirname(os.path.realpath(__file__))
DAG_NAME = 'soop_popular_live'

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36",
    "Accept": "application/json"
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    f'{DAG_NAME}_dag',
    default_args=default_args,
    description='SOOP(아프리카티비) 인기 라이브 DAG',
    schedule_interval='0 * * * *',  # 매 정시마다 실행
    catchup=False
)

def get_popular_lives(**context):
    try:
        url = "https://live.sooplive.co.kr/api/myplus/preferbjOnLnbController.php"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        json_data = response.json()
        logging.info("인기 라이브 추출 완료")
        return json_data
    except requests.exceptions.RequestException as e:
        logging.error(f"lives API 호출 중 에러 발생: {e}")
        raise

def load_popular_lives(**context):
    try:
        json_data = context["task_instance"].xcom_pull(key="return_value", task_ids="get_popular_lives")
        execution_ts = datetime.strptime(context['ts_nodash'], '%Y%m%dT%H%M%S') + timedelta(hours=10)
        columns = ['execution_ts', 'live_id', 'live_title', 'viewers_count', 'channel_id', 'channel_name', 'category', 'is_adult']
        rows = []

        for data in json_data['DATA']['streamer_list']:
            live_info = [execution_ts] + get_live_info(live_data=data)
            if len(live_info) != len(columns):
                logging.error(f"라이브 데이터 크기가 올바르지 않습니다.\n{live_info}")
                raise ValueError("라이브 데이터 크기 오류")
            rows.append(live_info)

        dir_path = make_dir(execution_ts)
        save_to_csv(columns=columns, rows=rows, dir_path=dir_path, file_name=f'soop_popular_lives_{execution_ts}')

    except Exception as e:
        logging.error(f"라이브 데이터 로드 중 에러 발생: {e}")
        raise

def get_live_info(live_data):
    try:
        live_id = live_data['broad_no']
        live_title = live_data['broad_title']
        viewers_count = live_data['view_cnt']
        channel_id = live_data['user_id']
        channel_name = live_data['user_nick']
        category = live_data['cate_name']
        is_adult = live_data['grade']

        return [live_id, live_title, viewers_count, channel_id, channel_name, category, is_adult]
    except Exception as e:
        logging.error(f"{live_data}의\n 라이브 데이터 추출 중 에러 발생: {e}")
        raise

def make_dir(execution_ts):
    year, month, day, hour = execution_ts.year, execution_ts.month, execution_ts.day, execution_ts.hour
    dir_path = os.path.join(CUR_PATH, f'output/transaction_files/{DAG_NAME}/{year}/{month}/{day}/{hour}')
    try:
        os.makedirs(dir_path, exist_ok=True)
        logging.info(f'{dir_path} has been created.')
    except Exception as e:
        logging.error(f"Error creating {dir_path}: {e}")
    return dir_path

def save_to_csv(columns, rows, dir_path, file_name):
    csv_path = f'{dir_path}/{file_name}.csv'
    try:
        with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(columns)
            writer.writerows(rows)
            logging.info(f'{csv_path} has been saved.')

    except Exception as e:
        logging.error(f"Error saving {csv_path}: {e}")
        raise

get_popular_lives_task = PythonOperator(
    task_id='get_popular_lives',
    python_callable=get_popular_lives,
    dag=dag
)

load_popular_lives_task = PythonOperator(
    task_id='load_popular_lives',
    python_callable=load_popular_lives,
    dag=dag
)

get_popular_lives_task >> load_popular_lives_task
