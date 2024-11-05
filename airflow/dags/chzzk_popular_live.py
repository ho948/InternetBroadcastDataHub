from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import logging
import csv
import requests

CUR_PATH = os.path.dirname(os.path.realpath(__file__))
DAG_NAME = 'chzzk_popular_live'

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
    description='치지직 인기 라이브 DAG',
    schedule_interval='0 * * * *',  # 매 정시마다 실행
    catchup=False
)

def get_popular_lives(**context):
    try:
        url = "https://api.chzzk.naver.com/service/v1/lives?size=20&sortType=POPULAR"
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
        columns = ['execution_ts', 'live_id', 'live_title', 'viewers_count', 'channel_id', 'category_type', 'category_id', 'category_name', 'is_adult', 'open_ts']
        rows = []

        for data in json_data['content']['data']:
            live_info = [execution_ts] + get_live_info(live_data=data)
            if len(live_info) != len(columns):
                logging.error(f"라이브 데이터 크기가 올바르지 않습니다.\n{live_info}")
                raise ValueError("라이브 데이터 크기 오류")
            rows.append(live_info)

        dir_path = make_dir(execution_ts)
        save_to_csv(columns=columns, rows=rows, dir_path=dir_path, file_name=f'chzzk_popular_lives_{execution_ts}')

        channel_ids = [row[4] for row in rows]
        return channel_ids

    except Exception as e:
        logging.error(f"라이브 데이터 로드 중 에러 발생: {e}")
        raise

def load_channel_infos(**context):
    try:
        channel_ids = context["task_instance"].xcom_pull(key="return_value", task_ids="load_popular_lives")
        execution_ts = datetime.strptime(context['ts_nodash'], '%Y%m%dT%H%M%S') + timedelta(hours=10)
        columns = ['execution_ts', 'channel_id', 'channel_name', 'follower_count', 'verified_mark', 'channel_type', 'channel_description']
        rows = []

        for channel_id in channel_ids:
            channel_info = [execution_ts] + get_channel_info(channel_id=channel_id)
            if len(channel_info) != len(columns):
                logging.error(f"채널 데이터 크기가 올바르지 않습니다.\n{channel_info}")
                raise ValueError("채널 데이터 크기 오류")
            rows.append(channel_info)

        dir_path = make_dir(execution_ts)
        save_to_csv(columns=columns, rows=rows, dir_path=dir_path, file_name=f'chzzk_popular_channels_{execution_ts}')

    except Exception as e:
        logging.error(f"채널 데이터 로드 중 에러 발생: {e}")
        raise

def get_live_info(live_data):
    try:
        live_id = live_data['liveId']
        live_title = live_data['liveTitle']
        viewers_count = live_data['concurrentUserCount']
        channel_id = live_data['channel']['channelId']
        category_type = live_data['categoryType']
        category_id = live_data['liveCategory']
        category_name = live_data['liveCategoryValue']
        is_adult = live_data['adult']
        open_ts = live_data['openDate']

        return [live_id, live_title, viewers_count, channel_id, category_type, category_id, category_name, is_adult, open_ts]
    except Exception as e:
        logging.error(f"{live_data}의\n 라이브 데이터 추출 중 에러 발생: {e}")
        raise

def get_channel_info(channel_id=None):
    if channel_id is None:
        logging.error("채널 ID가 존재하지 않습니다.")
        raise ValueError("채널 ID 누락")
    try:
        url = f"https://api.chzzk.naver.com/service/v1/channels/{channel_id}"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        json_data = response.json()

        channel_id = json_data['content']['channelId']
        channel_name = json_data['content']['channelName']
        follower_count = json_data['content']['followerCount']
        verified_mark = json_data['content']['verifiedMark']
        channel_type = json_data['content']['channelType']
        channel_description = json_data['content']['channelDescription']

        logging.info(f"{channel_id}에 대한 채널 정보 추출 완료")
        return [channel_id, channel_name, follower_count, verified_mark, channel_type, channel_description]
    except requests.exceptions.RequestException as e:
        logging.error(f"채널 API 호출 중 에러 발생: {e}")
        raise

def make_dir(execution_ts):
    year, month, day, hour = execution_ts.year, execution_ts.month, execution_ts.day, execution_ts.hour
    dir_path = os.path.join(CUR_PATH, f'output/transaction_files/{DAG_NAME}/{year}/{month}/{day}/{hour}')
    try:
        os.makedirs(dir_path, exist_ok=True)
        logging.info(f'{dir_path} 생성 완료')
    except Exception as e:
        logging.error(f"{dir_path} 생성 중 오류: {e}")
        raise
    return dir_path

def save_to_csv(columns, rows, dir_path, file_name):
    csv_path = os.path.join(dir_path, f'{file_name}.csv')
    try:
        with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(columns)
            writer.writerows(rows)
            logging.info(f'{csv_path} 저장 완료')
    except Exception as e:
        logging.error(f"{csv_path} 저장 중 오류: {e}")
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

load_channel_infos_task = PythonOperator(
    task_id='load_channel_infos',
    python_callable=load_channel_infos,
    dag=dag
)

get_popular_lives_task >> load_popular_lives_task >> load_channel_infos_task
