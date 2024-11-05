from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import logging
import csv

CUR_PATH = os.path.dirname(os.path.realpath(__file__))
DAG_NAME = 'soop_popular_live'

# Webdriver options
OPTIONS = webdriver.ChromeOptions()
OPTIONS.add_argument("--headless")
OPTIONS.add_argument('--no-sandbox')
OPTIONS.add_argument('--disable-gpu')
OPTIONS.add_argument('--disable-dev-shm-usage')
OPTIONS.add_argument("--disable-software-rasterizer")
OPTIONS.add_argument("--lang=ko")

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
        execution_ts = datetime.strptime(context['ts_nodash'], '%Y%m%dT%H%M%S') + timedelta(hours=10)
        driver = webdriver.Chrome(service=Service(), options=OPTIONS)
        driver.get("https://www.sooplive.co.kr/live/all")
        driver.implicitly_wait(10)

        live_infos = []
        for i in range(1, 21):
            live_info = [execution_ts] + get_live_info(driver=driver, idx=i)
            live_infos.append(live_info)

        logging.info("popular_lives 크롤링 완료")
        return live_infos

    except Exception as e:
        logging.error(f"popular_lives 크롤링 중 에러 발생: {e}")
        raise

    finally:
        driver.quit()

def load_popular_lives(**context):
    try:
        live_infos = context["task_instance"].xcom_pull(key="return_value", task_ids="get_popular_lives")
        columns = ['execution_ts', 'live_id', 'channel_id', 'channel_name', 'live_title', 'viewers_count', 'category', 'open_ts']

        execution_ts = datetime.strptime(context['ts_nodash'], '%Y%m%dT%H%M%S') + timedelta(hours=10)
        dir_path = make_dir(execution_ts)
        save_to_csv(columns=columns, rows=live_infos, dir_path=dir_path, file_name=f'soop_popular_lives_{execution_ts}')

    except Exception as e:
        logging.error(f"load_popular_lives 중 에러 발생: {e}")
        raise

def get_live_info(driver=None, idx=None):
    if driver is None:
        logging.error("드라이버가 존재하지 않습니다.")
        raise ValueError("Driver not available")

    info = []
    default_xpath = f"/html/body/div[1]/main/div[2]/div[3]/ul/li[{idx}]"

    try:
        live_link_element = driver.find_element(By.XPATH, f"{default_xpath}/div[1]/a")
        live_link = live_link_element.get_attribute("href")
        parts = live_link.split('/')
        live_id = parts[-1]
        channel_id = parts[-2]
        info.extend([live_id, channel_id])
    except Exception as e:
        logging.error(f"live_link 추출 중 에러 발생: {e}")
        raise

    try:
        channel_name_element = driver.find_element(By.XPATH, f"{default_xpath}/div[2]/div/div[1]/a/span")
        info.append(channel_name_element.text)
    except Exception as e:
        logging.error(f"channel_name 추출 중 에러 발생: {e}")
        raise

    try:
        live_title_element = driver.find_element(By.XPATH, f"{default_xpath}/div[2]/div/h3/a")
        info.append(live_title_element.get_attribute("title"))
    except Exception as e:
        logging.error(f"live_title 추출 중 에러 발생: {e}")
        raise

    try:
        viewers_count_element = driver.find_element(By.XPATH, f"{default_xpath}/div[1]/span[2]/em")
        info.append(viewers_count_element.text)
    except Exception as e:
        logging.error(f"viewers_count 추출 중 에러 발생: {e}")
        raise

    try:
        category_element = driver.find_element(By.XPATH, f"{default_xpath}/div[2]/div/div[3]/a[1]")
        info.append(category_element.text)
    except Exception as e:
        logging.error(f"category 추출 중 에러 발생: {e}")
        raise

    try:
        open_ts_element = driver.find_element(By.XPATH, f"{default_xpath}/div[1]/span[3]")
        info.append(open_ts_element.text)
    except Exception as e:
        logging.error(f"open_ts 추출 중 에러 발생: {e}")
        raise

    logging.info(f"{idx}번 째 라이브 데이터 스크래핑 완료")
    return info

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
        exit()

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
