from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import logging
import csv

CUR_PATH = os.path.dirname(os.path.realpath(__file__))
DAG_NAME = 'youtube_trending_latest'

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
    'start_date': datetime(2024, 10, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    f'{DAG_NAME}_dag',
    default_args=default_args,
    description='YouTube latest trending videos DAG',
    schedule_interval='0 * * * *', # 매 정시마다 실행
    catchup=False
)

def get_trending_latest_links(**context):
    try:
        execution_ts = datetime.strptime(context['ts_nodash'], '%Y%m%dT%H%M%S') + timedelta(hours=10)
        dir_path = make_dir(execution_ts)

        driver = webdriver.Chrome(service=Service(), options=OPTIONS)
        driver.get("https://www.youtube.com/feed/trending")
        driver.implicitly_wait(10)

        latest_videos = driver.find_elements(By.XPATH, '//*[@id="video-title"]')
        latest_links = [video.get_attribute('href') for video in latest_videos if video.get_attribute('href')]
        # shorts_latest_links = [link for link in latest_links if "shorts" in link]
        latest_links = [link for link in latest_links if "shorts" not in link] # 쇼츠에 해당하지 않는 것만 추출.
        logging.info(f"YouTube latest trending video links extracted (current time: {execution_ts})")

        trending_latest_columns = ['rank', 'link', 'execution_ts']
        trending_latest_rows = [
            [i + 1, latest_links[i], execution_ts] for i in range(len(latest_links))
        ]
        save_to_csv(columns=trending_latest_columns, rows=trending_latest_rows, dir_path=dir_path, file_name=f'trending_latest_links_{execution_ts}')

        return latest_links

    except Exception as e:
        logging.error(f"Error during YouTube trending videos page crawling: {e}")
    finally:
        driver.quit()

def get_video_infos(**context):
    try:
        execution_ts = datetime.strptime(context['ts_nodash'], '%Y%m%dT%H%M%S') + timedelta(hours=10)
        dir_path = make_dir(execution_ts)

        links = context["task_instance"].xcom_pull(key="return_value", task_ids="get_trending_latest_links")
        infos = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(get_video_info, link) for link in links]

            for future in as_completed(futures):
                result = future.result()
                if result:
                    infos.append(result)

        trending_latest_video_columns = [
            "link", "title", "views_count", "uploaded_date", "thumbsup_count", "thumbnail_img", 
            "video_text", "channel_link", "channel_name", "subscribers_count", "channel_img",
            "execution_ts"
        ]
        trending_latest_video_rows = [infos[i] + [execution_ts] for i in range(len(infos))]
        save_to_csv(columns=trending_latest_video_columns, rows=trending_latest_video_rows, dir_path=dir_path, file_name=f'trending_latest_video_infos_{execution_ts}')

    except Exception as e:
        logging.error(f"Error during get_video_infos task execution: {e}")

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

def get_video_info(link):
    try:
        logging.info(f"Starting scraping for video page {link}")
        info = [link]

        driver = webdriver.Chrome(service=Service(), options=OPTIONS)
        driver.get(link)
        driver.implicitly_wait(10)

        try:
            title_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[1]/h1/yt-formatted-string")
            title = title_element.text
            info.append(title if title else '')
        except Exception as e:
            logging.error(f"제목 추출 중 에러 발생: {e}")
        
        try:
            views_count_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[4]/div[1]/div/ytd-watch-info-text/div/yt-formatted-string/span[1]")
            views_count = views_count_element.text
            info.append(views_count if views_count else '')
        except Exception as e:
            logging.error(f"조회수 추출 중 에러 발생: {e}")

        try:
            uploaded_date_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[4]/div[1]/div/ytd-watch-info-text/div/yt-formatted-string/span[3]")
            uploaded_date = uploaded_date_element.text
            info.append(uploaded_date if uploaded_date else '')
        except Exception as e:
            logging.error(f"업로드일 추출 중 에러 발생: {e}")

        try:
            thumbsup_count_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[2]/div[2]/div/div/ytd-menu-renderer/div[1]/segmented-like-dislike-button-view-model/yt-smartimation/div/div/like-button-view-model/toggle-button-view-model/button-view-model/button/div[2]")
            thumbsup_count = thumbsup_count_element.text
            info.append(thumbsup_count if thumbsup_count else '')
        except Exception as e:
            logging.error(f"좋아요 수 추출 중 에러 발생: {e}")

        try:
            thumbnail_element = driver.find_element(By.XPATH, "/html/body/div[1]/link[2]")
            thumbnail_img = thumbnail_element.get_attribute("href")
            info.append(thumbnail_img if thumbnail_img else '')
        except Exception as e:
            logging.error(f"썸네일 이미지 추출 중 에러 발생: {e}")

        try:
            video_text_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[4]/div[1]/div/ytd-text-inline-expander/div[1]")
            video_text = video_text_element.text
            info.append(video_text if video_text else '')
        except Exception as e:
            logging.error(f"영상 정보 추출 중 에러 발생: {e}")

        try:
            channel_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[2]/div[1]/ytd-video-owner-renderer/div[1]/ytd-channel-name/div/div/yt-formatted-string/a")
            channel_link = channel_element.get_attribute("href")
            channel_name = channel_element.text
            info.append(channel_link if channel_link else '')
            info.append(channel_name if channel_name else '')
        except Exception as e:
            logging.error(f"채널 링크 및 이름 추출 중 에러 발생: {e}")

        try:
            subscribers_count_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[2]/div[1]/ytd-video-owner-renderer/div[1]/yt-formatted-string")
            subscribers_count = subscribers_count_element.text
            info.append(subscribers_count if subscribers_count else '')
        except Exception as e:
            logging.error(f"구독자 수 추출 중 에러 발생: {e}")

        try:
            channel_img_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[2]/div[1]/ytd-video-owner-renderer/a/yt-img-shadow/img")
            channel_img = channel_img_element.get_attribute("src")
            info.append(channel_img if channel_img else '')
        except Exception as e:
            logging.error(f"채널 이미지 추출 중 에러 발생: {e}")

        # try:
        #     scroll_count = 10
        #     for _ in range(scroll_count):
        #         driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        #         driver.implicitly_wait(10)
        # except Exception as e:
        #     logging.error(f"스크롤링 중 에러 발생: {e}")
        # try:
        #     comment_elements = driver.find_elements(By.CSS_SELECTOR, "yt-formatted-string#content-text")
        #     max_count = 5 # 최대 상위 5개 추출 (좋아요 수 기준)
        #     for comment_element in comment_elements[:max_count]:
        #         comment = comment_element.text
        #         info.append(comment if comment else '')
        # except Exception as e:
        #     logging.error(f"댓글 추출 중 에러 발생: {e}")

        logging.info(f"Scraping for video page {link} completed.")
        return info

    except Exception as e:
        logging.error(f"Error during video page scraping for {link}: {e}")
    finally:
        driver.quit()

get_trending_latest_links_task = PythonOperator(
    task_id='get_trending_latest_links',
    python_callable=get_trending_latest_links,
    dag=dag
)

get_video_infos_task = PythonOperator(
    task_id='get_video_infos',
    python_callable=get_video_infos,
    dag=dag
)

get_trending_latest_links_task >> get_video_infos_task
