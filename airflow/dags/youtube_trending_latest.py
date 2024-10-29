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
EXECUTION_TS = None
DIR_PATH = None

# webdriver's options
OPTIONS = webdriver.ChromeOptions()
OPTIONS.add_argument("--headless")
OPTIONS.add_argument('--no-sandbox')
OPTIONS.add_argument('--disable-gpu')
OPTIONS.add_argument('--disable-dev-shm-usage')
OPTIONS.add_argument("--disable-software-rasterizer")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    f'{DAG_NAME}_dag',
    default_args=default_args,
    description='유튜브 최신 인기 급상승 동영상 dag',
    schedule_interval='@hourly',
    catchup=False
)

def get_trending_latest_links(**context):
    try:
        global EXECUTION_TS
        EXECUTION_TS = datetime.strptime(context['ts_nodash'], '%Y%m%dT%H%M%S') + timedelta(hours=10)
        make_dir()

        driver = webdriver.Chrome(service=Service(), options=OPTIONS)
        driver.get("https://www.youtube.com/feed/trending")
        driver.implicitly_wait(10)

        latest_videos = driver.find_elements(By.XPATH, '//*[@id="video-title"]')
        latest_links = [video.get_attribute('href') for video in latest_videos if video.get_attribute('href')]        
        logging.info(f"유튜브 최신 인기 급상승 동영상 링크 추출 완료 (현재 시각: {EXECUTION_TS})")
        
        trending_latest_columns = ['rank', 'link', 'execution_ts']
        trending_latest_rows = [[i + 1, latest_links[i], EXECUTION_TS] for i in range(len(latest_links))]
        save_to_csv(columns=trending_latest_columns, rows=trending_latest_rows, file_name='trending_latest_links')
        
        return latest_links
    
    except Exception as e:
        logging.error(f"유튜브 최신 인기 급상승 동영상 페이지 크롤링 중 에러 발생: {e}")
    finally:
        driver.quit()

def get_video_infos(**context):
    try:
        links = context["task_instance"].xcom_pull(key="return_value", task_ids="get_trending_latest_links")
        infos = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for link in links:
                futures.append(executor.submit(get_video_info, link))

            for future in as_completed(futures):
                result = future.result()
                if result:
                    infos.append(result)
        
        trending_latest_video_columns = ["link", "title", "views_count", "uploaded_date", "thumbsup_count", "thumbnail_img", "video_text", "channel_link", "channel_name", "subscribers_count", "channel_img", "comments", 'execution_ts']
        trending_latest_video_rows = [infos[i] + [EXECUTION_TS] for i in range(len(infos))]
        save_to_csv(columns=trending_latest_video_columns, rows=trending_latest_video_rows, file_name='trending_latest_video_infos')
    
    except Exception as e:
        logging.error(f"get_video_infos 테스크 실행 중 에러 발생: {e}")
        exit()

def make_dir():
    year = EXECUTION_TS.year
    month = EXECUTION_TS.month
    day = EXECUTION_TS.day
    hour = EXECUTION_TS.hour
    global DIR_PATH
    DIR_PATH = os.path.join(CUR_PATH, f'output/transaction_files/{DAG_NAME}/{year}/{month}/{day}/{hour}')
    try:
        os.makedirs(DIR_PATH)
        logging.info(f'{DIR_PATH}가 생성되었습니다.')

    except Exception as e:
        logging.error(f'{DIR_PATH} 생성 중 에러 발생: {e}')

def save_to_csv(columns, rows, file_name):
    csv_path = f'{DIR_PATH}/{file_name}_{EXECUTION_TS}.csv'
    try:
        with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(columns)
            writer.writerows(rows)
            logging.info(f'{csv_path}가 저장되었습니다.')

    except Exception as e:
        logging.error(f"{csv_path} 저장 중 에러 발생: {e}")

def get_video_info(link):
    # 영상 정보 추출: [링크, 제목, 조회수, 업로드일, 좋아요 수, 썸네일 이미지, 영상 텍스트, 채널 링크, 채널명, 구독자 수, 채널 이미지, 댓글]
    try:
        logging.info(f"{link} 동영상 페이지 스크래핑 시작")
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

        try:
            scroll_count = 10
            for _ in range(scroll_count):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                driver.implicitly_wait(10)
        except Exception as e:
            logging.error(f"스크롤링 중 에러 발생: {e}")
        try:
            comments = []
            comment_elements = driver.find_elements(By.CSS_SELECTOR, "#content-text")
            max_count = 5 # 최대 상위 5개 추출 (좋아요 수 기준)
            for comment_element in comment_elements[:max_count]:
                comments.append(comment_element.text)
            info.append(comments if comments else '')
        except Exception as e:
            logging.error(f"댓글 추출 중 에러 발생: {e}")

        logging.info(f"{link} 동영상 페이지 스크래핑 완료")
        return info

    except Exception as e:
        logging.error(f"{link} 동영상 페이지 스크래핑 중 에러 발생: {e}")
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