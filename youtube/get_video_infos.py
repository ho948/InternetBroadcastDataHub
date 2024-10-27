from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import time

def get_video_info(url):
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    # options.add_argument("--disable-gpu ")
    driver = webdriver.Chrome(service=Service(), options=options)

    try:
        driver.get(url)
        driver.implicitly_wait(10)

        try:
            thumbnail_element = driver.find_element(By.XPATH, "/html/body/div[1]/link[2]")
            thumbnail = thumbnail_element.get_attribute("href")
            print('thumbnail:', thumbnail)
        except Exception as e:
            print("썸네일을 찾지 못했습니다:", e)

        try:
            title_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[1]/h1/yt-formatted-string")
            title = title_element.text
            print('title:', title)
        except Exception as e:
            print("제목을 찾지 못했습니다:", e)

        try:
            channel_img_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[2]/div[1]/ytd-video-owner-renderer/a/yt-img-shadow/img")
            channel_img = channel_img_element.get_attribute("src")
            print('channel_img:', channel_img)
        except Exception as e:
            print("채널 이미지를 찾지 못했습니다:", e)

        try:
            channel_name_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[2]/div[1]/ytd-video-owner-renderer/div[1]/ytd-channel-name/div/div/yt-formatted-string/a")
            channel_name = channel_name_element.text
            channel_link = channel_name_element.get_attribute("href")
            print('channel_name:', channel_name)
            print('channel_link:', channel_link)
        except Exception as e:
            print("채널 이름을 찾지 못했습니다:", e)

        try:
            channel_subscribers_count_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[2]/div[1]/ytd-video-owner-renderer/div[1]/yt-formatted-string")
            channel_subscribers_count = channel_subscribers_count_element.text
            print('channel_subscribers_count:', channel_subscribers_count)
        except Exception as e:
            print("구독자 수를 찾지 못했습니다:", e)

        try:
            thumbsup_count_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[2]/div[2]/div/div/ytd-menu-renderer/div[1]/segmented-like-dislike-button-view-model/yt-smartimation/div/div/like-button-view-model/toggle-button-view-model/button-view-model/button/div[2]")
            thumbsup_count = thumbsup_count_element.text
            print('thumbsup_count:', thumbsup_count)
        except Exception as e:
            print("좋아요 수를 찾지 못했습니다:", e)

        try:
            views_count_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[4]/div[1]/div/ytd-watch-info-text/div/yt-formatted-string/span[1]")
            views_count = views_count_element.text
            print('views_count:', views_count)
        except Exception as e:
            print("조회수를 찾지 못했습니다:", e)

        try:
            uploaded_date_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[4]/div[1]/div/ytd-watch-info-text/div/yt-formatted-string/span[3]")
            uploaded_date = uploaded_date_element.text
            print('uploaded_date:', uploaded_date)
        except Exception as e:
            print("업로드 날짜를 찾지 못했습니다:", e)

        try:
            video_text_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[4]/div[1]/div/ytd-text-inline-expander/div[1]")
            video_text = video_text_element.text
            print('video_text:', video_text)
        except Exception as e:
            print("영상 설명을 찾지 못했습니다:", e)

        # 댓글 영역으로 스크롤
        try:
            # 스크롤을 제한된 횟수만큼만 내리기
            scroll_count = 10
            for _ in range(scroll_count):
                driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
                driver.implicitly_wait(10)
        except Exception as e:
            print("스크롤 다운 중 에러 발생", e)

        # 댓글 추출
        try:
            comments = []
            comment_elements = driver.find_elements(By.CSS_SELECTOR, "#content-text")

            # 상위 5개 댓글 추출
            for i, comment_element in enumerate(comment_elements[:5]):
                comments.append(comment_element.text)
                print(f"{i+1}: {comment_element.text}")
        except Exception as e:
            print("댓글 정보를 찾지 못했습니다:", e)

    except Exception as e:
        print("오류 발생:", e)
    finally:
        driver.quit()

video_url = 'https://www.youtube.com/watch?v=pvLlRyMtul8'
get_video_info(video_url)
