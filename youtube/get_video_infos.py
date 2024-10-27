from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def get_video_info(url):
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(), options=options)

    try:
        driver.get(url)
        driver.implicitly_wait(10)

        title_element = driver.find_element(By.XPATH, "/html/head/meta[3]")
        title = title_element.get_attribute("content")
        print('title:', title)

        channel_name_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[2]/div[1]/ytd-video-owner-renderer/div[1]/ytd-channel-name/div/div/yt-formatted-string/a")
        channel_name = channel_name_element.text
        channel_link = channel_name_element.get_attribute("href")
        print('channel_name:', channel_name)
        print('channel_link:', channel_link)

        channel_subscribers_count_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[2]/div[1]/ytd-video-owner-renderer/div[1]/yt-formatted-string")
        channel_subscribers_count = channel_subscribers_count_element.text
        print('channel_subscribers_count:', channel_subscribers_count)

        views_count_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[4]/div[1]/div/ytd-watch-info-text/div/yt-formatted-string/span[1]")
        views_count = views_count_element.text
        print('views_count:', views_count)

        # 댓글 로딩 대기
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#contents #content-text")))

        # 댓글 추출
        comments = []
        comment_elements = driver.find_elements(By.CSS_SELECTOR, '#contents #content-text')
        for comment in comment_elements[:10]:  # 상위 10개 댓글 가져오기
            comments.append(comment.text)
        print('comments:', comments)

        uploaded_date_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[4]/div[1]/div/ytd-watch-info-text/div/yt-formatted-string/span[3]")
        uploaded_date = uploaded_date_element.text
        print('uploaded_date:', uploaded_date)

        video_text_element = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[4]/div[1]/div/ytd-text-inline-expander/yt-attributed-string")
        video_text = video_text_element.text
        print('video_text:', video_text)

    except Exception as e:
        print(e)
    finally:
        driver.quit()

video_url = 'https://www.youtube.com/watch?v=VjKUBbzIYtE'
get_video_info(video_url)
