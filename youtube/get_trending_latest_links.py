from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service

# Chrome WebDriver 설정
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # 브라우저 창을 띄우지 않고 실행 (옵션)
driver = webdriver.Chrome(service=Service(), options=options)

try:
    # 1. 최신 인기 급상승 동영상 크롤링
    driver.get("https://www.youtube.com/feed/trending")
    driver.implicitly_wait(10)  # 페이지 로딩 대기

    # 동영상 링크 수집
    latest_videos = driver.find_elements(By.XPATH, '//*[@id="video-title"]')
    latest_links = [video.get_attribute('href') for video in latest_videos if video.get_attribute('href')]

    print("최신 인기 급상승 동영상 링크들:")
    rank = 1
    for link in latest_links:
        print(rank, link)
        rank += 1
except Exception as e:
    print(e)
finally:
    driver.quit()