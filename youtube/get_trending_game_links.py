from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ChromeDriver 설정
options = webdriver.ChromeOptions()
options.add_argument("--headless")
driver = webdriver.Chrome(service=Service(), options=options)

try:
    # 트렌딩 페이지 열기
    driver.get("https://www.youtube.com/feed/trending")
    driver.implicitly_wait(10)

    # "게임" 카테고리 탭이 로드될 때까지 기다린 후 클릭
    game_tab = WebDriverWait(driver, 10).until(
    EC.element_to_be_clickable((By.XPATH, "//yt-tab-shape[@tab-title='게임']"))
    )
    game_tab.click()
    driver.implicitly_wait(10)

    trending_games_url = driver.current_url
    driver.get(trending_games_url)

    # 게임 카테고리의 동영상 링크 수집
    game_videos = driver.find_elements(By.XPATH, '//*[@id="video-title"]')
    game_links = [video.get_attribute('href') for video in game_videos if video.get_attribute('href')]

    rank = 1
    print("게임 카테고리의 동영상 링크:")
    for link in game_links:
        print(rank, link)
        rank += 1

except Exception as e:
    print(f"오류 발생: {e}")
finally:
    driver.quit()
