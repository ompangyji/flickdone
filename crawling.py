from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import requests
from PIL import Image
import yt_dlp
import time
import os
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# 저장할 디렉토리 설정
save_dir = '/root/flickdone/data'
os.makedirs(save_dir, exist_ok=True)

# Initialize WebDriver
driver = None
try:
    # Set up Chrome WebDriver
    service = Service('/usr/local/bin/chromedriver')  # Path to ChromeDriver
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run headless mode
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # Start the WebDriver
    driver = webdriver.Chrome(service=service, options=options)
    
    # Navigate to the YouTube channel videos page
    channel_url = 'https://www.youtube.com/c/MBCNEWS11/videos'  # Correct URL
    driver.get(channel_url)

    # Wait for the page to fully load
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'a#video-title-link')))

    # Scroll down to load more videos
    for _ in range(10):  # Adjust this range to load more videos if necessary
        driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
        time.sleep(2)  # Adjust sleep time if necessary

    # Get the page source and parse it with BeautifulSoup
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')

    # Find all video elements
    videos = soup.find_all('a', {'id': 'video-title-link'}, limit=100)

    if videos:
        for idx, video in enumerate(videos, start=1):
            video_url = 'https://www.youtube.com' + video.get('href')
            
            # Skip Shorts videos by checking the URL
            if '/shorts/' in video_url:
                print(f"Skipping Shorts video: {video_url}")
                continue
            
            title = video.get('title')
            print(f"Processing {idx}: {title}")
            print(f"Video URL: {video_url}")
            
            # Save the title to a text file
            title_file = os.path.join(save_dir, f'{idx}.txt')
            with open(title_file, 'w', encoding='utf-8') as f:
                f.write(title)
            print(f"Title saved: {title_file}")

            # yt-dlp options for downloading audio
            ydl_opts = {
                'format': 'bestaudio/best',
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '192',
                }],
                'outtmpl': os.path.join(save_dir, f'{idx}'),
            }

            # Download the audio using yt-dlp
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])
            print(f"Audio saved: {os.path.join(save_dir, f'{idx}.mp3')}")

            # 썸네일 다운로드
            thumbnail_url = f"https://img.youtube.com/vi/{video_url.split('v=')[1]}/maxresdefault.jpg"
            thumbnail_file = os.path.join(save_dir, f'{idx}.jpg')
            img_data = requests.get(thumbnail_url).content
            with open(thumbnail_file, 'wb') as handler:
                handler.write(img_data)
            print(f"Thumbnail downloaded: {thumbnail_file}")
            
            # Delay between requests to avoid being blocked
            time.sleep(5)

    else:
        print("No video elements found")
        driver.quit()
        exit()

except Exception as e:
    print(f"Extract data task failed: {e}")
    raise

finally:
    # Quit the WebDriver
    if driver is not None:
        print('Closing WebDriver')
        driver.quit()

print("Extract data task completed successfully")
