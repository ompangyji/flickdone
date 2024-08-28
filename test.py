from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import requests
from PIL import Image
import yt_dlp
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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
    channel_url = 'https://www.youtube.com'  # Correct URL
    driver.get(channel_url)

    # Wait for the page to fully load
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'a#video-title-link')))

    # Get the page source and parse it with BeautifulSoup
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')

    # Find the first video element
    video = soup.find('a', {'id': 'video-title-link'})  # Corrected ID

    if video is not None:
        title = video.get('title')
        video_url = 'https://www.youtube.com' + video.get('href')
        print(f"Title: {title}")
        print(f"Video URL: {video_url}")
    else:
        print("Video element not found")
        driver.quit()
        exit()

    # yt-dlp options for downloading audio
    ydl_opts = {
        'format': 'bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        'outtmpl': '/tmp/audio.mp3',
    }

    # Download the audio using yt-dlp
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([video_url])

    # Find and download the thumbnail image
    thumbnail_tag = soup.find('img', {'src': True})
    if thumbnail_tag:
        thumbnail_url = thumbnail_tag['src']
        thumbnail_file = '/tmp/thumbnail.jpg'
        img_data = Image.open(requests.get(thumbnail_url, stream=True).raw)
        img_data.save(thumbnail_file)
        print(f"Thumbnail downloaded: {thumbnail_file}")
    else:
        print("Thumbnail not found")

    # Complete the extraction
    print("Extract data task completed successfully")

except Exception as e:
    print(f"Extract data task failed: {e}")
    raise

finally:
    # Quit the WebDriver
    if driver is not None:
        print('Closing WebDriver')
        driver.quit()
