import os
import base64
import requests
import yt_dlp
from PIL import Image
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import requests
from PIL import Image
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    dag_id='multimodal_pipeline',
    default_args=default_args,
    description='A multimodal data processing pipeline',
    schedule_interval=timedelta(days=1),
)

def get_next_index(directory):
    """주어진 디렉토리 내 파일의 개수를 세어 다음 인덱스를 반환합니다."""
    file_count = len(os.listdir(directory))
    return file_count + 1

# GraphQL API 호출 및 파일 저장 함수(데이터 추출)
def extract_data(**kwargs):
    try:
        # GraphQL API 엔드포인트
        graphql_url = 'http://localhost:8000/graphql/'

        # GraphQL 쿼리 또는 뮤테이션 (데이터 추출)
        mutation = """
        mutation {
          uploadRandomFiles {
            success
            mediaFiles {
              fileName
              fileType
              fileContent
            }
          }
        }
        """

        # GraphQL API 호출
        response = requests.post(graphql_url, json={'query': mutation})

        # 응답 처리
        if response.status_code == 200:
            data = response.json()

            if data.get("data", {}).get("uploadRandomFiles", {}).get("success"):
                print("Data extraction and upload successful.")
                media_files = data["data"]["uploadRandomFiles"]["mediaFiles"]
                
                # 저장 경로 설정
                save_dir = '/root/flickdone/ext'

                # 다음 인덱스 가져오기
                idx = get_next_index(save_dir)

                # 저장된 파일 경로 목록
                saved_files = {}

                # 각 파일을 적절한 이름과 확장자로 저장
                for media_file in media_files:
                    file_type = media_file["fileType"]
                    file_content = media_file["fileContent"]
                    
                    if file_type == 'text':
                        file_path = os.path.join(save_dir, f'ext{idx}.txt')
                        saved_files['text'] = file_path
                        # 텍스트 파일은 그대로 저장
                        with open(file_path, 'w', encoding='utf-8') as file:
                            file.write(file_content)
                    elif file_type in ['image', 'audio']:
                        # 이미지나 오디오 파일은 base64로 인코딩된 데이터일 것임
                        file_path = os.path.join(save_dir, f'ext{idx}.{"jpg" if file_type == "image" else "mp3"}')
                        saved_files[file_type] = file_path
                        with open(file_path, 'wb') as file:
                            file.write(base64.b64decode(file_content))
                    else:
                        continue

                print(f"Files saved with index {idx}.")

                # 파일 경로를 XCom에 저장
                kwargs['ti'].xcom_push(key='text_file', value=saved_files.get('text'))
                kwargs['ti'].xcom_push(key='image_file', value=saved_files.get('image'))
                kwargs['ti'].xcom_push(key='audio_file', value=saved_files.get('audio'))

            else:
                print("Data extraction failed.")
        else:
            print(f"GraphQL API 요청 실패: {response.status_code}, {response.text}")
            response.raise_for_status()

    except Exception as e:
        print(f"Error in extract_data task: {e}")
        raise

# 데이터 추출 작업 정의 (PythonOperator 사용)
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

# 데이터 변환 작업 정의 (Spark 스크립트 실행)
transform_task = BashOperator(
    task_id='transform_data',
    bash_command="""
    set -e
    echo "Starting transform task"
    spark-submit /root/flickdone/spark_transform.py "{{ ti.xcom_pull(task_ids='extract_data', key='text_file') }}" "{{ ti.xcom_pull(task_ids='extract_data', key='audio_file') }}" "{{ ti.xcom_pull(task_ids='extract_data', key='image_file') }}"
    echo "Transform task completed successfully"
    """,
    dag=dag,
)


# 데이터 로드 작업 정의
def load_data():
    try:
        print("Starting load data task")
        # 여기에 데이터를 로드하는 로직을 추가하세요
        print("Data loaded successfully")
    except Exception as e:
        print(f"Load data task failed: {e}")
        raise

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# 작업 간의 순서 정의
extract_task >> transform_task >> load_task
