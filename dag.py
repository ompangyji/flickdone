from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pytube import YouTube
from pydub import AudioSegment
from PIL import Image

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

dag = DAG(
    'multimodal_pipeline',
    default_args=default_args,
    description='A multimodal data processing pipeline',
    schedule_interval=timedelta(days=1),
)

def extract_data(**kwargs):
    yt = YouTube('https://www.youtube.com/watch?v=your_video_id')
    
    # 오디오 다운로드
    audio_stream = yt.streams.filter(only_audio=True).first()
    audio_file = '/tmp/audio.mp4'
    audio_stream.download(output_path='/tmp', filename='audio.mp4')
    
    # 썸네일 다운로드
    thumbnail_url = yt.thumbnail_url
    thumbnail_file = '/tmp/thumbnail.jpg'
    img_data = Image.open(requests.get(thumbnail_url, stream=True).raw)
    img_data.save(thumbnail_file)
    
    # 제목 추출
    title = yt.title

    # xcom에 저장
    kwargs['ti'].xcom_push(key='title', value=title)
    kwargs['ti'].xcom_push(key='audio_file', value=audio_file)
    kwargs['ti'].xcom_push(key='thumbnail_file', value=thumbnail_file)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = BashOperator(
    task_id='transform_data',
    bash_command="""
    spark-submit /path/to/your/spark_transform.py "{{ ti.xcom_pull(task_ids='extract_data', key='title') }}" "{{ ti.xcom_pull(task_ids='extract_data', key='audio_file') }}" "{{ ti.xcom_pull(task_ids='extract_data', key='thumbnail_file') }}"
    """,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=lambda: print("Data loaded successfully"),  # 여기에 필요한 로드 작업을 추가
    dag=dag,
)

extract_task >> transform_task >> load_task
