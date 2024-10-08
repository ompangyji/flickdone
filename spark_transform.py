import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pydub import AudioSegment
from PIL import Image
import sys

def get_next_index(directory, extension):
    """주어진 디렉토리 내에서 특정 확장자를 가진 파일의 개수를 세어 다음 인덱스를 반환합니다."""
    files = [f for f in os.listdir(directory) if f.endswith(extension)]
    return len(files) + 1

def transform_data(title_file, audio_file, thumbnail_file):
    try:
        # Spark 세션 생성
        spark = SparkSession.builder \
            .appName("YouTubeMultimodalETL_Test") \
            .config("spark.jars", "/opt/spark/jars/mysql-connector-java-9.0.0.jar") \
            .getOrCreate()
    
        # title_file_path에 저장된 텍스트 파일에서 제목 읽기
        with open(title_file, 'r', encoding='utf-8') as f:
            title = f.read().strip()  # 파일의 내용을 읽어 title에 저장
        
        # 데이터프레임 생성
        data = [(title, audio_file, thumbnail_file)]
        df = spark.createDataFrame(data, ['title', 'audio_file', 'thumbnail_file'])

        # 변환된 데이터를 저장할 디렉토리 경로 설정
        transformed_dir = "/root/flickdone/transformed"

        # 오디오 파일 변환 및 저장
        audio_idx = get_next_index(transformed_dir, ".mp3")
        audio_export_path = f"{transformed_dir}/tf{audio_idx}.mp3"
        audio = AudioSegment.from_file(audio_file)
        audio.export(audio_export_path, format='mp3')

        # 이미지 파일 변환 및 저장
        image_idx = get_next_index(transformed_dir, ".jpg")
        thumbnail_resized_path = f"{transformed_dir}/tf{image_idx}.jpg"
        img = Image.open(thumbnail_file)
        img = img.resize((128, 128))
        img.save(thumbnail_resized_path)

        # 변환된 오디오 파일 경로 및 썸네일 경로 업데이트
        df = df.withColumn('audio_file', F.lit(audio_export_path)) \
               .withColumn('thumbnail_file', F.lit(thumbnail_resized_path))

        # 데이터프레임 출력 확인
        df.show()

        # 변환된 데이터 MySQL에 적재
        df.select(
            F.col('title'),
            F.col('audio_file'),
            F.col('thumbnail_file')
        ).write.format('jdbc').options(
            url='jdbc:mysql://localhost/flickdonedb',
            driver='com.mysql.cj.jdbc.Driver',
            dbtable='youtube_data',
            user='root',
            password='root1234'
        ).mode('append').save()

    except Exception as e:
        print(f"Error in transform_data: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    title = sys.argv[1]
    audio_file = sys.argv[2]
    thumbnail_file = sys.argv[3]
    
    transform_data(title, audio_file, thumbnail_file)
