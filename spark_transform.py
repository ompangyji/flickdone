from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pydub import AudioSegment
from PIL import Image
import mysql.connector
import sys

def transform_data(title, audio_file, thumbnail_file):
    # Spark 세션 생성
    spark = SparkSession.builder \
        .appName("YouTubeMultimodalETL") \
        .getOrCreate()

    # 제목 변환: 제목의 길이 계산
    data = [(title,)]
    df = spark.createDataFrame(data, ['title'])
    df = df.withColumn('title_length', F.length(F.col('title')))
    
    # 오디오 변환: MP3로 변환
    audio = AudioSegment.from_file(audio_file)
    audio_export_path = "/tmp/audio_converted.mp3"
    audio.export(audio_export_path, format='mp3')
    
    # 이미지 변환: 크기 조정
    img = Image.open(thumbnail_file)
    img = img.resize((128, 128))
    thumbnail_resized_path = "/tmp/thumbnail_resized.jpg"
    img.save(thumbnail_resized_path)
    
    # 데이터프레임 출력 확인
    df.show()

    # 변환된 데이터 MySQL에 적재
    df.write.format('jdbc').options(
        url='jdbc:mysql://localhost/your_database',
        driver='com.mysql.cj.jdbc.Driver',
        dbtable='youtube_titles',
        user='your_username',
        password='your_password'
    ).mode('append').save()

if __name__ == "__main__":
    title = sys.argv[1]
    audio_file = sys.argv[2]
    thumbnail_file = sys.argv[3]
    
    transform_data(title, audio_file, thumbnail_file)
