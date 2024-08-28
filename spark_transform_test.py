from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pydub import AudioSegment
from PIL import Image

def transform_data():
    try:
        # 임의의 데이터 설정
        title = "Sample Video Title2"
        audio_file = "/root/flickdone/data/2.mp3"  # 임의의 오디오 파일 경로
        thumbnail_file = "//root/flickdone/data/2.jpg"  # 임의의 썸네일 파일 경로

        # Spark 세션 생성
        spark = SparkSession.builder \
            .appName("YouTubeMultimodalETL_Test") \
            .config("spark.jars", "/opt/spark/jars/mysql-connector-java-9.0.0.jar") \
            .getOrCreate()

        # 데이터프레임 생성
        data = [(title, audio_file, thumbnail_file)]
        df = spark.createDataFrame(data, ['title', 'audio_file', 'thumbnail_file'])
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
    transform_data()
