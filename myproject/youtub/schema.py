import graphene
import base64  # base64도 임포트 필요
from graphene_django.types import DjangoObjectType
from .models import youtub
import os
import random

# MediaFile 클래스 정의
class MediaFile(graphene.ObjectType):
    fileName = graphene.String()
    fileType = graphene.String()
    fileContent = graphene.String()

# UploadRandomFiles 클래스 정의
class UploadRandomFiles(graphene.Mutation):
    success = graphene.Boolean()
    mediaFiles = graphene.List(MediaFile)

    def mutate(self, info):
        # 파일이 저장된 디렉토리
        data_dir = '/root/flickdone/data'
        files = os.listdir(data_dir)

        # 파일 이름의 숫자 부분 추출
        file_numbers = set(int(os.path.splitext(f)[0]) for f in files if f.split('.')[0].isdigit())

        # 랜덤으로 하나의 번호 선택
        selected_number = random.choice(list(file_numbers))

        # 선택된 번호와 일치하는 파일들을 처리
        media_files = []
        file_set = {
            'image': None,
            'audio': None,
            'text': None,
        }

        for file_name in files:
            if file_name.startswith(str(selected_number)):
                file_path = os.path.join(data_dir, file_name)
                _, ext = os.path.splitext(file_name)

                if ext in ['.jpg', '.jpeg', '.png']:
                    file_set['image'] = file_path
                elif ext == '.mp3':
                    file_set['audio'] = file_path
                elif ext == '.txt':
                    file_set['text'] = file_path

        for file_type, file_path in file_set.items():
            if file_path:
                with open(file_path, 'rb') as f:
                    if file_type == 'text':
                        file_content = f.read().decode('utf-8')  # 텍스트 파일인 경우 UTF-8로 디코딩
                    else:
                        file_content = base64.b64encode(f.read()).decode('utf-8')  # 바이너리 파일은 base64로 인코딩

                media_files.append(MediaFile(
                    fileName=os.path.basename(file_path),
                    fileType=file_type,
                    fileContent=file_content
                ))

        return UploadRandomFiles(success=True, mediaFiles=media_files)

class Mutation(graphene.ObjectType):
    upload_random_files = UploadRandomFiles.Field()

class Query(graphene.ObjectType):
    hello = graphene.String(default_value="Hello, World!")

schema = graphene.Schema(query=Query, mutation=Mutation)
