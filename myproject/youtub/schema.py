import graphene
from graphene_django.types import DjangoObjectType
from .models import youtub
import os
import random

class youtubType(DjangoObjectType):
    class Meta:
        model = youtub

class UploadRandomFiles(graphene.Mutation):
    success = graphene.Boolean()

    def mutate(self, info):
        # 파일이 저장된 디렉토리
        data_dir = '/root/flickdone/data'
        files = os.listdir(data_dir)

        # 파일 이름의 숫자 부분 추출
        file_numbers = set(int(os.path.splitext(f)[0]) for f in files if f.split('.')[0].isdigit())

        # 랜덤으로 하나의 번호 선택
        selected_number = random.choice(list(file_numbers))

        # 선택된 번호와 일치하는 파일들을 처리
        file_set = {
            'image': None,
            'audio': None,
            'text': None,
        }

        for file_name in files:
            if file_name.startswith(str(selected_number)):
                file_path = os.path.join(data_dir, file_name)
                _, ext = os.path.splitext(file_name)

                if ext == '.jpg' or ext == '.jpeg' or ext == '.png':
                    file_set['image'] = file_path
                elif ext == '.mp3':
                    file_set['audio'] = file_path
                elif ext == '.txt':
                    file_set['text'] = file_path

        # MediaFile 모델에 파일 저장
        for file_type, file_path in file_set.items():
            if file_path:
                MediaFile.objects.create(
                    file_name=os.path.basename(file_path),
                    file_type=file_type,
                    file_path=file_path
                )

        return UploadRandomFiles(success=True)

class Mutation(graphene.ObjectType):
    upload_random_files = UploadRandomFiles.Field()

schema = graphene.Schema(mutation=Mutation)
