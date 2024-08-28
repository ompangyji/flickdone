# flickdone

## 아키텍처
개발환경 : wsl-ubuntu 22.04.3LTS 
데이터베이스 : MySQL 
데이터 파이프라인 도구 : Apache Airflow 
데이터 저장 및 분산 처리 도구 : Apache Spark 
형상관리 : git 
코딩 툴 : vscode

## 설치방법
### git
sudo apt-get install -y git

### MySQL
sudo apt-get update
sudo apt-get install -y mysql-server
##### 설치확인
sudo /etc/init.d/mysql start

sudo mysql -u root -p (최초 패스워드 입력시 엔터)
mysql> alter user 'root'@'localhost' identified with mysql_native_password by 'root1234'; 

## Python 및 가상환경 설치
sudo apt-get install -y python3 python3-pip python3-venv
python3 -m venv airflow_venv
##### 설치 확인
source airflow_venv/bin/activate

## Apache Airflow
### 설치
pip install apache-airflow\==2.6.0 apache-airflow-providers-mysql\==3.4.0 apache-airflow-providers-apache-spark\==4.0.0

설치 중 에러나면 실행(`pkg-config`와 MySQL 개발 라이브러리를 설치)
sudo apt-get update 
sudo apt-get install -y pkg-config
sudo apt-get install -y libmysqlclient-dev
###  Airflow 초기화 및 웹서버 시작
pip uninstall pydantic
pip install pydantic\==1.10.9

pip uninstall flask-session 
pip install flask-session\==0.4.0

mkdir /var/lib/airflow && cd /var/lib/airflow
mkdir dags
export AIRFLOW_HOME=/var/lib/airflow
airflow db init

init 에러시
airflow db reset
airflow db init

airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin1234
##### 설치 확인
airflow webserver --port 8080
airflow scheduler

### Spark 설치
wget 'apache spark 다운로드페이지에서 링크참고'
tar xvf 설치파일
sudo mv spark-3.4.0-bin-hadoop3 /opt/spark

#### 환경변수 설정
vi ~/.bashrc
아래 내용 추가
export SPARK_HOME=/opt/spark  
export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin
source ~/.bashrc

#### 자바jdk 설치
##### 자바설치
sudo apt update 
sudo apt install openjdk-11-jdk

##### JAVA_HOME 설정
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc 
echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> ~/.bashrc
source ~/.bashrc

##### 설치 완료 체크
pyspark

