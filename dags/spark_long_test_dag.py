from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.models import Connection
from datetime import datetime

with DAG(
    dag_id='spark_long_running_test',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    # 1. 스크립트를 MinIO로 업로드 (git-sync로 가져온 파일을 복사)
    upload_script = S3CreateObjectOperator(
        task_id='upload_pyspark_script',
        s3_bucket='datalake',
        s3_key='scripts/long_running_job.py',
        # git-sync 경로 주의: subPath 설정에 따라 달라질 수 있음
        data=open('/opt/airflow/dags/repo/scripts/long_running_job.py', 'rb').read(),
        aws_conn_id='minio_s3_conn',
        replace=True
    )
    conn = Connection.get_connection_from_secrets('minio_s3_conn')
    access_key = conn.login
    secret_key = conn.password

    submit_spark = SparkKubernetesOperator(
        task_id='submit_spark_job',
        namespace='spark',
        application_file=f"""
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: long-test-{{{{ ds_nodash }}}}
spec:
  type: Python
  mode: cluster
  image: "hyeondata/spark-py-aws:3.5.7-v1"
  mainApplicationFile: "s3a://datalake/scripts/long_running_job.py"
  sparkVersion: "3.5.7"
  serviceAccount: spark-sa
  
  # [해결책] 모든 S3A 설정을 hadoopConf로 통합하여 초기 다운로드 단계를 통과시킵니다.
  hadoopConf:
    "fs.s3a.endpoint": "http://192.168.0.14:9000"
    "fs.s3a.path.style.access": "true"
    "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
    "fs.s3a.connection.ssl.enabled": "false"
    "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    "fs.s3a.access.key": "{access_key}"
    "fs.s3a.secret.key": "{secret_key}"
    # 아래 설정들을 sparkConf에서 이리로 옮겼습니다.
    "fs.s3a.endpoint.region": "us-east-1"
    "fs.s3a.signing-algorithm": "S3SignerType"
    "fs.s3a.change.detection.mode": "none" # 400 에러 방지를 위한 추가 옵션

  sparkConf:
    "spark.eventLog.enabled": "true"
    "spark.eventLog.dir": "s3a://datalake/logs/spark-log/"
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2"

  driver:
    cores: 1
    memory: "512m"
    serviceAccount: spark-sa
    labels:              
      version: 3.5.7
  executor:
    cores: 1
    instances: 1
    memory: "512m"
    labels:              
      version: 3.5.7
""",
    )

    upload_script >> submit_spark