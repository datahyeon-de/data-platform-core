from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
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

    # 2. Spark Job 제출
    submit_spark = SparkKubernetesOperator(
        task_id='submit_spark_job',
        namespace='spark',
        application_file="""
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: long-test-{{ ds_nodash }}
spec:
  type: Python
  mode: cluster
  image: "hyeondata/spark-py-aws:3.5.7-v1"
  mainApplicationFile: "s3a://datalake/scripts/long_running_job.py"
  sparkVersion: "3.5.7"
  serviceAccount: spark-sa
  sparkConf:
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
    "spark.hadoop.fs.s3a.endpoint": "http://192.168.0.14:9000"
    "spark.hadoop.fs.s3a.path.style.access": "true"
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
    "spark.hadoop.fs.s3a.metadatastore.impl": "org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore"
    "spark.hadoop.fs.s3a.endpoint.region": "us-east-1"
    "spark.hadoop.fs.s3a.signing-algorithm": "S3SignerType"
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2"
    "spark.eventLog.enabled": "true"
    "spark.eventLog.dir": "s3a://datalake/logs/spark-log/"
  driver:
    cores: 1
    memory: "512m"
    serviceAccount: spark-sa
    labels:              
      version: 3.5.7
    env:
      # S3A 커넥터는 아래 환경 변수 이름을 기본으로 읽습니다.
      - name: AWS_ACCESS_KEY_ID
        valueFrom:
          secretKeyRef:
            name: minio-s3-keys
            key: access-key
      - name: AWS_SECRET_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-s3-keys
            key: secret-key
  executor:
    cores: 1
    instances: 1
    memory: "512m"
    labels:              
      version: 3.5.7
    env:
      - name: AWS_ACCESS_KEY_ID
        valueFrom:
          secretKeyRef:
            name: minio-s3-keys
            key: access-key
      - name: AWS_SECRET_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-s3-keys
            key: secret-key
""",
    )

    upload_script >> submit_spark