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
        namespace='spark', # 우리가 만든 spark 네임스페이스
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
  nodeSelector:
    workload-type: spark-executor
  driver:
    cores: 1
    memory: "512m"
    serviceAccount: spark-sa
  executor:
    cores: 1
    instances: 1
    memory: "512m"
""",
    )

    upload_script >> submit_spark