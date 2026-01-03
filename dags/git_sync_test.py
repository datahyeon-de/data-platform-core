from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='git_sync_verification',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # 수동 실행
    catchup=False,
    tags=['test']
) as dag:

    # 레포지토리 전체 구조 출력
    print_structure = BashOperator(
        task_id='print_repo_structure',
        # git-sync는 보통 /opt/airflow/dags/repo 아래에 전체 레포를 가져옵니다.
        bash_command='ls -R /opt/airflow/dags/repo'
    )

    # 현재 작업 디렉토리 확인
    print_pwd = BashOperator(
        task_id='print_pwd',
        bash_command='pwd && ls -al'
    )

    print_structure >> print_pwd