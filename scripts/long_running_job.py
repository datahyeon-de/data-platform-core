from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("Airflow-Spark-Long-Test").getOrCreate()

print("Spark Job 시작! 3분간 대기하며 리소스를 점유합니다.")
# 리소스를 점유하며 유지되는 시간을 벌기 위해 sleep 사용
time.sleep(360) 

df = spark.createDataFrame([(1, "Success"), (2, "Running")], ["id", "status"])
df.show()

print("Spark Job 완료!")
spark.stop()