from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("Airflow-Spark-Long-Test").getOrCreate()

print("Spark Job 시작! Stage를 반복 생성합니다.")

df = spark.createDataFrame([(i, "Running") for i in range(1, 1000)], ["id", "status"])
df = df.repartition(4)  # stage 유발(셔플)

for i in range(1, 7):  # 6번 반복 (대충 3분 정도)
    print(f"=== Iteration {i} ===")
    df.groupBy("status").count().show()  # shuffle + action => stage 확실히 생김
    time.sleep(30)  # 30초 쉬면서 UI 관찰

print("Spark Job 완료!")
spark.stop()