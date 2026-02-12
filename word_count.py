from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def run_spark_job():
	spark = SparkSession.builder \
		.appName("WordCountMaster") \
		.master("local[*]") \
		.getOrCreate()

	data = [
		("Spark is fast!",),
		("spark, is Fun.",),
		("Data Engineering, is cool",),
		("Spark is cool.",)
		]
	columns = ["sentence"]

	df = spark.createDataFrame(data, columns)
	print("Raw Data:")
	df.show(truncate=False)

	df = df.withColumn("words", F.split(df["sentence"], " "))
	df = df.withColumn("word", F.explode(df["words"]))

	stop_words = ["is", "the", "a", "an", "and"]
	
	#final_counts = df.groupBy(df["word"]).count()

	final_counts = df.withColumn("word", F.lower(F.col("word"))) \
			.withColumn("word", F.regexp_replace(F.col("word"), "[^a-z0-9]", "")) \
			.filter(~F.col("word").isin(stop_words)) \
			.filter(F.col("word") != "") \
			.groupBy("word") \
			.count()
	
	final_counts.write \
		.mode("overwrite") \
		.format("parquet") \
		.save("de-handbook/word_count")
	#df = df.withColumn("word", F.lower(F.col("word")))
	#df = df.withColumn("word", F.regexp_replace(F.col("word"), "[^a-z0-9]", ""))
	#df = df.filter(~F.col("word").isin(stop_words))
	
	print("Word Count")
	final_counts.show()

	verified_df = spark.read.parquet("de-handbook/word_count")
	verified_df.show()

	spark.stop()

if __name__ == "__main__":
	run_spark_job()
