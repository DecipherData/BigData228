from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Initialize SparkSession with Hive support
spark = SparkSession.builder \
    .appName("SaveAsTableExample") \
    .config("spark.sql.warehouse.dir", "/apps/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://sandbox-hdp.hortonworks.com:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("id", StringType(), True),
    StructField("selftext", StringType(), True),
    StructField("subredditvar", StringType(), True),
    StructField("title", StringType(), True),
    StructField("compound_score", FloatType(), True),
    StructField("sentiment", StringType(), True)
])

import random
import string

def generate_sample_data(num_samples=100):
    """Generate sample data with a specific structure."""
    data = []
    for _ in range(num_samples):
        # Generate a random ID with a prefix 't3_' and 6 random digits
        id = "t3_" + ''.join(random.choices(string.digits, k=6))

        # Generate random text for 'selftext' and 'title'
        selftext = ''.join(random.choices(string.ascii_letters + " ", k=random.randint(20, 100)))
        title = ''.join(random.choices(string.ascii_letters + " ", k=random.randint(10, 50)))

        # Fixed category for demonstration
        subredditvar = "Kafka"

        # Generate a random compound score between -1.0 and 1.0
        compound_score = round(random.uniform(-1.0, 1.0), 4)

        # Randomly select a sentiment label
        sentiment = random.choice(["negative", "neutral", "positive"])

        # Append the generated record to the dataset
        data.append((id, selftext, subredditvar, title, compound_score, sentiment))

    return data

# Generate 100 sample records
sample_data = generate_sample_data(100)
# Print the first 5 records to check
for record in sample_data[:5]:
    print(record)

# Sample data
#data = [("t3_1azyc86", "Some text", "Kafka", "Why Kafka's novels are so dark?", 0.2625, "negative"),
        #("t3_1azi9jz", "More text", "Kafka", "Nauseatingly miserable beyond repair?", -0.5994, "negative")]

# Create DataFrame with the specified schema
df = spark.createDataFrame(sample_data, schema=schema)

spark.sql("USE reddit_sentiments")

# Save the DataFrame as a Hive table
df.write.mode("overwrite").saveAsTable("reddit_sentiments.reddit_sentiment_analysis")

