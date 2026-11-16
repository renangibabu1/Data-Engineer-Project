
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.serverless().getOrCreate()

# Sample data
data = [
    (1, "Customer A", 50000),
    (2, "Customer B", 75000),
    (3, "Customer C", 60000)
]

columns = ["Customer_ID", "Customer_Name", "Income"]

# Create Spark DataFrame
df = spark.createDataFrame(data, columns)

# Display records
df.show()

# Count records
print("Total Records:", df.count())
print("Databricks connection successful!")
