from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.serverless(True).getOrCreate()

# 1. Create the target table (represents existing, already-loaded data)
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.customers_target (
  customer_id INT,
  name STRING,
  email STRING,
  is_deleted BOOLEAN
)
""")

spark.sql("""
INSERT INTO workspace.default.customers_target VALUES
  (1, 'Alice', 'alice@old.com', false),
  (2, 'Bob', 'bob@example.com', false)
""")

# 2. Create the source/changes table (represents an incoming CDC batch)
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.customers_changes (
  customer_id INT,
  name STRING,
  email STRING,
  operation STRING
)
""")

spark.sql("""
INSERT INTO workspace.default.customers_changes VALUES
  (1, 'Alice', 'alice@new.com', 'UPDATE'),
  (3, 'Charlie', 'charlie@example.com', 'INSERT'),
  (2, 'Bob', 'bob@example.com', 'DELETE')
""")

# 3. Apply the changes with MERGE INTO
spark.sql("""
MERGE INTO workspace.default.customers_target AS target
USING workspace.default.customers_changes AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND source.operation = 'DELETE'
  THEN UPDATE SET target.is_deleted = true
WHEN MATCHED AND source.operation = 'UPDATE'
  THEN UPDATE SET target.email = source.email
WHEN NOT MATCHED AND source.operation = 'INSERT'
  THEN INSERT (customer_id, name, email, is_deleted)
  VALUES (source.customer_id, source.name, source.email, false)
""")

# 4. Verify the result
print("Result after CDC merge:")
spark.sql("SELECT * FROM workspace.default.customers_target ORDER BY customer_id").show()