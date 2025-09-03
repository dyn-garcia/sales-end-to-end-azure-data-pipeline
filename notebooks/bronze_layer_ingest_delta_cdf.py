# Databricks notebook source
# MAGIC %md
# MAGIC ## Azure ADLS Connection Setup in Databricks

# COMMAND ----------

storage_account   = "" #add your own storage acct
app_id            = "" #add your own app id
tenant_id         = "" #add your own tenant id

# Get secret securely from Key Vault
service_credential = "" #add your own service credential

# ─── Spark Config Setup ────────────────────────────────────
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", app_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", 
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze ingest (Delta with CDF)

# COMMAND ----------

# =======================
# Bronze: CSV Snapshot -> Delta (CDF ON) via MERGE (Unity Catalog–safe)
# =======================
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

dbutils.widgets.text("entity", "sales")
dbutils.widgets.text("proc_date", "2025-09-03")
dbutils.widgets.dropdown("apply_snapshot_deletes", "true", ["true","false"])

entity    = dbutils.widgets.get("entity").lower().strip()
proc_date = dbutils.widgets.get("proc_date").strip()
apply_snapshot_deletes = dbutils.widgets.get("apply_snapshot_deletes") == "true"

KEYS = {
    "sales":   ["orderkey","linenumber"],
    "store":   ["storekey"],
    "product": ["productkey"],
    "customer":["customerkey"],
    "date":    ["date"]
}
if entity not in KEYS:
    raise ValueError(f"Unsupported entity '{entity}'. Allowed: {sorted(KEYS)}")
key_cols = KEYS[entity]

raw_base          = "abfss://raw@storagesalesprodcontoso.dfs.core.windows.net"
bronze_base       = "abfss://bronze@storagesalesprodcontoso.dfs.core.windows.net"
csv_today_path    = f"{raw_base}/ingest_date={proc_date}/{entity}.csv"
bronze_delta_path = f"{bronze_base}/{entity}_delta"

def path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path); return True
    except Exception:
        return False

def add_row_hash(df, keys, exclude=None):
    excl = set(exclude or [])
    cols = [c for c in df.columns if c not in set(keys) | excl]
    exprs = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *exprs), 256))

# 1) Read today's CSV snapshot
if not path_exists(csv_today_path):
    raise FileNotFoundError(f"Missing CSV for {entity} on {proc_date}: {csv_today_path}")

reader = spark.read.option("header", True).option("inferSchema", True)
df_raw = reader.csv(csv_today_path)

# 2) Add lineage/meta (UC-safe: use _metadata.file_path instead of input_file_name())
df_stage = (df_raw
    .withColumn("ingest_date", F.lit(proc_date))
    .withColumn("source_file", F.col("_metadata.file_path"))  # <-- UC-safe
    .drop("_metadata")                                        # keep schema clean
)

# 3) Build row_hash (exclude keys + lineage cols)
df_stage = add_row_hash(df_stage, key_cols, exclude={"ingest_date", "source_file"})

# 4) Ensure Bronze table exists (Delta + CDF ON)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
if not DeltaTable.isDeltaTable(spark, bronze_delta_path):
    (df_stage.limit(0).write.format("delta").mode("overwrite").save(bronze_delta_path))
    spark.sql(f"""
        ALTER TABLE delta.`{bronze_delta_path}`
        SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)

bronze = DeltaTable.forPath(spark, bronze_delta_path)

# 5) MERGE snapshot (upserts)
on_expr = " AND ".join([f"t.{k}=s.{k}" for k in key_cols])
set_all = {c: f"s.{c}" for c in df_stage.columns}

(bronze.alias("t")
   .merge(df_stage.alias("s"), on_expr)
   .whenMatchedUpdate(condition="t.row_hash <> s.row_hash", set=set_all)
   .whenNotMatchedInsert(values=set_all)
   .execute())

# 6) Optional snapshot deletes (if source snapshot is authoritative)
if apply_snapshot_deletes:
    keys_today = df_stage.select(*key_cols).dropDuplicates()
    (bronze.alias("t")
       .merge(keys_today.alias("s"), on_expr)
       .whenNotMatchedBySourceDelete()
       .execute())

# 7) Log last operation
hist = bronze.history(1).select("version","operation","operationMetrics").collect()[0]
print(f"[BRONZE] {entity} MERGE complete for {proc_date} at {bronze_delta_path}")
print(f"Version={hist['version']} Operation={hist['operation']} Metrics={hist['operationMetrics']}")
display(spark.read.format("delta").load(bronze_delta_path).limit(10))
