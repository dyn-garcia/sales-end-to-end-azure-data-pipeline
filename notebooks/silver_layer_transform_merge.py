# Databricks notebook source
# MAGIC %md
# MAGIC ## Azure ADLS Connection Setup in Databricks

# COMMAND ----------

from datetime import datetime, timedelta, timezone# ─── Variables ─────────────────────────────────────────────

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
# MAGIC #### Read & Transform Entity Data

# COMMAND ----------

# =======================================
# Silver: Consume Bronze CDF -> Transform -> MERGE (C/U/D) with watermark
# =======================================
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# --------- Widgets / Params ---------
dbutils.widgets.text("entity", "sales")                   # sales | product | customer | store | date
entity = dbutils.widgets.get("entity").lower().strip()

# --------- Paths ----------
bronze_base = "abfss://bronze@storagesalesprodcontoso.dfs.core.windows.net"
silver_base = "abfss://silver@storagesalesprodcontoso.dfs.core.windows.net"
bronze_delta_path = f"{bronze_base}/{entity}_delta"       # Bronze Delta with CDF ON
silver_path       = f"{silver_base}/{entity}"             # Silver Delta
meta_path         = f"{silver_base}/_meta/cdf_watermarks" # watermark table

# --------- Entities / Keys ----------
ALLOWED = {"sales","store","product","customer","date"}
if entity not in ALLOWED:
    print(f"[{entity}] not supported. Exiting.")
    dbutils.notebook.exit("SKIPPED")

KEYS = {
    "sales":   ["orderkey","linenumber"],
    "store":   ["storekey"],
    "product": ["productkey"],
    "customer":["customerkey"],
    "date":    ["date"]
}
key_cols = KEYS[entity]

# --------- Helpers ----------
def ensure_watermark_table(path: str):
    try:
        if DeltaTable.isDeltaTable(spark, path): return
    except Exception:
        pass
    (spark.createDataFrame([], "entity STRING, last_version LONG")
          .write.format("delta").mode("overwrite").save(path))

def get_last_version(path: str, entity: str) -> int:
    try:
        df = (spark.read.format("delta").load(path)
                .where(F.col("entity")==entity)
                .select("last_version").limit(1))
        rows = df.collect()
        return rows[0]["last_version"] if rows else -1
    except Exception:
        return -1

def upsert_watermark(path: str, entity: str, version: int):
    ensure_watermark_table(path)
    tbl = DeltaTable.forPath(spark, path)
    payload = spark.createDataFrame([(entity, version)], "entity STRING, last_version LONG")
    (tbl.alias("t").merge(payload.alias("s"), "t.entity = s.entity")
        .whenMatchedUpdate(set={"last_version": "s.last_version"})
        .whenNotMatchedInsert(values={"entity": "s.entity", "last_version": "s.last_version"})
        .execute())

def add_row_hash_excluding_meta(df, key_cols, extra_exclude=None):
    ignore = set(key_cols) | set(extra_exclude or [])
    meta_cols = {c for c in df.columns if c.startswith("_")}
    compare_cols = [c for c in df.columns if c not in ignore | meta_cols]
    exprs = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in compare_cols]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *exprs), 256))

# --------- Domain Transforms ----------
def to_usd(col_name):
    return F.when(
        (F.col("exchangerate").isNull()) | (F.col("exchangerate") <= 0) | (F.col(col_name).isNull()),
        F.lit(None).cast("double")
    ).otherwise(F.col(col_name) * F.col("exchangerate"))

def transform_sales(df):
    return (df
        .withColumn("unitprice", F.round(to_usd("unitprice"), 2))
        .withColumn("unitcost",  F.round(to_usd("unitcost"),  2))
        .withColumn("netprice",  F.round(to_usd("netprice"),  2))
        .drop("currencycode","exchangerate","ingest_date","source_file"))

def transform_customer(df):
    return (df
        .withColumn("gender",      F.initcap(F.trim(F.col("gender"))))
        .withColumn("title",       F.upper(F.trim(F.col("title"))))
        .withColumn("city",        F.initcap(F.trim(F.col("city"))))
        .withColumn("countryfull", F.initcap(F.trim(F.col("countryfull"))))
        .withColumn("full_name",
            F.concat_ws(" ",
                F.col("givenname"),
                F.when(F.col("middleinitial").isNull() | (F.col("middleinitial")==""), None)
                 .otherwise(F.concat(F.col("middleinitial"), F.lit("."))),
                F.col("surname")
            )
        )
        .withColumn("full_address",
            F.concat_ws(", ", "streetaddress", "city", "statefull", "countryfull", "zipcode"))
        .withColumn("is_active",
            F.when(F.current_timestamp().between(F.col("startdt"), F.col("enddt")), F.lit(1)).otherwise(F.lit(0)))
        .drop("age","source_file")
        .withColumn("age",
            F.when(F.date_format(F.current_date(),"MMdd") >= F.date_format(F.col("birthday"),"MMdd"),
                   F.year(F.current_date())-F.year(F.col("birthday")))
             .otherwise(F.year(F.current_date())-F.year(F.col("birthday"))-1))
    )

def transform_product(df):
    oz_to_g = 28.3495
    lb_to_g = 453.592
    return (df
        .withColumn("weight",
            F.when(F.col("weightunit")=="grams",  F.col("weight"))
             .when(F.col("weightunit")=="ounces", F.col("weight")*oz_to_g)
             .when(F.col("weightunit")=="pounds", F.col("weight")*lb_to_g)
             .otherwise(F.col("weight")))
        .withColumn("weight", F.round("weight",2))
        .withColumn("weightunit", F.lit("grams"))
        .drop("ingest_date","source_file"))

def transform_store(df):
    return (df
        .withColumn("status", F.when(F.col("closedate").isNull(), F.lit("Active")).otherwise(F.lit("Closed")))
        .drop("ingest_date","source_file"))

def transform_date(df):
    return df.drop("ingest_date","source_file")

TRANSFORM = {
    "sales":    transform_sales,
    "customer": transform_customer,
    "product":  transform_product,
    "store":    transform_store,
    "date":     transform_date
}
transform_fn = TRANSFORM[entity]

# --------- Main ---------
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
ensure_watermark_table(meta_path)

# 1) Bronze must exist
try:
    bronze_dt = DeltaTable.forPath(spark, bronze_delta_path)
except Exception as e:
    raise FileNotFoundError(f"[{entity}] Bronze Delta path not found: {bronze_delta_path}") from e

# 2) Version window & bootstrap check
hist_df = bronze_dt.history()
current_ver = hist_df.select("version").agg(F.max("version")).first()[0]
last_ver    = get_last_version(meta_path, entity)
start_ver   = last_ver + 1

cdf_enable_rows = (
    hist_df
    .where(F.col("operation")=="SET TBLPROPERTIES")
    .where(F.lower(F.col("operationParameters").cast("string")).contains("delta.enablechangedatafeed"))
    .where(F.lower(F.col("operationParameters").cast("string")).contains("true"))
    .select("version").orderBy("version").collect()
)
if cdf_enable_rows:
    first_cdf_commit_ver = cdf_enable_rows[0]["version"]
    cdf_first_readable_ver = first_cdf_commit_ver + 1
else:
    cdf_first_readable_ver = 0

needs_bootstrap = (start_ver < cdf_first_readable_ver)

if start_ver > current_ver:
    print(f"[{entity}] No new versions. last_ver={last_ver}, current_ver={current_ver}")
    dbutils.notebook.exit("NO_CHANGES")

if needs_bootstrap:
    print(f"[{entity}] Bootstrapping Silver (start_ver={start_ver} < CDF start={cdf_first_readable_ver})")
    snapshot = spark.read.format("delta").load(bronze_delta_path)
    snap_tx = transform_fn(snapshot)
    snap_tx = add_row_hash_excluding_meta(snap_tx, key_cols)

    if not DeltaTable.isDeltaTable(spark, silver_path):
        (snap_tx.limit(0).write.format("delta").mode("overwrite").save(silver_path))
    tgt = DeltaTable.forPath(spark, silver_path)

    on_expr = " AND ".join([f"t.{k}=s.{k}" for k in key_cols])
    set_all = {c: f"s.{c}" for c in snap_tx.columns}
    if snap_tx.limit(1).count() > 0:
        (tgt.alias("t")
            .merge(snap_tx.alias("s"), on_expr)
            .whenMatchedUpdate(condition="t.row_hash <> s.row_hash", set=set_all)
            .whenNotMatchedInsert(values=set_all)
            .execute())
        print(f"[{entity}] Bootstrap snapshot merged into Silver.")
    upsert_watermark(meta_path, entity, current_ver)
    print(f"[{entity}] Watermark set to {current_ver}. Bootstrap complete.")
    dbutils.notebook.exit("BOOTSTRAPPED")

# 3) Normal incremental read
print(f"[{entity}] Reading CDF versions {start_ver}..{current_ver}")
cdf = (spark.read.format("delta")
       .option("readChangeFeed","true")
       .option("startingVersion", start_ver)
       .option("endingVersion",   current_ver)
       .load(bronze_delta_path))

# 4) Split into upserts and deletes
cdf_upserts = cdf.where(F.col("_change_type").isin("insert","update_postimage"))
cdf_deletes = cdf.where(F.col("_change_type")=="delete").select(*key_cols).dropDuplicates()

# 5) Transform changed rows
df_upserts_tx = transform_fn(cdf_upserts)
df_upserts_tx = add_row_hash_excluding_meta(df_upserts_tx, key_cols)

# 6) Ensure Silver exists
if not DeltaTable.isDeltaTable(spark, silver_path):
    (df_upserts_tx.limit(0).write.format("delta").mode("overwrite").save(silver_path))
tgt = DeltaTable.forPath(spark, silver_path)

# 7) MERGE upserts
on_expr = " AND ".join([f"t.{k}=s.{k}" for k in key_cols])
set_all = {c: f"s.{c}" for c in df_upserts_tx.columns}
if df_upserts_tx.limit(1).count() > 0:
    (tgt.alias("t")
        .merge(df_upserts_tx.alias("s"), on_expr)
        .whenMatchedUpdate(condition="t.row_hash <> s.row_hash", set=set_all)
        .whenNotMatchedInsert(values=set_all)
        .execute())
    print(f"[{entity}] Upserts merged into Silver.")

# 8) Apply deletes
if cdf_deletes.limit(1).count() > 0:
    (tgt.alias("t")
        .merge(cdf_deletes.alias("d"), " AND ".join([f"t.{k}=d.{k}" for k in key_cols]))
        .whenMatchedDelete()
        .execute())
    print(f"[{entity}] Deletes applied in Silver.")

# 9) Advance watermark
upsert_watermark(meta_path, entity, current_ver)
print(f"[{entity}] Watermark advanced to {current_ver}. Done.")

display(spark.read.format("delta").load(silver_path).limit(10))
