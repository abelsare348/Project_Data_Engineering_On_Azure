# Databricks notebook source
# MAGIC %md
# MAGIC <center>This Notebook is used to create Treatment_Cost_Analysis Table in Enterprise Level.</center>
# MAGIC
# MAGIC <table>
# MAGIC <tr><th>Title</th><th>Description</th></tr>
# MAGIC <tr><td>Creator</td><td>Aniket Belsare</td></tr>
# MAGIC <tr><td>Reviewer</td><td>Ankit Negi</td></tr>
# MAGIC <tr><td>DataSets Used</td><td><ul><li>refine_treatment</li>
# MAGIC <li>refine_visit</li>
# MAGIC </ul></td></tr>
# MAGIC </table>

# COMMAND ----------

treatmentDF=spark.read.table("refine.treatment")

# COMMAND ----------

visitsDF=spark.read.table("refine.visit")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

treatmentCostAnalysisDF=treatmentDF.alias("t") \
    .join(visitsDF.alias("v"), "Treatment_ID") \
    .groupBy("t.Treatment_ID", "t.Treatment_Type") \
    .agg(
        F.count("v.Visit_ID").alias("Total_Treatments"),
        F.sum("t.Cost").alias("Total_Cost"),
        F.avg("t.Cost").alias("Avg_Cost_Per_Treatment")
    ) \
    .orderBy(F.desc("Total_Cost"))

# COMMAND ----------

delta_table_path = "abfss://output@freefinalstorageaccount.dfs.core.windows.net/enterprise/treatmentCostAnalysis/table/"

try:
    # Try to access the Delta table
    treatmentCostAnalysisEnterpriseDF= DeltaTable.forPath(spark, delta_table_path)

    # If the table exists, perform the merge operation
    treatmentCostAnalysisEnterpriseDF.alias("target").merge(
        treatmentCostAnalysisDF.alias("source"),
        "target.Treatment_ID = source.Treatment_ID",
        "target.Treatment_Type = source.Treatment_Type"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()

except Exception as e:
    print("Delta table does not exist. Creating a new table...")
    treatmentCostAnalysisDF.write.format("delta").mode("overwrite").option("path",delta_table_path).saveAsTable("enterprise.treatmentCostAnalysis")
