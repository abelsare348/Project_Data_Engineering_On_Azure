# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <center>This Notebook is used to create Patient_Visit_Frequency Table in Enterprise Level.</center>
# MAGIC
# MAGIC <table>
# MAGIC <tr><th>Title</th><th>Description</th></tr>
# MAGIC <tr><td>Creator</td><td>Aniket Belsare</td></tr>
# MAGIC <tr><td>Reviewer</td><td>Ankit Negi</td></tr>
# MAGIC <tr><td>DataSets Used</td><td><ul>
# MAGIC <li>refine_visit</li>
# MAGIC </ul></td></tr>
# MAGIC </table>

# COMMAND ----------

visitsDF=spark.read.table("refine.visit")

# COMMAND ----------

from pyspark.sql import functions as F 

# COMMAND ----------

patientVisitFrequencyDF=visitsDF \
    .groupBy("Patient_ID") \
    .agg(
        F.count("Visit_ID").alias("Total_Visits"),
        F.sum("Total_Cost").alias("Total_Cost_Incured"),
        F.avg("Total_Cost").alias("Avg_Cost_Per_Visit")
    ) \
    .orderBy(F.desc("Total_Visits"))


# COMMAND ----------

delta_table_path = "abfss://output@freefinalstorageaccount.dfs.core.windows.net/enterprise/patientVisitFrequency/table/"

try:
    # Try to access the Delta table
    patientVisitFrequencyEnterpriseDF= DeltaTable.forPath(spark, delta_table_path)

    # If the table exists, perform the merge operation
    patientVisitFrequencyEnterpriseDF.alias("target").merge(
        patientVisitFrequencyDF.alias("source"),
        "target.Patient_ID = source.Patient_ID"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()

except Exception as e:
    print("Delta table does not exist. Creating a new table...")
    patientVisitFrequencyDF.write.format("delta").mode("overwrite").option("path",delta_table_path).saveAsTable("enterprise.patientVisitFrequency")
