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

visitsDF=spark.read.table("refine.visit")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

ICU_Stay_AnalysisDF=visitsDF \
    .filter(visitsDF.ICU_Stay == 'Yes') \
    .groupBy("Patient_ID") \
    .agg(
        F.count("Visit_ID").alias("Total_ICU_Visits"),
        F.sum("Total_Cost").alias("Total_Cost_Incurred"),
        F.avg(F.datediff("Discharge_Date", "Admission_Date")).alias("Avg_Length_of_ICU_Stay")
    ) \
    .orderBy(F.desc("Total_Cost_Incurred"))


# COMMAND ----------

delta_table_path = "abfss://output@freefinalstorageaccount.dfs.core.windows.net/enterprise/ICUStayAnalysis/table/"

try:
    # Try to access the Delta table
    ICU_Stay_AnalysisEnterpriseDF= DeltaTable.forPath(spark, delta_table_path)

    # If the table exists, perform the merge operation
    ICU_Stay_AnalysisEnterpriseDF.alias("target").merge(
        ICU_Stay_AnalysisDF.alias("source"),
        "target.Patient_ID = source.Patient_ID"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()

except Exception as e:
    print("Delta table does not exist. Creating a new table...")
    ICU_Stay_AnalysisDF.write.format("delta").mode("overwrite").option("path",delta_table_path).saveAsTable("enterprise.ICUStayAnalysis")
