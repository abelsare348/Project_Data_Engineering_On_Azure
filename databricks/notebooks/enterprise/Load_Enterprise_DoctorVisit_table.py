# Databricks notebook source
# MAGIC %md
# MAGIC <center>This Notebook is used to create Patient_Treatment_History Table in Enterprise Level.</center>
# MAGIC
# MAGIC <table align="center" border="1"> 
# MAGIC <tr><th>Title</th><th>Description</th></tr>
# MAGIC <tr><td>Creator</td><td>Aniket Belsare</td></tr>
# MAGIC <tr><td>Reviewer</td><td>Ankit Negi</td></tr>
# MAGIC <tr><td>DataSets Used</td><td><ul><li>refine_doctor</li>
# MAGIC <li>refine_visit</li>
# MAGIC </ul></td></tr>
# MAGIC </table>
# MAGIC

# COMMAND ----------

doctorDF=spark.read.table("refine.doctor")

# COMMAND ----------

visitsDF=spark.read.table("refine.visit")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

doctorVisitJNRDF=doctorDF.alias("D").join(visitsDF.alias("V"),doctorDF.Doctor_ID==visitsDF.Doctor_ID,"inner").select(F.col("D.Doctor_ID"),
        F.col("D.Name").alias("Doctor_Name"),
        F.col("V.Visit_ID"),
        F.col("V.Total_Cost"),
        F.col("V.Patient_ID"),
        F.col("V.Discharge_Date"),
        F.col("V.Admission_Date"))

# COMMAND ----------

doctorVisitSummaryDF=doctorVisitJNRDF.groupBy("Doctor_ID", "Doctor_Name") \
    .agg(
        F.count("Visit_ID").alias("Total_Visits_Attended"),  
        F.sum("Total_Cost").alias("Total_Revenue_Generated"),  
        F.countDistinct("Patient_ID").alias("Unique_Patients"),  
        F.avg(F.datediff("Discharge_Date", "Admission_Date")).alias("Avg_Length_of_Stay_Per_Patient")  
    )

# COMMAND ----------

delta_table_path = "abfss://output@freefinalstorageaccount.dfs.core.windows.net/enterprise/doctorVisits/table/"

try:
    # Try to access the Delta table
    doctorVisitSummaryEnterpriseDF= DeltaTable.forPath(spark, delta_table_path)

    # If the table exists, perform the merge operation
    doctorVisitSummaryEnterpriseDF.alias("target").merge(
        doctorVisitSummaryDF.alias("source"),
        "target.Doctor_ID = source.Doctor_ID",
        "target.Name = source.Name"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()

except Exception as e:
    print("Delta table does not exist. Creating a new table...")
    doctorVisitSummaryDF.write.format("delta").mode("overwrite").option("path",delta_table_path).saveAsTable("enterprise.doctorVisits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from enterprise.doctorVisits
