# Databricks notebook source
# MAGIC %md
# MAGIC <center>This Notebook is used to create Patient_Treatment_History Table in Enterprise Level.</center>
# MAGIC
# MAGIC <table>
# MAGIC <tr><th>Title</th><th>Description</th></tr>
# MAGIC <tr><td>Creator</td><td>Aniket Belsare</td></tr>
# MAGIC <tr><td>Reviewer</td><td>Ankit Negi</td></tr>
# MAGIC <tr><td>DataSets Used</td><td><ul><li>refine_patient</li>
# MAGIC <li>refine_visit</li>
# MAGIC <li>refine_treatment</li></ul></td></tr>
# MAGIC </table>

# COMMAND ----------

patientDF=spark.read.table("refine.patient")

# COMMAND ----------

doctorDF=spark.read.table("refine.doctor")

# COMMAND ----------

visitsDF=spark.read.table("refine.visit")

# COMMAND ----------

treatmentDF=spark.read.table("refine.treatment")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

patientVisitJNRDF = patientDF.alias("P").join(visitsDF.alias("V"), patientDF.Patient_ID == visitsDF.Patient_ID, "inner").select(
    F.col("P.Patient_ID"),
    F.col("P.Name").alias("Patient_Name"),  # Corrected typo here
    F.col("V.Admission_Date"),
    F.col("V.Discharge_Date"),
    F.col("V.ICU_Stay"),
    F.col("V.Total_Cost"),
    F.col("V.Doctor_ID"),
    F.col("V.Treatment_ID")
)

patientVisitDoctorJNRDF = patientVisitJNRDF.join(doctorDF, patientVisitJNRDF.Doctor_ID == doctorDF.Doctor_ID, "inner").select(
    F.col("Patient_ID"),
    F.col("Patient_Name"),  # Corrected typo here
    doctorDF["Doctor_ID"],
    F.col("Name").alias("Doctor_Name"),
    F.col("Specialization"),
    F.col("Treatment_ID"),
    F.col("V.Admission_Date"),
    F.col("V.Discharge_Date"),
    F.col("V.ICU_Stay"),
    F.col("V.Total_Cost")
)

patientTreatmentHistoryDF = patientVisitDoctorJNRDF.join(treatmentDF, patientVisitDoctorJNRDF.Treatment_ID == treatmentDF.Treatment_ID, "inner").select(
    F.col("Patient_ID"),
    F.col("Patient_Name"),  # Ensure this matches the corrected alias
    F.col("Doctor_ID"),
    F.col("Doctor_Name"),  # This assumes Doctor_Name is correctly defined in the previous DataFrame
    F.col("Specialization"),
    treatmentDF["Treatment_ID"],
    F.col("Treatment_Type"),
    F.col("V.Admission_Date"),
    F.col("V.Discharge_Date"),
    F.col("V.ICU_Stay"),
    F.col("V.Total_Cost")
)

# COMMAND ----------

delta_table_path = "abfss://output@freefinalstorageaccount.dfs.core.windows.net/enterprise/patientTreatmentHistory/table/"

try:
    # Try to access the Delta table
    patientTreatmentHistoryEnterpriseDF= DeltaTable.forPath(spark, delta_table_path)

    # If the table exists, perform the merge operation
    patientTreatmentHistoryEnterpriseDF.alias("target").merge(
        patientTreatmentHistoryDF.alias("source"),
        "target.patient_id = source.patient_id",
        "target.Admission_Date = source.Admission_Date",
        "target.Discharge_Date = source.Discharge_Date"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()

except Exception as e:

    print("Delta table does not exist. Creating a new table...")
    
    patientTreatmentHistoryDF.write.format("delta").mode("overwrite").option("path",delta_table_path).saveAsTable("enterprise.patientTreatmentHistory")
