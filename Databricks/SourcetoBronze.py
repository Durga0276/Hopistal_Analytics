# Databricks notebook source
df_appointment=spark.read.format("csv").option("header",True).load("abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Source/appointments.csv")
df_departments=spark.read.format("csv").option("header",True).load("abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Source/departments.csv")
df_doctor=spark.read.format("csv").option("header",True).load("abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Source/doctors.csv")
df_patient=spark.read.format("csv").option("header",True).load("abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Source/patients.csv")
df_treatments=spark.read.format("csv").option("header",True).load("abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Source/treatments.csv")

# COMMAND ----------

display(df_appointment)
display(df_departments)
display(df_doctor)
display(df_patient)
display(df_treatments)

# COMMAND ----------

df_appointment.write.format("delta").mode("overwrite").option("path","abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Bronze/appointments")\
.saveAsTable("hospital.Bronze.appointments")

df_departments.write.format("delta").mode("overwrite").option("path","abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Bronze/departments")\
.saveAsTable("hospital.Bronze.departments")

df_patient.write.format("delta").mode("overwrite").option("path","abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Bronze/patient")\
.saveAsTable("hospital.Bronze.patient")

df_doctor.write.format("delta").mode("overwrite").option("path","abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Bronze/doctors")\
.saveAsTable("hospital.Bronze.doctors")

df_treatments.write.format("delta").mode("overwrite").option("path","abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Bronze/treatments")\
.saveAsTable("hospital.Bronze.treatments")