# Databricks notebook source
s_appoint=spark.read.format("delta").load("abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Silver/appointments")

s_patient=spark.read.format("delta").load("abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Silver/patient")

s_doctor=spark.read.format("delta").load("abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Silver/doctor")

s_treat=spark.read.format("delta").load("abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Silver/treat")


s_appoint.show()
s_patient.show()
s_doctor.show()
s_treat.show()


# COMMAND ----------

g_department_load= s_appoint.groupBy("department")\
    .agg(
        {"patientid":"count","waittime":"avg"}
    )\
    .withColumnRenamed("count(patientid)","Dailypatients")\
    .withColumnRenamed("avg(waittime)","avgwaittime")

display(g_department_load)

# COMMAND ----------

g_doctor_effiency= s_appoint.groupBy("Doctorid")\
    .agg(
        {"patientid":"count","duration":"avg"}
    )\
    .withColumnRenamed("count(patientid)","Dailypatients")\
    .withColumnRenamed("avg(duration)","avg_duration")
    
display(g_doctor_effiency)


# COMMAND ----------

g_treatment_summary=s_treat.groupBy("department","treatmenttype").count()

display(g_treatment_summary)

# COMMAND ----------

g_treatment_summary.write.format("delta").mode("overwrite").option("path","abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Gold/treatment_summary")\
.saveAsTable("hospital.gold.treatment_summary")

g_department_load.write.format("delta").mode("overwrite").option("path","abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Gold/department_load")\
.saveAsTable("hospital.gold.department_load")

g_doctor_effiency.write.format("delta").mode("overwrite").option("path","abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Gold/doctor_effiency")\
.saveAsTable("hospital.gold.doctor_effiency")