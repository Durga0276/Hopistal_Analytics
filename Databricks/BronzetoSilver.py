# Databricks notebook source
b_appoint=spark.read.format("delta").load("abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Bronze/appointments")

b_patient=spark.read.format("delta").load("abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Bronze/patient")

b_doctor=spark.read.format("delta").load("abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Bronze/doctors")

b_treat=spark.read.format("delta").load("abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Bronze/treatments")

b_depart=spark.read.format("delta").load("abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Bronze/departments")
b_appoint.show()
b_patient.show()
b_doctor.show()
b_treat.show()
b_depart.show()

# COMMAND ----------

from pyspark.sql.functions import to_date , col , date_diff , current_date , round , lower , unix_timestamp , select
s_patient=b_patient.dropDuplicates(['patientid'])\
    .withColumn('DOB',to_date(col("DOB")))\
    .withColumn("AGE",round(date_diff(current_date(), col("DOB"))/365))
display(s_patient)


s_doctor=b_doctor.na.drop(subset="doctorid")\
    .withColumn("deparment",lower(col("department")))
display(s_doctor)

# COMMAND ----------

s_appoint=b_appoint.na.drop(subset=['CheckInTime'])\
    .join(b_patient,"patientid","left")\
    .withColumn("CheckInTime",to_date("CheckInTime"))\
    .withColumn("CheckOutTime",to_date("CheckOutTime"))\
    .withColumn("appointmentdate",to_date("appointmentdate"))\
    .withColumn("Waittime",unix_timestamp("CheckInTime")-unix_timestamp("appointmentdate"))\
    .withColumn("Duration",unix_timestamp("CheckInTime")-unix_timestamp("CheckOutTime"))

display(s_appoint)

s_appoint.describe()

# COMMAND ----------

s_treat=b_treat.na.drop(subset=['TreatmentID','PatientID'])\
    .join(s_appoint.select("appointmentid","patientid","department"),"patientid","left")

s_treat.show()

# COMMAND ----------

s_appoint.write.format("delta").mode("overwrite").option("path","abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Silver/appointments")\
.saveAsTable("hospital.silver.appointments")

s_doctor.write.format("delta").mode("overwrite").option("path","abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Silver/doctor")\
.saveAsTable("hospital.silver.doctor")

s_patient.write.format("delta").mode("overwrite").option("path","abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Silver/patient")\
.saveAsTable("hospital.silver.patient")

s_treat.write.format("delta").mode("overwrite").option("path","abfss://ex-metastore@meadls.dfs.core.windows.net/root/Hopistal_Analystics/Silver/treat")\
.saveAsTable("hospital.silver.treat")