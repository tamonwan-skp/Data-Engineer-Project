# Databricks notebook source
v_result = dbutils.notebook.run("1.circuits_ingestion", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("2.race_ingestion", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("3.constructors_ingestion", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("4.drivers_ingestion", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("5.results_ingestion", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("6.pit_stops_ingestion", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("7.lap_times_ingestion", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("8.qualifying_ingestion", 0, {"p_data_source": "Ergast API"})