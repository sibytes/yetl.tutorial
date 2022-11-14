# Databricks notebook source
# MAGIC %pip install regex pyaml

# COMMAND ----------

from src.demo_landing_to_raw import landing_to_raw
from yetl.flow import Timeslice, OverwriteSave
from yetl.workflow import multithreaded as yetl_wf
import yaml

timeslice = Timeslice("*", "*", "*")
project = "demo"
maxparallel = 4

path = f"./config/project/{project}/{project}_tables.yml"

with open(path, "r", encoding="utf-8") as f:
    metdata = yaml.safe_load(f)

tables: list = [t["table"] for t in metdata.get("tables")]

yetl_wf.load(project, tables, landing_to_raw, timeslice, OverwriteSave, maxparallel)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from demo_raw.customer_details
# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from demo_raw.customer_preferences

# COMMAND ----------

dbutils.notebook.exit("YETL!")

