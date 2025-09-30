# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

notebookutils.variableLibrary.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

varLib = notebookutils.variableLibrary.getLibrary("VariablesLibrary")
print(varLib.WorkspaceID)
print(varLib.LakehouseID)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path = f"abfss://{varLib.WorkspaceID}@onelake.dfs.fabric.microsoft.com/{varLib.LakehouseID}/Files/Landing/Customers"
df_customers = spark.read.option("header", True).csv(path)
display(df_customers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
