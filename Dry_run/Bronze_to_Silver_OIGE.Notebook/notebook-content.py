# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ed317608-524d-40ca-a4cc-1e0cc9eed98e",
# META       "default_lakehouse_name": "develop_lakehouse",
# META       "default_lakehouse_workspace_id": "2e6f0c95-ff58-4732-8c60-ed4006c3b317",
# META       "known_lakehouses": [
# META         {
# META           "id": "ed317608-524d-40ca-a4cc-1e0cc9eed98e"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "61e58d39-ca83-a778-4aca-d565a96ab536",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import env.util as util

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tabl

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = util.read_table(spark, notebookutils, "TestVariables", "bronze", "OIGE")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_in = util.latest_by_inserted_ts(df, "Numerator")

view_name = f'vw_{input_config["SAPB1"][0]["bronze"]["table"]}'

df_in.createOrReplaceTempView(view_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_out = spark.sql(f"""
    SELECT * 
    FROM {view_name}
""")
display(df_out)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

util.write_insert_only(df_out, "develop_lakehouse", "silver", output_config['SAPB1']['table'], output_config["SAPB1"]["composite_key"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

util.write_insert_only(spark, df_out, "develop_lakehouse", "silver", output_config['SAPB1']['table'], output_config["SAPB1"]["composite_key"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM develop_lakehouse.silver.oige LIMIT 100")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM develop_lakehouse.silver.oige WHERE Numerator = 98640 LIMIT 100")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
