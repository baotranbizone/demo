# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bd483095-e4b2-4b15-b2cc-455118a55622",
# META       "default_lakehouse_name": "testing_lakehouse",
# META       "default_lakehouse_workspace_id": "848043c6-3903-45b0-82de-3dcb0f24cc09",
# META       "known_lakehouses": [
# META         {
# META           "id": "bd483095-e4b2-4b15-b2cc-455118a55622"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "a96ab536-d565-4aca-a778-ca8361e58d39",
# META       "workspaceId": "2e6f0c95-ff58-4732-8c60-ed4006c3b317"
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import env.util as util

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vl = notebookutils.variableLibrary.getLibrary('TestVariables')
workspace_id = vl.getVariable('WorkspaceID')
lakehouse_id = vl.getVariable('SourceLakehouse')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

input_config = {
    "SAPB1": [ 
        {
            "bronze": {
                "table": "OIGE",
              # "read_method":"latest_by_inserted_delta", 
                "composite_key": ["Numerator"]
            }
        }
    ]    
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

loading_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/bronze/{input_config['SAPB1'][0]['bronze']['table']}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("delta").load(loading_path)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_in = util.latest_by_inserted_ts(df, input_config['SAPB1'][0]['bronze']["composite_key"])
df_in.persist()
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

output_config = {
    "SAPB1" : {
        #   "write_method":"SPARK_TRANSFORM_TO_SILVER",   # "SPARK_LOAD_TO_DIM", "SPARK_LOAD_TO_FACT"
        #   "write_mode": "INSERT_ONLY",
        #   "merge_using_key_hash": True,
        "table":"OIGE",
        #   "format": "delta",
        "composite_key": ["Numerator"]
    }
}

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
