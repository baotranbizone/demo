# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import env.util as util

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
                "table": "IGE1",
              # "read_method":"latest_by_inserted_delta", 
                "composite_key": ["Document_Internal_ID"]
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

vl = notebookutils.variableLibrary.getLibrary('TestVariables')
workspace_id = vl.getVariable('SourceWorkspaceID')
lakehouse_id = vl.getVariable('SourceLakehouseID')

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
display(df_out)

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
        "table":"IGE1",
        #   "format": "delta",
        "composite_key": ["Document_Internal_ID"]
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

util.write_upsert(spark, df_out, "develop_lakehouse", "silver", output_config['SAPB1']['table'], output_config["SAPB1"]["composite_key"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
