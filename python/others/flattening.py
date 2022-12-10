
tableNames =pyspark.read.format("bigquery") \
   .option("credentials", dbutils.secrets.get(scope = "databricks-kv-adls", key = KEY_ID)) \
   .option("parentProject", PROJECT_ID) \
   .option("dataset", DATASET_NAME) \
   .option("viewsEnabled", "true") \
   .option("materializationDataset", DATASET_NAME) \
  .option("query", """SELECT table_id,TIMESTAMP_MILLIS(creation_time) as created_time,TIMESTAMP_MILLIS(last_modified_time) as modified_time FROM `onepulse-3329a.analytics_222501239.__TABLES__` where  table_id like "%event_%" order by TIMESTAMP_MILLIS(last_modified_time) desc""") \
   .load()
tableNames.createOrReplaceTempView("tableNames")

import json
from pyspark.sql.types import *
from pyspark.sql.functions import *
from collections import Counter


def list_to_map(s):
  try:
    output={}
    for i in s:
      output[i["key"]]=[i for i in i["value"] if i is not None][0]
    
    return json.dumps(output)
  except Exception as e:
    return str({"error":e})

pyspark.udf.register("list_to_map",list_to_map,StringType())

def fetch_duplicate_columns(nested_field,column_list):
  """
    fetch duplicate columns and generate new value for the duplicate columns
    Arguments:
        nested_field: column name
        column_list: list of nested field names
        sample input: {'eventParams': ['endTime', 'content_id',  'timeStamp', 'timeStamp'], 'userProperties': [ 'country', 'language', 'gender', 'Country', 'Gender', 'ga_session_number', 'user_id', 'Language']}
    Returns:
        1.Dictionary with duplicate column name as key and new column name as value : 
                sample output: {'eventParams': {'timeStamp': 'timeStamp_1'}, 'userProperties': {'Country': 'Country_1', 'Gender': 'Gender_1', 'Language': 'Language_1'}}
        2.This is list of column name after replaced the duplicate column with new values 
                sample output: {'eventParams': ['content_id', 'endtime', 'timestamp_1', 'timestamp'], 'userProperties': ['country_1', 'gender_1', 'language_1', 'country', 'ga_session_number', 'gender', 'language', 'user_id']}
        
        
    """ 
  column_list.sort()
  col_counter=Counter([i.lower() for i in column_list])
  duplicate_cols=[k for k,v in col_counter.items() if v>1]
  final_columns=[]
  duplicate_column_for_transformation={}
  for i in column_list:
    lower_i=i.lower()
    if lower_i in duplicate_cols:
      temp_val=col_counter[lower_i]-1
      col_counter[lower_i]=temp_val
      if temp_val>0:
        final_columns.append(i+"_"+str(temp_val))
        temp_o=i
        temp_n=i+"_"+str(temp_val)
        duplicate_column_for_transformation[temp_o]=temp_n
      else:
        final_columns.append(i)
    else:
      final_columns.append(i)
  return (duplicate_column_for_transformation,[i.lower() for i in final_columns])


#   @udf
def handle_duplicate_columns(s,mapping_col):
  """
    handle duplicate columns
    Arguments:
        nested_field: column value from dataframe
        column_list: column name whose value is passed
    Returns:
       json string in which column value is modified by replacing old keys with new keys 
    """ 
  sd=json.loads(s)
  for k,v in duplicate_column_mapping[mapping_col].items():
    try:
      sd[v]=sd.pop(k)
    except Exception as e:
      pass
  return json.dumps(sd)


def generate_dynamic_query(mapping,dic={}):
  """
    Dynamically generate sql query with takes care of renaming and pivot as of nested fields 
    Arguments:
        nested_field: dictionary which is returned as second output of fetch_duplicate_columns needs to be passed -- second output of fetch_duplicate_columns has to be passed 
            sample input= {'eventParams': ['content_id', 'endtime', 'timestamp_1', 'timestamp'], 'userProperties': ['country_1', 'gender_1', 'language_1', 'country', 'ga_session_number', 'gender', 'language', 'user_id']}
        dic: optional parameter 
            sample input: {'userProperties_country':'up_country','userProperties_language':'up_lang','userProperties_gender':'up_gender'}
    Returns:
        Sql query 
    """ 
  
  try:
    query="select *,"
    for k,each_mapping in mapping.items():
      for key in each_mapping:
        lower_key=key.lower()
        if (k+"_"+key in dic.keys()):
          query+=f""" \ncase when {k}:`{key}` is not null then {k}:`{key}`  else 'NA'  end as {dic[k+"_"+key]},"""
        else:
          query+=f""" \ncase when {k}:`{key}` is not null then {k}:`{key}`  else 'NA'  end as `{k}_{lower_key}`,"""

    query=query[:-1]+ " from deduplicated_df"
    return query
  
  except Exception as e:
    raise("Invalid mapping provided",e)


#----------- calling

final_mapping={}
duplicate_column_mapping={}
for k,v in mapping.items():
  duplicate_column_mapping[k],final_mapping[k]=fetch_duplicate_columns(k,v)
  
print(duplicate_column_mapping)
print(final_mapping)

deduplicated_df=df.select("*",
                                         handle_duplicate_columns(col("eventParams_flattened"),lit("eventParams")).alias("eventParams"),                                                         handle_duplicate_columns(col("userProperties_flattened"),lit("userProperties")).alias("userProperties")).drop("userProperties_flattened","eventParams_flattened")
deduplicated_df.createOrReplaceTempView("deduplicated_df")

query_final=generate_dynamic_query(final_mapping)
df_structured_flattened=pyspark.sql(query_final)
