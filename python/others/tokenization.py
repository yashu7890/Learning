from typing import Dict, Any, List
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import json
import random
import string

testEmails = ["accoliteindia.com","yopmail.com","accolitedigital.com", "test_user@TestEmail.com"]
fieldstoTokenise = ["addressdetails","email","phone","national_id","nationality","placeofbirth","countryofbirth","lanamedobid","firstname","middlename","surname","lifestyle","maritalstatus","occupation","termsconditions","beneficiaries","disability","doctor","hospital","customername","newcontactvalue","relationshiptype","eventprincipalvalue"]

def tokenisev2(s: str) -> str:
  if any(item in s for item in testEmails):
    return "test_user@TestEmail.com"
  else:
    randomstr = ''.join(random.choice(string.ascii_lowercase) for i in range(5))
    randomstr2 = ''.join(random.choice(string.ascii_lowercase) for i in range(5))
    return randomstr + 'MaskedValue' + randomstr2

def field_walker(field: any, tokenise_fields: list, tokenise_immediately: bool = False):
  if tokenise_immediately:
    if isinstance(field, dict):
      return {key: field_walker(value, tokenise_fields, True) for (key, value) in field.items()}
    elif isinstance(field, list):
      return [field_walker(item, tokenise_fields, True) for item in field]
    elif isinstance(field, str):
      return tokenise_while(field)
    else:
      return str(tokenise_while(field))
  else:
    if isinstance(field, dict):
      return {key: (field_walker(value, tokenise_fields, True) if any([field in key.lower() for field in tokenise_fields])
                    else field_walker(value, tokenise_fields)) 
              for (key, value) in field.items()}
    elif isinstance(field, list):
      return [field_walker(item, tokenise_fields) for item in field]
    else:
      return field
  
def tokenise_fieldsv2(json_string: str) -> str:
  as_json = json.loads(json_string)
  as_json_tokenized = field_walker(as_json, fieldstoTokenise)
  return json.dumps(as_json_tokenized)

spark.udf.register("json_tokeniser_functionv2", tokenise_fieldsv2)