import os
import yaml
import json
import pandas as pd
import pandasql as ps
import time
env = os.environ['ENV']
os.environ['ENVIRONMENT'] = 'production'
from tardis import client


def ClientRes(input_src_list,sql_query):
  n=0
  print(env)
  print(f"Connected to Tardis at URL '{client.API_URL}'")
  while n < 10:
    response = client.Get(get_type='DataAvailability',sourceName=input_src_list)
    if response.status_code == 200:
      res=response.read()
      dataAvail_json_pd_DF=res['data']['dataAvailability']['results']
      dataAvail_normalize_pd_DF=pd.json_normalize(dataAvail_json_pd_DF)
      dataAvail_normalize_pd_DF.rename(columns={'source.source':'source','status.status':'status'},inplace=True)
      Final=pd.DataFrame(dataAvail_normalize_pd_DF)
      result=ps.sqldf(sql_query)
      check = result.any()
      final_one = check.final_count
      print('Final bool status -->', final_one)
      n = 11
    else:
      if n == 10:
        raise RuntimeError("Tardis Data Log Failed. Reason: No reply from Tardis Server")
      n = n + 1
      time.sleep(60)
      continue
    return final_one

def Criteria(input_src_list,sql_query):
  var_time=0
  print('Input source list -->' , input_src_list)
  print('Input sql query -->', sql_query)
  while var_time<3600 :
    Criteria=ClientRes(input_src_list,sql_query)
    if Criteria==True:
      print('Success criteria met hence dag suceeded')
      var_time=3601
    else:
      if Criteria==False and var_time < 3600:
        print('Time Before' ,var_time)
        time.sleep(300)
        var_time+=300
        print('Time current' , var_time)
        if var_time < 3600:
          continue
        else:
          raise RuntimeError("Failure criteria met hence dag Failed")