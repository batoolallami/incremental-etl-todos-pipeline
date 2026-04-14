import logging
import requests
import pandas as pd
from config import todos_api_url

def extract_todos() ->pd.DataFrame:
  logging.info("Extracting todos from API")
  response = requests.get(todos_api_url)
  response.raise_for_status()
  todos_json=response.json()
  todos_df=pd.DataFrame(todos_json)
  todos_df=todos_df[['id','userId','completed']]
  logging.info(f"Extracted {len(todos_df)} todos")
  return todos_df
