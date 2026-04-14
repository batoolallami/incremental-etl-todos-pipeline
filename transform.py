import logging
import pandas as pd

def transform_todos(todos_df:pd.DataFrame):
  df=todos_df.copy()
  df=df.drop_duplicates(subset=['id'])
  df=df.rename(columns={'userId':'user_id'})
  logging.info(f"transformed todos {len(df)} rows")
  return df
