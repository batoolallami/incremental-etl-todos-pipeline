import logging
import pandas as pd
from sqlalchemy import create_engine
from config import Db_url 

def get_engine():
    return create_engine(Db_url)

def load_raw(df:pd.DataFrame)->None:
    if df.empty:
        logging.info("no new raw rows to save")
        return
    engine=get_engine()
    df.to_sql(
        "raw_todos",engine,if_exists='append',index=False
    )
    logging.info ("new raw data saved")

def load_clean(result_df:pd.DataFrame):
    engine=get_engine()
    result_df.to_sql("clean_todos",engine,if_exists='replace',index=False)
    logging.info("clean data saved to postgresql")
