import logging
import pandas as pd
from datetime import datetime
from config import pipeline_name
from load import get_engine

def get_last_max_id():
    engine=get_engine()
    try:
        raw_count = pd.read_sql('select count(*) as cnt from raw_todos',engine)
        if raw_count['cnt'].iloc[0]==0:
            logging.info("raw_todos is empty reset last max id to 0")
            return 0
    except Exception:
        logging.info("raw_todos table not found -> reset last max id to 0")
        return 0

    try:
        existing_df=pd.read_sql(""" 
                                select last_max_id from pipeline_runs 
                                where pipeline_name='incremental_todos_pipeline' and status='success' 
                                order by run_time desc limit 1 
                                """,engine)
        if existing_df.empty:
            return 0
        return int(existing_df["last_max_id"].iloc[0])  
    except Exception:
        logging.info("raw todos table not found yet, starting fresh")
        return 0


def log_pipeline_run(pipeline_name:str,rows_loaded:int,status:str,last_max_id:int|None):
    engine=get_engine()
    log_df=pd.DataFrame([{"pipeline_name":pipeline_name,
                          "run_time":datetime.now(),
                          "rows_loaded":rows_loaded,
                          "last_max_id":last_max_id,
                          "status":status}])
    log_df.to_sql(
        "pipeline_runs",engine,if_exists='append',index=False
    )
    logging.info("pipeline run logged")
