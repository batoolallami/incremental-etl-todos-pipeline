import requests
import pandas as pd
import logging
from sqlalchemy import create_engine
from datetime import datetime
"""
this program read the last id from pipeline_runs and compare them with api id because method get_existing_ids it correct not wrong
but the problem with this method if the table becomes huge reading all IDs every run becomes expensive
so we use the metadata table to remember last_max_id  then compare it 
only keep rows where id> last_max_id 
this is much faster
previous
API data -> compare against every id in raw_todos table

current
API data -> compare only against one value:last_max_id

"""
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
Db_url="postgresql://postgres:YOUR_PASSWORD@localhost:5432/YOUR_DATABASE"

def get_engine():
    return create_engine(Db_url)

def extract_todos() ->pd.DataFrame:
    logging.info("Extracting todos from API")
    response = requests.get('https://jsonplaceholder.typicode.com/todos')
    response.raise_for_status()
    todos_json=response.json()
    todos_df=pd.DataFrame(todos_json)
    todos_df=todos_df[['id','userId','completed']]
    logging.info(f"Extracted {len(todos_df)} todos")
    return todos_df

def get_last_max_id():
    engine=get_engine()
    try:
        #check if raw table exists and has data. if raw table is empty:
        #start from scratch (load everything)
        #else:
        #use last_max_id
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



def load_raw(df:pd.DataFrame)->None:
    if df.empty:
        logging.info("no new raw rows to save")
        return
    engine=get_engine()
    df.to_sql(
        "raw_todos",engine,if_exists='append',index=False
    )
    logging.info ("new raw data saved")

def transform_todo(todos_df : pd.DataFrame) ->pd.DataFrame:
    df=todos_df.copy()
    df=df.drop_duplicates(subset=['id'])
    df=df.rename(columns={'userId':'user_id'})
    logging.info(f"transformed todos {len(df)} rows")
    return df

def validate_output(result_df:pd.DataFrame) ->None:
    if result_df.empty:
        raise ValueError("pipeline output is empty")
    if result_df['id'].isna().any():
        raise ValueError('Missing todos id in output')
    if not result_df['id'].is_unique:
        raise ValueError('duplicate todos id in output')
    logging.info("validation passed")

def load_clean(result_df:pd.DataFrame):
    engine=get_engine()
    result_df.to_sql("clean_todos",engine,if_exists='replace',index=False)
    logging.info("clean data saved to postgresql")

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

def run_pipeline()->pd.DataFrame:
    pipeline_name="incremental_todos_pipeline"
    logging.info("starting incremental todos pipeline..")
    try:
        api_df=extract_todos()
        last_max_id=get_last_max_id()
        logging.info(f"last max id = {last_max_id}")

        new_raw_df=api_df[api_df['id'] > last_max_id].copy()
        logging.info(f"new rows to load = {len(new_raw_df)}")

        load_raw(new_raw_df)
        clean_df=transform_todo(api_df)
        validate_output(clean_df)
        last_max_id=int(api_df['id'].max()) if not api_df.empty else None
        log_pipeline_run(
            pipeline_name=pipeline_name,
            rows_loaded=len(new_raw_df),
            status='success',
            last_max_id=last_max_id
        )
        logging.info("pipeline finished successfully")
        return clean_df
    except Exception as e:
        logging.error(f"pipeline failed: {e}")
        log_pipeline_run(
            pipeline_name=pipeline_name,
            rows_loaded=0,
            status='failed',
            last_max_id=None)
        raise

if __name__=="__main__":
    final_df=run_pipeline()
    print("\nfinal result:")
    print(final_df.head())
        
