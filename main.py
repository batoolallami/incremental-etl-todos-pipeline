import logging
from extract import extract_todos
from transform import transform_todo
from validate import validate_output
from load import load_raw, load_clean

logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s - %(levelname)s - %(message)s"
)

def run_pipeline():
  pipeline_name="incremental_todos_pipeline"
  logging.info("Starting incremental todos pipeline...")

try:
  api_df=extract_todos()
  last_max_id = get_last_max_id()
  logging.info(f"last max id = {last_max_id}")
  new_raw_df = api_df[api_df['id'] > last_max_id].copy()
  logging.info(f"new rows to load = {len(new_raw_df)}")
  load_raw(new_raw_df)

  clean_df =transform_todo(api_df)
  validate_output(clean_df)
  load_clean(clean_df)

  current_max_id = int(api_df['id'].max()) if not api_df.empty else None
  log_pipeline_run (
    pipeline_name=pipeline_name
    rows_loaded=len(new_raw_df)
    status="success"
    last_max_id=current_max_id
    )
    logging.info("pipeline finished successfully")
    return clean_df
except Exception as e:
  logging.error(f"pipeline failed: {e}")
  log_pipeline_run(
    pipeline_name:pipeline_name,
    rows_loaded=0,
    status='failed'
    last_max_id =None
  )
  raise

if __name__=="__main__"
    final_df = run_pipeline()
    print("\nfinal result:")
    print(final_df.head())

  
