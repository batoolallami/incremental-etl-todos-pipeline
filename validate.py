import logging
import pandas as pd

def validate_output(result_df:pd.DataFrame) ->None:
    if result_df.empty:
        raise ValueError("pipeline output is empty")
    if result_df['id'].isna().any():
        raise ValueError('Missing todos id in output')
    if not result_df['id'].is_unique:
        raise ValueError('duplicate todos id in output')
    logging.info("validation passed")
