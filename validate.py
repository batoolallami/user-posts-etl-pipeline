import logging
import pandas as pd

def validate_output(result_df:pd.DataFrame):
    if result_df.empty:
        raise ValueError("pipeline output is empty")
    if result_df['user_id'].isna().any():
        raise ValueError("missing user_id in output")
    if not result_df['user_id'].is_unique:
        raise ValueError("duplicate user_id in output")
    if result_df['total_post'].isna().any():
        raise ValueError("missing total_post in output")
    logging.info("validation passed")
