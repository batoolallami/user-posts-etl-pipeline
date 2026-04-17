import logging
import pandas as pd
from datetime import datetime
from config import pipeline_name
from load import get_engine

def get_last_max_user_id():
    engine=get_engine()
    try:
        raw_count = pd.read_sql('select count(*) as cnt from raw_users',engine)
        if raw_count['cnt'].iloc[0]==0:
            logging.info("raw_users is empty -> reset last_max_user_id to 0")
            return 0

    except Exception:
        logging.info("raw_users table not found -> reset last_max_user)id to 0")
        return 0

    try:
        existing_df=pd.read_sql(""" 
                                select last_max_user_id from pipeline_runs 
                                where pipeline_name='user_post_summary_pipeline' and status='success' 
                                order by run_time desc limit 1 
                                """,engine)
        if existing_df.empty:
            return 0
        return int(existing_df["last_max_user_id"].iloc[0])
        
    except Exception:
        logging.info("raw users table not found yet, starting fresh")
        return 0
def get_last_max_post_id():
    engine=get_engine()
    try:
        raw_count = pd.read_sql('select count(*) as cnt from raw_posts',engine)
        if raw_count['cnt'].iloc[0]==0:
            logging.info("raw_post is empty -> reset last_max_post_id to 0")
            return 0

    except Exception:
        logging.info("raw_post table not found -> reset last_max_post_id to 0")
        return 0

    try:
        existing_df=pd.read_sql(""" 
                                    select last_max_post_id from pipeline_runs 
                                    where pipeline_name='user_post_summary_pipeline' and status='success' 
                                    order by run_time desc limit 1 
                                    """,engine)
        if existing_df.empty:
            return 0
        return int(existing_df["last_max_post_id"].iloc[0])
            
    except Exception:
        logging.info("raw post table not found yet, starting fresh")
        return 0

def log_pipeline_run(pipeline_name:str,
                     rows_user_loaded:int,
                     rows_post_loaded:int,status:str,
                     last_max_user_id:int|None,
                     last_max_post_id:int|None):
    engine=get_engine()
    log_df=pd.DataFrame([{"pipeline_name":pipeline_name,
                          "run_time":datetime.now(),
                          "rows_user_loaded":rows_user_loaded,
                          "rows_post_loaded":rows_post_loaded,
                          "last_max_user_id":last_max_user_id,
                          "last_max_post_id":last_max_post_id,
                          "status":status}])
    log_df.to_sql(
        "pipeline_runs",engine,if_exists='append',index=False
    )
    logging.info("pipeline run logged")
