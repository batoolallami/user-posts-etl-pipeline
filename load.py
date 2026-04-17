import json
import logging
import pandas as pd
from sqlalchemy import create_engine
from config import Db_url

def get_engine():
    return create_engine(Db_url)

def load_raw_users(df:pd.DataFrame) ->None:
    if df.empty:
        logging.info("no new user rows to save")
        return
    
    engine=get_engine()
    raw_df=df.copy()
    raw_df['address'] = raw_df['address'].apply(json.dumps)
    raw_df['company']=raw_df['company'].apply(json.dumps)
    raw_df.to_sql("raw_users",engine,if_exists='append',index=False)
    logging.info(f"saved {len(raw_df)} new raw users")

def load_raw_posts(df:pd.DataFrame) ->None:
    if df.empty:
        logging.info("no new post rows to save")
        return
    engine=get_engine()
    
    df.to_sql("raw_posts",engine,if_exists='append',index=False)
    logging.info(f"saved {len(df)} new raw posts")

def read_raw_users() ->pd.DataFrame:
    engine=get_engine()
    df=pd.read_sql("select * from raw_users",engine)
    logging.info(f"read {len(df)} rows from raw_users")
    return df

 def read_raw_posts() ->pd.DataFrame:
    engine=get_engine()
    df=pd.read_sql("select * from raw_posts",engine)
    logging.info(f"read {len(df)} rows from raw_posts")
    return df
   
def load_clean(result_df:pd.DataFrame):
    engine=get_engine()
    result_df.to_sql("clean_user_post_summary",engine,if_exists='replace',index=False)
    logging.info("clean summary saved to postgresql")
