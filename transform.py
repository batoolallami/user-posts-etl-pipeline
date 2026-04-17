import logging
import pandas as pd
import json

def transform_users (df:pd.DataFrame) ->pd.DataFrame:
    clean_df=df.copy()
    clean_df['address']=clean_df['address'].apply(
        lambda x:json.loads(x) if isinstance(x,str) else x
    )
    clean_df['city']=clean_df['address'].apply(
        lambda x:x.get('city') if isinstance(x,dict) else None
    )
    clean_df=clean_df.rename(columns={"id":"user_id"})
    clean_df=clean_df[['user_id','name','email','city']]
    clean_df=clean_df.drop_duplicates(subset=['user_id'])
    logging.info(f"transformed users: {len(clean_df)} rows")
    return clean_df 

def transform_posts(df:pd.DataFrame) ->pd.DataFrame:
    clean_df=df.copy()
    clean_df=clean_df.rename(columns={'id':'post_id','userId':'user_id'})
    clean_df['title_length']=clean_df['title'].str.len()
    clean_df=clean_df[['post_id','user_id','title','title_length']]
    clean_df=clean_df.drop_duplicates(subset=['post_id'])
    logging.info(f"Transformed posts: {len(clean_df)} rows")
    return clean_df


