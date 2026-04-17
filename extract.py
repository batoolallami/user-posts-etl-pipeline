import logging
import requests
import pandas as pd
from config import users_api_url, posts_api_url

def extract_user() ->pd.DataFrame:
    logging.info("Extracted users")
    response=requests.get(users_api_url)
    response.raise_for_status()
    user_json=response.json()
    users_df=pd.DataFrame(user_json)
    logging.info(f"Extracted {len(users_df)} users")
    return users_df

def extract_post() ->pd.DataFrame:
    logging.info("Extracted posts")
    response=requests.get(posts_api_url)
    response.raise_for_status()
    post_json=response.json()
    post_df=pd.DataFrame(post_json)
    logging.info(f"Extracted {len(post_df)} posts")
    return post_df
