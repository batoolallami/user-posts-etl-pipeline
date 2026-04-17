import pandas as pd
import logging

def merge_post_user(user_df:pd.DataFrame,post_df:pd.DataFrame)-> pd.DataFrame:
    merge_df=post_df.merge(user_df,on='user_id',how='left')
    logging.info(f"Merged dateframe: {(len(merge_df))} rows")
    return merge_df

def aggregate_df(merge_df:pd.DataFrame) ->pd.DataFrame:
    result_df=(
        merge_df.groupby(['user_id','name','city'], as_index=False)
        .agg(
            total_post=("post_id","count"),
            avg_title_length=("title_length","mean")
            )
    
    )
    logging.info(f"Aggregated summary: {len(result_df)} rows")

    return result_df
