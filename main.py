import logging
from extract import extract_users, extract_posts
from transform import transform_users, transform_posts
from merge_and_aggregate import merge_post_user,aggregate_df

from load import (
load_raw_users,
load_raw_posts,
read_raw_users,
read_raw_posts,
load_clean )

from validate import validate_output
from metadat import (
get_last_max_user_id,
get_last_max_post_id,
log_pipeline_run
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s "
)
def run_pipeline()->pd.DataFrame:
    logging.info("starting user_post summary pipeline..")
    try:
        # step 1 : extract from API
        user_df=extract_user()
        post_df=extract_post()

        # step 2: Get last processed IDs
        last_max_user_id=get_last_max_user_id()
        last_max_post_id=get_last_max_post_id()
        logging.info(f"last max user id = {last_max_user_id}")
        logging.info(f"last max post id = {last_max_post_id}")

        # step 3: Filter only new raw rows
        new_raw_user_df=user_df[user_df['id'] > last_max_user_id].copy()
        new_raw_post_df=post_df[post_df['id'] > last_max_post_id].copy()

        logging.info(f"new rows to load = {len(new_raw_user_df)}")
        logging.info(f"new rows to load = {len(new_raw_post_df)}")

        # step 4: LOad raw incrementally
        load_raw_users(new_raw_user_df)
        load_raw_posts(new_raw_post_df)

        # step 5: Read all raw data from database
        raw_users_df=read_raw_users()
        raw_posts_df=read_raw_posts()

        # step 6: Transform raw data
        transform_user=transform_users(raw_users_df)
        transform_post=transform_posts(raw_posts_df)

        # step 7: Merge + Aggregate 
        merge_df=merge_post_user(transform_user,transform_post)
        result_df=aggregate_df(merge_df)

        # step 8: Validate + load clean summary
        validate_output(result_df)
        load_clean(result_df)

        # step 9: Log metadata
        current_max_user_id=int(user_df['id'].max()) if not user_df.empty else None
        current_max_post_id=int(post_df['id'].max()) if not post_df.empty else None
        log_pipeline_run(
            pipeline_name=pipeline_name,
            rows_user_loaded=len(new_raw_user_df),
            rows_post_loaded=len(new_raw_post_df),
            status='success',
            last_max_user_id=current_max_user_id,
            last_max_post_id=current_max_post_id)
        
        logging.info("pipeline finished successfully")
        return result_df
except Exception as e:
        logging.error(f"pipeline failed: {e}")
        log_pipeline_run(
            pipeline_name=pipeline_name,
            rows_user_loaded=0,
            rows_post_loaded=0,
            status='failed',
            last_max_user_id=None,
            last_max_post_id=None)
        raise

if __name__=="__main__":
    final_df=run_pipeline()
    print("\nfinal result:")
    print(final_df.head())
