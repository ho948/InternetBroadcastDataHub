import pandas as pd
import psycopg2
import glob
import os
from psycopg2 import sql
from datetime import datetime, timedelta

class Loader:
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**self.db_config)
        except Exception as e:
            print(f"Error 'connect_db': {e}")
            raise
    
    def close_connect(self):
        try:
            self.conn.close()
        except Exception as e:
            print(f"Error 'close_connect': {e}")
            raise

    def load_data_to_db(self, cols, rows, table_name):
        try:
            cursor = self.conn.cursor()
            insert_query = sql.SQL(
            "INSERT INTO raw.{table} ({fields}) VALUES ({values})"
            ).format(
                table=sql.Identifier(table_name),
                fields=sql.SQL(', ').join(map(sql.Identifier, cols)),
                values=sql.SQL(', ').join(map(sql.Placeholder, cols))
            )
            for row in rows:
                row_dict = dict(zip(cols, row))
                cursor.execute(insert_query, row_dict)
    
            self.conn.commit()
            cursor.close()
            print(f"Data successfully inserted into {table_name}.")

        except Exception as e:
            print(f"Error 'load_data_to_db': {e}")
        finally:
            if cursor:
                cursor.close()
    
    def get_csv_data(self, start_date, end_date, dir_name, file_name):
        all_rows = []

        try:
            current_date = start_date
            while current_date <= end_date:
                year = current_date.year
                month = current_date.month
                day = current_date.day

                file_pattern = os.path.join(os.getcwd(), '../airflow/dags/output/transaction_files', dir_name, f"{year}/{month}/{day}/*/{file_name}*.csv")
                csv_files = glob.glob(file_pattern)
                for file in csv_files:
                    df = pd.read_csv(file)
                    for _, row in df.iterrows():
                        all_rows.append(row.tolist())

                current_date += timedelta(days=1)
        except Exception as e:
            print(f"Error 'get_csv_data': {e}")
            raise

        return all_rows

    def load_chzzk_popular_lives(self):
        chzzk_popular_lives_cols = [
            "execution_ts", "live_id", "live_title", 
            "viewers_count", "channel_id", "category_type", 
            "category_id", "category_name", "is_adult", "open_ts"
        ]
        chzzk_popular_lives_rows = []
        
        start_date = datetime(2024, 11, 6)
        end_date = datetime(2024, 12, 6)
        dir_name = 'chzzk_popular_live'
        file_name = 'chzzk_popular_lives'
        all_rows = self.get_csv_data(start_date=start_date, end_date=end_date, dir_name=dir_name, file_name=file_name)
        
        for row in all_rows:
            execution_ts = row[0]
            live_id = row[1]
            live_title = row[2]
            viewers_count = row[3]
            channel_id = row[4]
            category_type = row[5]
            category_id = row[6]
            category_name = row[7]
            is_adult = row[8]
            open_ts = row[9]
            chzzk_popular_lives_rows.append((execution_ts, live_id, live_title, viewers_count, channel_id, category_type, category_id, category_name, is_adult, open_ts))
        
        table_name = 'chzzk_popular_lives'
        self.load_data_to_db(cols=chzzk_popular_lives_cols, rows=chzzk_popular_lives_rows, table_name=table_name)
    
    def load_chzzk_popular_channels(self):
        chzzk_popular_channels_cols = [
            "execution_ts", "channel_id", "channel_name", 
            "followers_count", "is_verified", "channel_type", 
            "channel_description"
        ]
        chzzk_popular_channels_rows = []

        start_date = datetime(2024, 11, 6)
        end_date = datetime(2024, 12, 6)
        dir_name = 'chzzk_popular_live'
        file_name = 'chzzk_popular_channels'
        all_rows = self.get_csv_data(start_date=start_date, end_date=end_date, dir_name=dir_name, file_name=file_name)
        
        for row in all_rows:
            execution_ts = row[0]
            channel_id = row[1]
            channel_name = row[2]
            followers_count = row[3]
            is_verified = row[4]
            channel_type = row[5]
            channel_description = row[6]
            chzzk_popular_channels_rows.append((execution_ts, channel_id, channel_name, followers_count, is_verified, channel_type, channel_description))
        
        table_name = 'chzzk_popular_channels'
        self.load_data_to_db(cols=chzzk_popular_channels_cols, rows=chzzk_popular_channels_rows, table_name=table_name)

    def load_soop_popular_lives(self):
        soop_popular_lives_cols = [
            "execution_ts", "live_id", "live_title", 
            "viewers_count", "channel_id", "channel_name", 
            "category", "is_adult"
        ]
        soop_popular_lives_rows = []

        start_date = datetime(2024, 11, 6)
        end_date = datetime(2024, 12, 6)
        dir_name = 'soop_popular_live'
        file_name = 'soop_popular_lives'
        all_rows = self.get_csv_data(start_date=start_date, end_date=end_date, dir_name=dir_name, file_name=file_name)
        
        for row in all_rows:
            execution_ts = row[0]
            live_id = row[1]
            live_title = row[2]
            viewers_count = row[3]
            channel_id = row[4]
            channel_name = row[5]
            category = row[6]
            is_adult = True if row[7] else False
            soop_popular_lives_rows.append((execution_ts, live_id, live_title, viewers_count, channel_id, channel_name, category, is_adult))
        
        table_name = 'soop_popular_lives'
        self.load_data_to_db(cols=soop_popular_lives_cols, rows=soop_popular_lives_rows, table_name=table_name)

    def load_csv_files_to_db(self):
        # self.load_chzzk_popular_lives()
        # print("'load_chzzk_popular_lives' 완료")

        # self.load_chzzk_popular_channels()
        # print("'load_chzzk_popular_channels' 완료")

        self.load_soop_popular_lives()
        print("'load_soop_popular_lives' 완료")

        youtube_game_video_videos_cols = [
            "link", "execution_ts", "rank", 
            "title", "views_count", "uploaded_at", 
            "thumbsup_count", "video_text", "channel_link"
        ]
        youtube_game_video_videos_rows = []

        youtube_trending_channels_cols = [
            "channel_link", "execution_ts", "channel_name", 
            "subscribers_count"
        ]
        youtube_trending_channels_rows = []

        youtube_game_video_latest_cols = [
            "link", "execution_ts", "rank", 
            "title", "views_count", "uploaded_at", 
            "thumbsup_count", "video_text", "channel_link"
        ]
        youtube_game_video_latest_rows = []

        """
        <파일명>
        1. 치지직: chzzk_popular_lives, chzzk_popular_channels
        2. 숲: soop_popular_lives
        3. 유튜브: trending_game_video_infos, trending_latest_video_infos
        
        <테이블명>
        1. 치지직: chzzk_popular_lives, chzzk_popular_channels
        2. 숲: soop_popular_lives
        3. 유튜브: youtube_trending_game_videos, youtube_trending_channels, youtube_trending_lateset_videos
        
        <파일별 칼럼 종류>
        1. chzzk_popular_lives:
            [execution_ts	live_id	live_title	viewers_count	channel_id	category_type	category_id	category_name	is_adult	open_ts]
        2. chzzk_popular_channels:
            [execution_ts	channel_id	channel_name	follower_count	verified_mark	channel_type	channel_description]
        3. soop_popular_lives:
            [execution_ts	live_id	live_title	viewers_count	channel_id	channel_name	category	is_adult]
        4. trending_game_video_infos & trending_latest_video_infos:
            [link	title	views_count	uploaded_date	thumbsup_count	thumbnail_img	video_text	channel_link	channel_name	subscribers_count	channel_img	execution_ts]
        """