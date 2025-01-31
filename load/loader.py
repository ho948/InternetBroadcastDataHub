import pandas as pd
import psycopg2
import glob
import os
import re
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
    
    def parse_number(self, value):
        """K, M 등의 단위가 있는 문자열을 숫자로 변환"""
        value = re.sub(r"[^\dKM.]", "", value)
        if "K" in value:
            return int(float(value.replace("K", "")) * 1_000)
        elif "M" in value:
            return int(float(value.replace("M", "")) * 1_000_000)
        return int(value.replace(",", ""))

    def parse_date(self, date_str, execution_ts):
        """업로드일 문자열을 execution_ts 기준으로 변환"""
        execution_date = datetime.strptime(execution_ts, "%Y-%m-%d %H:%M:%S")
        if "day" in date_str:
            days = int(re.search(r'(\d+) day', date_str).group(1))
            return execution_date - timedelta(days=days)
        elif "hour" in date_str:
            hours = int(re.search(r'(\d+) hour', date_str).group(1))
            return execution_date - timedelta(hours=hours)
        return execution_date

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

    def load_youtube_trending_game_videos(self):
        # youtube_trending_game_videos_cols = [
        #     "link",	"title", "views_count",
        #     "uploaded_at", "thumbsup_count", "thumbnail_img",
        #     "video_text", "channel_link", "channel_name",
        #     "subscribers_count", "channel_img", "execution_ts"
        # ]
        youtube_trending_game_videos_cols = [
            "link",	"title", "views_count",
            "uploaded_at", "thumbnail_img",
            "video_text", "channel_link", "channel_name",
            "subscribers_count", "channel_img", "execution_ts"
        ]
        youtube_trending_game_videos_rows = []

        start_date = datetime(2024, 11, 6)
        end_date = datetime(2024, 12, 6)
        dir_name = 'youtube_trending_game'
        file_name = 'trending_game_video_infos'
        all_rows = self.get_csv_data(start_date=start_date, end_date=end_date, dir_name=dir_name, file_name=file_name)
        
        for row in all_rows:
            link = row[0]
            title = row[1]
            views_count = row[2]
            uploaded_at = row[3]
            # thumbsup_count = row[4]
            thumbnail_img = row[5]
            video_text = row[6]
            channel_link = row[7]
            channel_name = row[8]
            subscribers_count = row[9]
            channel_img = row[10]
            execution_ts = row[11]

            # 크롤링이 제대로 안 된 데이터 제외
            if not pd.isna(execution_ts):
                # 데이터 변환
                views_count = self.parse_number(value=views_count)
                uploaded_at = self.parse_date(date_str=uploaded_at, execution_ts=execution_ts)
                # thumbsup_count = self.parse_number(value=thumbsup_count)
                subscribers_count = self.parse_number(value=subscribers_count)

                # youtube_trending_game_videos_rows.append((link, title, views_count, uploaded_at, thumbsup_count, thumbnail_img, video_text, channel_link, channel_name, subscribers_count, channel_img, execution_ts))
                youtube_trending_game_videos_rows.append((link, title, views_count, uploaded_at, thumbnail_img, video_text, channel_link, channel_name, subscribers_count, channel_img, execution_ts))
        
        table_name = 'youtube_trending_game_videos'
        self.load_data_to_db(cols=youtube_trending_game_videos_cols, rows=youtube_trending_game_videos_rows, table_name=table_name)

    def load_csv_files_to_db(self):
        # self.load_chzzk_popular_lives()
        # print("'load_chzzk_popular_lives' 완료")

        # self.load_chzzk_popular_channels()
        # print("'load_chzzk_popular_channels' 완료")

        # self.load_soop_popular_lives()
        # print("'load_soop_popular_lives' 완료")

        self.load_youtube_trending_game_videos()
        print("'load_youtube_trending_game_videos' 완료")

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
