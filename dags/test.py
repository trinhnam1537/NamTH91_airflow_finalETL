#  E chạy airflow bằng docker mà khi đọc file từ local thì không đọc được ạ , nên e cp thư mục data vào thư mục airflow trong container nhưng mà vẫn ko đọc được data

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 
import os  
import glob 
import json 
import psycopg2 

# class SqlQueries:
#     songplay_table_insert = ("""
#         SELECT
#                 md5(events.sessionid || events.start_time) songplay_id,
#                 events.start_time, 
#                 events.userid, 
#                 events.level, 
#                 songs.song_id, 
#                 songs.artist_id, 
#                 events.sessionid, 
#                 events.location, 
#                 events.useragent
#                 FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
#             FROM staging_events
#             WHERE page='NextSong') events
#             LEFT JOIN staging_songs songs
#             ON events.song = songs.title
#                 AND events.artist = songs.artist_name
#                 AND events.length = songs.duration
#     """)

#     user_table_insert = ("""
#         SELECT distinct userid, firstname, lastname, gender, level
#         FROM staging_events
#         WHERE page='NextSong'
#     """)

#     song_table_insert = ("""
#         SELECT distinct song_id, title, artist_id, year, duration
#         FROM staging_songs
#     """)

#     artist_table_insert = ("""
#         SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
#         FROM staging_songs
#     """)

#     time_table_insert = ("""
#         SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
#                extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
#         FROM songplays
#     """)
default_args = {
    'owner':'NamTH91',
    'start_date': datetime(2025,7,20),
    'retries': 3 ,
    'retry_delay' : timedelta(minutes=5),
    'catchup': False,
    'email_on_retry':False
}
dag = DAG(
    'tunestream_etl_test',
    default_args= default_args,
    description = 'ETL pipeline ',
    schedule_interval= '@daily',
    catchup=False
)
# hàm load data từ thư mục đến table 
def load_song_data_to_table():
    all_files = glob.glob('data/song_data/**/*.json',recursive=True)
    print(f"tổng số file json tìm được là {len(all_files)}")
    for file_path in all_files :
        with open(file_path, 'r') as f:
            data = json.load(f)
            values = (
                data.get('num_songs'),
                data.get('artist_id'),
                data.get('artist_name'),
                data.get('artist_latitude'),
                data.get('artist_longtitude'),
                data.get('artist_location'),
                data.get('song_id'),
                data.get('title'),
                data.get('duration'),
                data.get('year')
            )
            print(values)
    # connect = psycopg2.connect(**conn_params)
    # cur =connect.cursor()
    # insert_sql = """
    # INSERT INTO stage_songs ( num_songs, artist_id ,artist_name, artist_latitude , artist_longitude, artist_location , song_id, title, duration , year)
    # Values ( %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    # """
    # for file_path in all_files :
    #     with open(file_path, 'r') as f:
    #         data = json.load(f)
    #         values = (
    #             data.get('num_songs'),
    #             data.get('artist_id'),
    #             data.get('artist_name'),
    #             data.get('artist_latitude'),
    #             data.get('artist_longtitude'),
    #             data.get('artist_location'),
    #             data.get('song_id'),
    #             data.get('title'),
    #             data.get('duration'),
    #             data.get('year')
    #         )
    #         try :
    #             cur.execute(insert_sql, values)
    #         except Exception as e :
    #             print(f" lỗi chèn {e}")
    #         else : 
    #             cur.commit()
    # cur.close()
    # connect.close()
    # print("đã chèn dữ liệu vào ")
# hàm load log data
def load_log_data_to_table():
    all_files = glob.glob('data/log_data/*.json')

    print(f"tổng số file json tìm được là {len(all_files)}")
    # connect = psycopg2.connect(**conn_params)
    # cur =connect.cursor()
    # insert_sql = """
    # INSERT INTO stage_songs (artist,auth,firstName,gender,itemInSession, lastName, length, level,location , method, page, registration,sessionId, song,status, ts,userAgent, userID)

    # Values ( %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    # """
    # for file_path in all_files :
    #     with open(file_path, 'r') as f:
    #         data = json.load(f)
    #         values = (
    #             data.get('artist'),
    #             data.get('auth'),
    #             data.get('firstName'),
    #             data.get('gender'),
    #             data.get('itemInSession'),
    #             data.get('lastName'),
    #             data.get('length'),
    #             data.get('level'),
    #             data.get('location'),
    #             data.get('method'),
    #             data.get('page'),
    #             data.get('registration'),
    #             data.get('sessionId'),
    #             data.get('song'),
    #             data.get('status'),
    #             data.get('ts'),
    #             data.get('userAgent'),
    #             data.get('userId')
    #         )
    #         try :
    #             cur.execute(insert_sql, values)
    #         except Exception as e :
    #             print(f" lỗi chèn {e}")
    #         else : 
    #             cur.commit()
    # cur.close()
    # connect.close()
    # print("đã chèn dữ liệu vào ")

# def run_insert_query(query):
#     def _run(**kwargs):
#         conn = psycopg2.connect(**conn_params)
#         cur = conn.cursor()
#         cur.execute(query)
#         conn.commit()
#         cur.close()
#         conn.close()
#     return _run

# conn_params = {
#     'host':'hainam',
#     'dbname':'hainam',
#     'user':'nam',
#     'password': 'hainam',
#     'port': 5432
# }

begin_execution = EmptyOperator(task_id = 'Begin_ETL1',dag = dag )
stage_events = PythonOperator(
    task_id = 'stage_eventss',
    python_callable= load_log_data_to_table,
    dag =dag 
   
)
stage_songs = PythonOperator(
    task_id = 'stage_songss',
    python_callable= load_song_data_to_table,

    dag = dag
)

# load_songplays_fact_tables = PythonOperator(
#     task_id ='load_songplays_face_table',
#     python_callable= run_insert_query(SqlQueries.songplay_table_insert),
#     dag=dag
# )
# load_user_dim_table = PythonOperator(
#     task_id = 'load_user_dim_table',
#     python_callable= run_insert_query(SqlQueries.user_table_insert),
#     dag = dag 
# )
# load_song_dim_table = PythonOperator(
#     task_id = 'load_song_dim_table', 
#     python_callable=run_insert_query(SqlQueries.song_table_insert),
#     dag= dag
# )
# load_artist_dim_table = PythonOperator(
#     task_id = 'load_time_dim_star',
#     python_callable= run_insert_query(SqlQueries.artist_table_insert),
#     dag= dag

# )
# load_time_dim_table = PythonOperator(
#     task_id ='load_time_dim',
#     python_callable=run_insert_query(SqlQueries.time_table_insert),
#     dag= dag 
# )
# def run_check_quality_data(**kwargs):
#     conn = psycopg2.connect(**conn_params)
#     cur = conn.cursor()
#     tables = ['songplays','users','songs','artists',' time']
#     for table in tables :
#         cur.execute(f"select count(*) from {table};")
#         count = cur.fetchone()[0]
#         if count == 0:
#             print(f" bảng {table} rỗng ")
#         else : 
#             print(f"đã chèn dữ liệu vào table {table}")
#     cur.close()
#     conn.close()
# data_quality_checks = PythonOperator(
#     task_id= 'data_quality_check',
#     python_callable=run_check_quality_data,
#     dag= dag 
# )

end_execution = EmptyOperator(task_id = 'End_ETLs', dag =dag)

begin_execution >>stage_events >>stage_songs>> end_execution 
#  >> load_song_dim_table>> load_time_dim_table >> load_user_dim_table >> load_artist_dim_table >> run_check_quality_data 