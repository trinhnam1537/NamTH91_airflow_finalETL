#  E chạy airflow bằng docker mà khi đọc file từ local thì không đọc được ạ , nên e cp thư mục data vào thư mục airflow trong container nhưng mà vẫn ko đọc được data

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 
import os  
import glob 
import json 
import psycopg2 
#  lớp thực thi câu lệnh queries 
class SqlQueries:
    songplay_table_insert = ("""
         INSERT INTO public.songplays (
            playid,
            start_time,
            userid,
            level,
            songid,
            artistid,
            sessionid,
            location,
            user_agent
        )
        SELECT
            md5(events.sessionid || events.start_time::text) AS playid,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
        FROM (
            SELECT 
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
                *
            FROM public.staging_events
            WHERE page = 'NextSong'
        ) AS events
        LEFT JOIN public.staging_songs AS songs
        ON events.song = songs.title
        AND events.artist = songs.artist_name
        AND events.length = songs.duration;
    """)

    user_table_insert = ("""
        INSERT INTO users (userid, first_name, last_name, gender, level)
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
        ON CONFLICT (userid) DO NOTHING
    """)

    song_table_insert = ("""
        insert into songs(songid,title,artistid, year,duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        INSERT INTO artists (artistid, name, location, lattitude, longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
                
    """)

    time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dow from start_time)
        FROM songplays
        ON CONFLICT (start_time) DO nothing
    """)



#  tham số mặc định của DAG 
default_args = {
    'owner':'NamTH91',
    'start_date': datetime(2025,7,20),
    'retries': 3 ,
    'retry_delay' : timedelta(minutes=5),
    'catchup': False,
    'email_on_retry':False
}
#  tạo DAG
dag = DAG(
    'tunestream_etl',
    default_args= default_args,
    description = 'ETL pipeline ',
    schedule_interval= '@daily',
    catchup=False
)
# hàm load data từ thư mục đến table 
def load_song_data_to_table(conn_params):
    all_files = glob.glob('data/song_data/**/*.json',recursive=True)
    print(f"tổng số file json tìm được là {len(all_files)}")
    connect = psycopg2.connect(**conn_params)
    cur =connect.cursor()
    insert_sql = """
    INSERT INTO staging_songs ( num_songs, artist_id ,artist_name, artist_latitude , artist_longitude, artist_location , song_id, title, duration , year)
    Values ( %s, %s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
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
            try :
                cur.execute(insert_sql, values)
            except Exception as e :
                print(f" lỗi chèn {e}")
            else : 
                connect.commit()
    cur.close()
    connect.close()
    print("đã chèn dữ liệu vào ")
# hàm load log data
def load_log_data_to_table(conn_params):
    all_files = glob.glob('data/log_data/*.json',recursive=True)
    print(f"tổng số file json tìm được là {len(all_files)}")
    connect = psycopg2.connect(**conn_params)
    cur =connect.cursor()
    insert_sql = """
    INSERT INTO staging_events (artist,auth,firstName,gender,itemInSession, lastName, length, level,location , method, page, registration,sessionId, song,status, ts,userAgent, userID)

    Values ( %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    def safe_data(val):
        try:
            return val if val not in ("", None) else None
        except ValueError:
            return None
    for file_path in all_files :
        with open(file_path, 'r') as f:
            for line in f:
                data = json.loads(line)
                values = (
                    data.get('artist'),
                    data.get('auth'),
                    data.get('firstName'),
                    data.get('gender'),
                    data.get('itemInSession'),
                    data.get('lastName'),
                    data.get('length'),
                    data.get('level'),
                    data.get('location'),
                    data.get('method'),
                    data.get('page'),
                    data.get('registration'),
                    data.get('sessionId'),
                    data.get('song'),
                    data.get('status'),
                    data.get('ts'),
                    data.get('userAgent'),
                    safe_data(data.get('userId'))
                )
                
                try :
                    cur.execute(insert_sql, values)
                except Exception as e :
                    print(f" lỗi chèn {e}")
                else : 
                    connect.commit()
    cur.close()
    connect.close()
    print("đã chèn dữ liệu vào ")
#  hàm thực thi câu lệnh
def run_insert_query(query):
    def _run(**kwargs):
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()
    return _run
#  hàm kiểm tra chất lượng data
def run_check_quality_data(**kwargs):
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()
    tables_info = {
        'songplays': 'playid',
        'users': 'userid',
        'songs': 'songid',
        'artists': 'artistid',
        'time': 'start_time'
    }
    def check_null(table_name, column_id):
        cur.execute(f"SELECT COUNT(*) FROM public.{table_name} where {column_id} is NULL")
        count = cur.fetchone()[0]
        if count > 0 :
            print(f"có cột null trong bảng {table_name}")
        else : 
            print(f"bảng {table_name} ko có null ")
    def check_table_empty(table_name):
        cur.execute(f"SELECT COUNT(*) FROM public.{table_name}")
        count = cur.fetchone()[0]
        if count < 1 :
            print(f"bảng {table_name} rỗng")
        else :
            print(f"bảng {table_name} đã được load data")
    def check_duplicate(table_name, column_name):
        cur.execute(f"select {column_name}, count(*) from {table_name} group by {column_name} having count(*) > 1")
        duplicates = cur.fetchall()
        if duplicates:
            print(f"bảng {table_name} có giá trị trùng lặp")
        else :
            print(f"bảng {table_name} ok ")
    for table, pk in tables_info.items():
        check_table_empty(table)
        check_duplicate(table,pk)
        check_null(table,pk)
    
    cur.close()
    conn.close()
conn_params = {
    'host':'host.docker.internal',
    'dbname':'hainam',
    'user':'nam',
    'password': 'hainam',
    'port': 5432
}
#  CÁC TASK 
begin_execution = EmptyOperator(task_id = 'Begin_ETL',dag = dag )
stage_events = PythonOperator(
    task_id = 'stage_events',
    python_callable= load_log_data_to_table,
    op_kwargs={
        
        'conn_params': conn_params
    }
)
stage_songs = PythonOperator(
    task_id = 'stage_songs',
    python_callable= load_song_data_to_table,
    op_kwargs = {
      
        'conn_params': conn_params
    },
    dag = dag
)

load_songplays_fact_tables = PythonOperator(
    task_id ='load_songplays_face_table',
    python_callable= run_insert_query(SqlQueries.songplay_table_insert),
    dag=dag
)
load_user_dim_table = PythonOperator(
    task_id = 'load_user_dim_table',
    python_callable= run_insert_query(SqlQueries.user_table_insert),
    dag = dag 
)
load_song_dim_table = PythonOperator(
    task_id = 'load_song_dim_table', 
    python_callable=run_insert_query(SqlQueries.song_table_insert),
    dag= dag
)
load_artist_dim_table = PythonOperator(
    task_id = 'load_artist_dim_star',
    python_callable= run_insert_query(SqlQueries.artist_table_insert),
    dag= dag

)
load_time_dim_table = PythonOperator(
    task_id ='load_time_dim',
    python_callable=run_insert_query(SqlQueries.time_table_insert),
    dag= dag 
)

data_quality_checks = PythonOperator(
    task_id= 'data_quality_check',
    python_callable=run_check_quality_data,
    dag= dag 
)

end_execution = EmptyOperator(task_id = 'End_ETL', dag =dag)

begin_execution >> stage_events >> stage_songs >> load_songplays_fact_tables


# sau khi load xong data vào các bảng stage thì load cac bảng dim 
load_songplays_fact_tables >> [load_song_dim_table, load_time_dim_table, load_user_dim_table, load_artist_dim_table]

# Sau khi tất cả bảng dim xong thì mới chạy kiểm tra chất lượng
[load_song_dim_table, load_time_dim_table, load_user_dim_table, load_artist_dim_table] >> data_quality_checks

# Kết thúc DAG
data_quality_checks >> end_execution