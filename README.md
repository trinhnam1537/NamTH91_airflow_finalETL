
# Airflow Final Project: Data Warehouse ETL Workflow

## 1. Prerequisites

Before running this project, please make sure you have the following software installed on your machine:

- Visual Studio Code
- Python 
- Docker Desktop  


---

## 2. Project Setup

1. Download this project as a ZIP file and extract it to your computer.
2. Create a new empty folder on your D drive (recommended).
3. Open this folder in VSCode. On Windows, you can do this by clicking the folder path bar, typing `cmd`, and pressing Enter. This will open a terminal window. In that terminal, type:

```bash
code .
```

This will launch VSCode opened at the folder you just created.

4. In VSCode, go to the **Terminal** menu and select **New Terminal**.

5. In the terminal, run the following command to initialize an empty Airflow project using Astro:

```bash
docker compose up -d
```


This command will build the required Docker images and start containers for Airflow.

Once the setup completes successfully, you will see a message like this:

<img width="1565" height="778" alt="Image" src="https://github.com/user-attachments/assets/0ac36e59-7d58-471e-a6f6-121eeb0be751" />

You can then access the Airflow UI by opening your browser at the displayed URL (usually [http://localhost:8080](http://localhost:8080)).

Your project is now set up and ready to run!

---

## 3. Running the DAG

Login the UI through account name admin and password admin 
we can see the layout of localhost 
<img width="1897" height="647" alt="Image" src="https://github.com/user-attachments/assets/e6acab65-064e-4ab8-ae43-ce858b3f3236" />


After saving the connection, go to the **DAGs** page.

![alt text](image-4.png)

Then, find the DAG named **tunestream_etl** and click the **Trigger** button at the top right corner.
Wait until the DAG finishes running. :>

---

## 4. Results

<img width="1748" height="717" alt="Image" src="https://github.com/user-attachments/assets/1f343717-9431-42ef-a6c5-7a26fa1745ec" />

### Graphs

<img width="1883" height="595" alt="Image" src="https://github.com/user-attachments/assets/2deef13a-efad-436b-aadb-00662b692c80" />

### Logs


#### stage_songs task
<img width="1906" height="550" alt="Image" src="https://github.com/user-attachments/assets/ee2242e7-3e4c-42b9-b5c7-bb96ac301bd4" />

#### stage_events task
<img width="1708" height="521" alt="Image" src="https://github.com/user-attachments/assets/943ed6d2-407f-426c-9aac-cc3ada105674" />
#### Load_songplays_fact_table task
<img width="1919" height="478" alt="Image" src="https://github.com/user-attachments/assets/467caefe-dfb0-4d7e-a5e4-24107966dc28" />
#### Load_user_dim_table task
<img width="1854" height="572" alt="Image" src="https://github.com/user-attachments/assets/6b4c11ba-832f-4b85-ac56-485c5f9d322e" />
#### Load_artist_dim_table task
<img width="1883" height="555" alt="Image" src="https://github.com/user-attachments/assets/9718d1fb-83bd-4582-83db-90f150a766de" />
#### Load_song_dim_table task
<img width="1919" height="530" alt="Image" src="https://github.com/user-attachments/assets/9064c015-36ad-48c0-97c4-3ea481042748" />
#### Load_time_dim_table task
<img width="1906" height="594" alt="Image" src="https://github.com/user-attachments/assets/44dc09af-ec08-4ec4-8816-1c670472c560" />

#### Run_data_quality_checks task
<img width="1877" height="632" alt="Image" src="https://github.com/user-attachments/assets/d3ffc149-cb9e-4f6b-8827-2f4b5f954d4e" />


### Check data directly in Postgres (in DBeaver)

-- Check first 10 rows in main tables
SELECT * FROM songplays LIMIT 10;
SELECT * FROM songs LIMIT 10;
SELECT * FROM artists LIMIT 10;
SELECT * FROM users LIMIT 10;
SELECT * FROM "time" LIMIT 10;
SELECT * FROM staging_events LIMIT 10;
SELECT * FROM staging_songs LIMIT 10;

-- Count total records in each table
SELECT COUNT(*) AS total_songplays FROM songplays;
SELECT COUNT(*) AS total_songs FROM songs;
SELECT COUNT(*) AS total_artists FROM artists;
SELECT COUNT(*) AS total_users FROM users;
SELECT COUNT(*) AS total_time FROM "time";
SELECT COUNT(*) AS total_staging_events FROM staging_events;
SELECT COUNT(*) AS total_staging_songs FROM staging_songs;


<img width="1265" height="770" alt="Image" src="https://github.com/user-attachments/assets/9410ec5e-5cbe-4069-b86b-c37335e11047" />

<img width="1056" height="552" alt="Image" src="https://github.com/user-attachments/assets/bf802e46-c06a-40c4-8195-71b870b81ca5" />

<img width="1238" height="819" alt="Image" src="https://github.com/user-attachments/assets/67bb53d8-ee79-4388-8364-6ed78530df4b" />

<img width="1285" height="808" alt="Image" src="https://github.com/user-attachments/assets/fc905daf-3f4d-4961-90cb-b06e6ea71dd4" />

<img width="1441" height="801" alt="Image" src="https://github.com/user-attachments/assets/0e65535e-7ed5-4cfe-9f25-e0acfe61886d" />

<img width="1478" height="814" alt="Image" src="https://github.com/user-attachments/assets/90fb7a4d-a203-40f0-89d6-f5734e215a54" />

<img width="1355" height="800" alt="Image" src="https://github.com/user-attachments/assets/9acc1c43-50b1-4cc1-857d-14059dda52e7" />

## 5. Contact me

if you have any problems , please contact via email and phone number 

```
trinhnama31609@gmail.com
0812042726

```
