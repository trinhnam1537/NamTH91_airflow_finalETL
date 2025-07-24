import os 
import glob
import json
all_files = glob.glob(os.path.join('D:/FPT materials/Airflow/prj_airflow/data/log_data', "**", "*.json"), recursive=True)
print(f"tổng số file json tìm được là {len(all_files)}")
all_files = glob.glob('data/log_data/*.json',recursive=True)
print(f"tổng số file json tìm được là {len(all_files)}")
 
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
                    data.get('userId')
                )
                print(values)