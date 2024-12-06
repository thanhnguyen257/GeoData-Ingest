import os
import json
import pandas as pd
import time

def save_json_to_excel(data, output_excel_file):
    df = pd.DataFrame(data)

    df.to_excel(output_excel_file, index=False)
    print(f"Data has been saved to {output_excel_file}")

def get_json_file_stats(directory):
    file_stats = []

    for root, _, files in os.walk(directory):
        for file in files:
            if 'RYES' in root:
                continue
            if file.endswith(".json"):
                file_path = os.path.join(root, file)

                file_size_bytes = os.path.getsize(file_path)
                file_size_kb = file_size_bytes / 1024
                file_size_mb = file_size_kb / 1024
                file_size_gb = file_size_mb / 1024

                file_2_size_bytes = 0
                file_2_size_kb = 0
                file_2_size_mb = 0
                file_2_size_gb = 0
                num_rows = 0
                time_load_json = 0
                time_create_df = 0
                time_export_parquet = 0
                try:
                    data, new_column_name, new_column_value = None, None, None
                    filename_no_ext = file.replace('.json', '')
                    year, station, data_type = "", "", ""
                    if 'HHOT' in root:
                        year, station = filename_no_ext.split('_')
                        new_column_name = ["year", "station"]
                        new_column_value = [year, station]
                    elif 'HLT' in root:
                        year, station = filename_no_ext.split('_')
                    elif 'MRS' in root:
                        year = filename_no_ext
                    elif 'TEMP' in root:
                        data_type, station = filename_no_ext.split('_')
                        new_column_name = ["data_type", "station"]
                        new_column_value = [data_type, station]

                    start_time = time.perf_counter_ns()
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        time_load_json = time.perf_counter_ns() - start_time
                        start_time = time.perf_counter_ns()
                        if 'HLT' in root:
                            data['fields'] = ['Month', 'Date', 'Time_1', 'Height_1(m)', 'Time_2', 'Height_2(m)', 'Time_3', 'Height_3(m)', 'Time_4', 'Height_4(m)']
                        df = pd.DataFrame(data['data'], columns=data['fields'])
                        time_create_df = time.perf_counter_ns() - start_time

                        if 'json_without_partition' in root and new_column_name and new_column_value:
                            data['fields'] += new_column_name
                            for i, row in enumerate(data['data']):
                                data['data'][i] = row + new_column_value

                        df = pd.DataFrame(data['data'], columns=data['fields'])
                        num_rows = len(df)

                        start_time = time.perf_counter_ns()
                        os.makedirs(root.replace('json','parquet'), exist_ok=True)
                        df.to_parquet(file_path.replace('json','parquet'), engine='pyarrow', compression='snappy', index=False)
                        time_export_parquet = time.perf_counter_ns() - start_time
                        file_2_size_bytes = os.path.getsize(file_path.replace('json','parquet'))
                        file_2_size_kb = file_2_size_bytes / 1024
                        file_2_size_mb = file_2_size_kb / 1024
                        file_2_size_gb = file_2_size_mb / 1024
                except Exception as e:
                    num_rows = "Error reading JSON"
                    print(f"Error reading {file_path}: {e}")

                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

                file_stats.append({
                    "root": root,
                    "file_path": file_path,
                    "num_rows": num_rows,
                    "json_size_bytes": file_size_bytes,
                    "json_size_kb": file_size_kb,
                    "json_size_mb": file_size_mb,
                    "json_size_gb": file_size_gb,
                    "parquet_size_bytes": file_2_size_bytes,
                    "parquet_size_kb": file_2_size_kb,
                    "parquet_size_mb": file_2_size_mb,
                    "parquet_size_gb": file_2_size_gb,
                    "time_load_json": f"{time_load_json/10**9:.4f}",
                    "time_create_df": f"{time_create_df/10**9:.4f}",
                    "time_export_parquet": f"{time_export_parquet/10**9:.4f}"
                })

    return file_stats

if __name__ == "__main__":
    directory = "/mnt/mycephfs/demo/json_with_partition"
    stats = get_json_file_stats(directory)

    directory = "/mnt/mycephfs/demo/json_without_partition"
    stats = get_json_file_stats(directory)

    output_file = "stats.xlsx"

    save_json_to_excel(stats, output_file)