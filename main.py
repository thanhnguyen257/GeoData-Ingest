from task_queue import TaskQueue
from thread_pool import ThreadPoolManager
from custom_tasks import CustomDataIngestionTask
import time
import os
from elasticsearch import Elasticsearch

THREAD_POOL_SIZE = 20
cephfs_dir = "/mnt/mycephfs"
datasets = [{'CLMTEMP', 'CLMMAXT', 'CLMMINT'},{'HHOT'},{'HLT'},{'MRS'},{'RYES'}]

def main():
    meta_data_list = []
    for dataset in datasets:
        if dataset == {'RYES'}:
            continue
        for attempt in range(1):
            print(f"Attemp {attempt} for dataset {dataset}")
            for i in range(1, THREAD_POOL_SIZE+1):
                task_queue = TaskQueue()

                customdataingestiontask = CustomDataIngestionTask("","")
                data_ingest_info = customdataingestiontask.create_data_info()
                for info in data_ingest_info:
                    if info["dataType"] in dataset:
                        task_queue.add_task(CustomDataIngestionTask(info["url"], f"{cephfs_dir}/demo/json/{info['path']}"))
                thread_pool = ThreadPoolManager(max_workers=i)
                start_time = time.perf_counter_ns()
                thread_pool.process_queue(task_queue)

                thread_pool.shutdown()

                finish_time = time.perf_counter_ns()
                exe_time = finish_time - start_time
                print(f"{exe_time/10**9:.4f}")

                meta_data_list = customdataingestiontask.meta_data
        print()
        
    if meta_data_list:
        es = Elasticsearch("http://localhost:9200")
        index_name = "metadata_index"
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name)
        for meta_id, meta_data in enumerate(meta_data_list, start=1):
            meta_data['filePath'] = f"{cephfs_dir}/demo/json/{meta_data['filePath']}"
            if os.path.exists(meta_data['filePath']):
                creation_time = os.path.getctime(meta_data['filePath'])
                formatted_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(creation_time))
                meta_data['ingestionTime'] = formatted_time
                es.index(index=index_name, id=meta_id, document=meta_data)

if __name__ == "__main__":
    main()
