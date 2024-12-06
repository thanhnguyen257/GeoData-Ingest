from concurrent.futures import ThreadPoolExecutor

class ThreadPoolManager:
    def __init__(self, max_workers):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def process_queue(self, task_queue):
        while not task_queue.is_empty():
            task = task_queue.get_task()
            self.executor.submit(task)

    def shutdown(self):
        self.executor.shutdown(wait=True)
