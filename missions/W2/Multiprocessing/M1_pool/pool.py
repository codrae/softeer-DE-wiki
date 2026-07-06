from multiprocessing import Pool
import time

def work_log(task):
    process_name, waiting_time = task
    print(f'Process {process_name} waiting {waiting_time} seconds')
    time.sleep(waiting_time)
    print(f'Process {process_name} Finished.')

if __name__ == "__main__":
    # with 구문을 사용하면 join(), close()를 하지 않아도 됨.(자동 정리)
    with Pool(2) as p:
        p.map(work_log, [("A",5),("B",2),("C",1),("D",3)])

