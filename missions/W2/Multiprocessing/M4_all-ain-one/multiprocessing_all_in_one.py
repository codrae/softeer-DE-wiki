from multiprocessing import Queue, current_process, Process
import queue
import time

tasks = [0,1,2,3,4,5,6,7,8,9]

task_to_accomplish = Queue()
task_that_are_done = Queue()
processes = []

# Queue를 전역으로 바로 사용하면 안되는 이유
def worker(to_do, done):
    while True:
        try :
            task = to_do.get_nowait()
        except queue.Empty:
            break
        else:
            print(f"Task no {task}")
            time.sleep(0.5)
            process_name = current_process().name
            done.put(f"Task no {task} is done by {process_name}")

if __name__ == '__main__':
    for t in tasks:
        task_to_accomplish.put(t)

    for _ in range(4):
        # 인자가 하나일때 ,를 적어주는 이유
        p = Process(target=worker, args=(task_to_accomplish,task_that_are_done))
        processes.append(p)
        p.start()

    # join 전에 담아두는 이유
    results = []
    # .empty를 사용하지 않는 이유
    for _ in range(len(tasks)):
       results.append(task_that_are_done.get())

    for p in processes:
        p.join()

    # join 이후에 출력하는 이유
    for result in results:
        print(result)

# 왜 줄바꿈 없이 출력되는 현상이 발생하는가