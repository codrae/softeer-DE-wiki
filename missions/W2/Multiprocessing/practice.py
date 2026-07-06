from multiprocessing import Pool, Process
import os
import time

'''
함수 f를 모듈 최상단에 정의해야 함.
자식 함수(process)가 부모 함수(f)를 가져와서 실행해야 하기 때문.
다른 함수 내부에 정의되거나 lambda 구문을 사용한다면 pickle 에러 발생.
'''
def f(x):
    return x*x

def info(title):
    print(title)
    print('module name: ', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())

def hello(name):
    info('function hello')
    print('hello', name)

def work_log(task):
    process_name, waiting_time = task
    print('Process', process_name , 'waiting', waiting_time, 'seconds')
    time.sleep(waiting_time)
    print('Process', process_name , 'Finished.')



# __main__진입점이 없다면, 본 스크립트가 import 될때마다 프로세스를 생성.
# 자식이 import 할때는 진입점 내부 구문 실행되지 않도록 막아줌.
if __name__ == "__main__":
    # 5개의 워커를 생성
    with Pool(5) as p:
        print(p.map(f, [1,2,3,4,5,6]))

    info('main line')
    p = Process(target = hello, args = ('bob',))
    p.start()
    p.join()

    with Pool(2) as p:
        p.map(work_log, [("A",5),("B",2),("C",1),("D",3)])

# GIL때문에 thread가 아닌 process를 활용해 병렬처리를 하는데,
# 이에 따라 실행중인 프로세스를 밖에서 terminate/interrupt/kill 할 수 있다.

