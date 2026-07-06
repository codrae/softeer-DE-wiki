### 학습 목표
Demonstrate your understanding of Python's multiprocessing.Pool by writing a script that utilizes a pool of workers to execute tasks concurrently.

### 기능요구사항
Task Definition:

Define a list of tasks (work) with each task having a name (like 'A', 'B', etc.) and a duration (integer representing seconds).

Worker Pool Setup:

Initialize a Pool with a specified number of workers (2 in this case).

Task Execution:

For each task in the work list, print a waiting message indicating the task name and duration.

Simulate the task by sleeping for the specified duration using time.sleep.

After completing the task, print a message indicating that the task has finished.

Concurrency:

Ensure that tasks are executed concurrently using the worker pool (Pool).

### 프로그래밍 요구사항
Use Python's multiprocessing module.

Define a list of tasks (work) where each task consists of a name and a duration.

Use multiprocessing.Pool to handle the execution of tasks concurrently.

Define a function (work_log) that simulates each task by printing a waiting message and then sleeping for the specified duration.

Use map function of Pool to map the work_log function to the list of tasks.

### 예상결과 및 동작예시
```
Process A waiting 5 seconds
Process B waiting 2 seconds
Process B Finished.
Process C waiting 1 seconds
Process C Finished.
Process D waiting 3 seconds
Process A Finished.
Process D Finished.
```

