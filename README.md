# MIT 6.584 (6.828)

This repo contains my implementations of mit 6.584 and notes.

## Lab1

All tests have been passed. 

Summary: 

- Lab1 implements a map-reduce system, including basic crash handles for workers.
- There is a coordinator running as a RPC server and tasks scheduler.
    - Splits input files to map tasks
    - Provide AllocateTask, which allocate a idle task to a worker
    - Provide SubmitTask, which receive a task finished by a worker
    - Create reduce tasks according to completed map tasks
    - Watching one task to be finished in 10 seconds. If timeout, assume the worker has crashed, and reschedule the task.
- There are workers running to fetch tasks, solve tasks, and submit tasks.

Notes: 
- notes/lab1.md