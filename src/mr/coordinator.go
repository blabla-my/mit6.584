package mr

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	// Task state
	TASK_IDLE      int32 = 0
	TASK_RUNNING   int32 = 1
	TASK_COMPLETED int32 = 2
	// Task type
	TYPE_NIL    int32 = -1
	TYPE_MAP    int32 = 0
	TYPE_REDUCE int32 = 1
	// worker state
	WORKER_BUSY int32 = 0
	WORKER_FREE int32 = 1
)

type Task struct {
	tasktype int32
	state    int32
	id       int
	/* for map Task */
	keyValues []KeyValue
	/* for reduce Task */
	hashIdx           int
	intermediateFiles []string
}

type Coordinator struct {
	// Your definitions here.
	Tasks []Task
	R     int
	mutex sync.Mutex
}

func CopyTask(tasksrc *Task, taskdst *Task) {
	taskdst.tasktype = tasksrc.tasktype
	taskdst.state = tasksrc.state
	taskdst.id = tasksrc.id
	taskdst.keyValues = tasksrc.keyValues
	taskdst.intermediateFiles = tasksrc.intermediateFiles
}

func (c *Coordinator) findReduceTaskByhash(hashidx int) *Task {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for i, _ := range c.Tasks {
		if c.Tasks[i].tasktype == TYPE_REDUCE && c.Tasks[i].hashIdx == hashidx {
			return &c.Tasks[i]
		}
	}
	c.Tasks = append(c.Tasks, Task{tasktype: TYPE_REDUCE, state: TASK_IDLE, id: rand.Int(), hashIdx: hashidx})
	return &c.Tasks[len(c.Tasks)-1]
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AllocateTask(args *interface{}, reply *Task) error {
	// Lock the coordinator
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Allocate map Task firstly

	for i, _ := range c.Tasks {
		if c.Tasks[i].tasktype == TYPE_MAP && c.Tasks[i].state == TASK_IDLE {
			c.Tasks[i].state = TASK_RUNNING
			CopyTask(&c.Tasks[i], reply)
			return nil
		}
	}
	// If there is no map tasks, allocate reduce tasks
	for i, _ := range c.Tasks {
		if c.Tasks[i].tasktype == TYPE_REDUCE && c.Tasks[i].state == TASK_IDLE {
			c.Tasks[i].state = TASK_RUNNING
			CopyTask(&c.Tasks[i], reply)
			return nil
		}
	}

	Tasknil := Task{tasktype: TYPE_NIL}
	CopyTask(&Tasknil, reply)
	return nil
}

func (c *Coordinator) SubmitTask(task *Task, reply interface{}) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i, _ := range c.Tasks {
		if c.Tasks[i].id == task.id {
			CopyTask(task, &c.Tasks[i])
			c.Tasks[i].state = TASK_COMPLETED
		}
	}
	if task.tasktype == TYPE_MAP {
		for _, kv := range task.keyValues {
			hashidx := ihash(kv.Key) % c.R
			reduceTask := c.findReduceTaskByhash(hashidx)
			reduceTask.intermediateFiles = append(reduceTask.intermediateFiles, task.intermediateFiles...)
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for idx, _ := range c.Tasks {
		if c.Tasks[idx].state != TASK_COMPLETED {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{[]Task{}, nReduce, sync.Mutex{}}
	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()

	fmt.Println("Coordinator start")
	c.server()
	return &c
}
