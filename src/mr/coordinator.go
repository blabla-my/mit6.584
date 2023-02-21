package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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
	Tasktype int32
	State    int32
	Id       int
	/* for map Task */
	KeyValues []KeyValue
	/* for reduce Task */
	HashIdx           int
	IntermediateFiles []string
	R                 int
}

type Coordinator struct {
	// Your definitions here.
	Tasks []Task
	R     int
	mutex sync.Mutex
}

func CopyTask(tasksrc *Task, taskdst *Task) {
	taskdst.Tasktype = tasksrc.Tasktype
	taskdst.State = tasksrc.State
	taskdst.Id = tasksrc.Id
	taskdst.KeyValues = tasksrc.KeyValues
	taskdst.IntermediateFiles = tasksrc.IntermediateFiles
	taskdst.R = tasksrc.R
	taskdst.HashIdx = tasksrc.HashIdx
}

func (c *Coordinator) findReduceTaskByhash(hashidx int) *Task {
	// c.mutex.Lock()
	// defer c.mutex.Unlock()
	for i, _ := range c.Tasks {
		if c.Tasks[i].Tasktype == TYPE_REDUCE && c.Tasks[i].HashIdx == hashidx {
			return &c.Tasks[i]
		}
	}
	c.Tasks = append(c.Tasks, Task{Tasktype: TYPE_REDUCE, State: TASK_IDLE, Id: rand.Int(), HashIdx: hashidx, R: c.R})
	return &c.Tasks[len(c.Tasks)-1]
}

func (c *Coordinator) checkTasks() {
	fmt.Println("------------------------------------------------------------------------------------------")
	for i, _ := range c.Tasks {
		fmt.Printf("%v, %v, %v, %v, %v\n", c.Tasks[i].Tasktype, c.Tasks[i].Id, c.Tasks[i].State, c.Tasks[i].HashIdx, c.Tasks[i].IntermediateFiles)
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AllocateTask(args *VArgs, reply *Task) error {
	// Lock the coordinator
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Allocate map Task firstly
	// log.Printf("Task length is %v", len(c.Tasks))
	for i, _ := range c.Tasks {
		if c.Tasks[i].Tasktype == TYPE_MAP && c.Tasks[i].State == TASK_IDLE {
			c.Tasks[i].State = TASK_RUNNING
			CopyTask(&c.Tasks[i], reply)
			return nil
		}
	}
	// If there is no map tasks, allocate reduce tasks
	for i, _ := range c.Tasks {
		if c.Tasks[i].Tasktype == TYPE_REDUCE && c.Tasks[i].State == TASK_IDLE {
			c.Tasks[i].State = TASK_RUNNING
			CopyTask(&c.Tasks[i], reply)
			return nil
		}
	}

	Tasknil := Task{Tasktype: TYPE_NIL, R: c.R}
	CopyTask(&Tasknil, reply)
	return nil
}

func (c *Coordinator) SubmitTask(task *Task, reply *VReply) error {
	c.mutex.Lock()
	// fmt.Printf("Locked coordinator")
	defer c.mutex.Unlock()

	for i, _ := range c.Tasks {
		if c.Tasks[i].Id == task.Id {
			CopyTask(task, &c.Tasks[i])
			c.Tasks[i].State = TASK_COMPLETED
		}
	}
	if task.Tasktype == TYPE_MAP {
		for _, file := range task.IntermediateFiles {
			splits := strings.Split(file, "-")
			hashidx, err := strconv.Atoi(splits[len(splits)-1])
			if err != nil {
				fmt.Println("Failed to atoi.")
			}
			reduceTask := c.findReduceTaskByhash(hashidx)
			reduceTask.IntermediateFiles = append(reduceTask.IntermediateFiles, file)
			// fmt.Println(c.Tasks)
		}
	}
	// fmt.Println("Submit done.")
	c.checkTasks()
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
		if c.Tasks[idx].State != TASK_COMPLETED {
			return false
		}
	}
	return true
}

func (c *Coordinator) createMapTask(filename string) error {
	task := Task{R: c.R, Id: rand.Int()}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return err
	}
	file.Close()

	task.Tasktype = TYPE_MAP
	task.State = TASK_IDLE
	task.KeyValues = append(task.KeyValues, KeyValue{filename, string(content)})

	c.Tasks = append(c.Tasks, task)

	return nil
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
	/* To make map tasks */
	for _, filename := range files {
		ok := c.createMapTask(filename)
		if ok != nil {
			log.Fatalf("Failed to initialize map task %v.", filename)
		}
	}
	c.server()
	return &c
}
