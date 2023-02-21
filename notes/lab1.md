## Summary: 

- Lab1 implements a map-reduce system, including basic crash handles for workers.
- There is a coordinator running as a RPC server and tasks scheduler.
    - Splits input files to map tasks
    - Provide AllocateTask, which allocate a idle task to a worker
    - Provide SubmitTask, which receive a task finished by a worker
    - Create reduce tasks according to completed map tasks
    - Watching one task to be finished in 10 seconds. If timeout, assume the worker has crashed, and reschedule the task.
- There are workers running to fetch tasks, solve tasks, and submit tasks.

## Start point

To be familiar with golang, I firstly turn to A Tour of Go to learn basic syntax and features.

I read the paper of map-reduce to get basic understandings. Then I read mrsequential.go to understand basic workflow of map-reduce within a single thread. 

There are some examples about the RPC in mr/rpc.go, mr/worker.go, mr/coordinator.go. No extra efforts on RPC are needed, just follow what CallExample() does. To be specific, implement a method for *Coordinator, with (args, reply) as arguments. Remember to define TYPE_A and TYPE_B with capitalized names:

```golang
type TYPE_A struct {
    ...
}

type TYPE_B struct {
    ...
}

func (c *Coordinator) RPCmethod(args TYPE_A, reply TYPE_B) error {
    ...
}
```

## Design of Coordinator

### Data Structure
Task structure represents a task. It may be a MAP task or a REDUCE task. Every Task obtains a unique Id which is generated randomly. For MAP task, the (key, value) pairs will be stored in Task structure. For REDUCE task, the hashIdx of the task and all intermediateFiles with hashidx=R will be stored in the task. 

The intermediateFiles are, files that contains the output (key, value) list from map function. The list is sorted by key. The files are encoded in json, easy to marshal and unmarshal. 

```golang
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
```

```golang 
type Coordinator struct {
	Tasks []Task
	R     int
	mutex sync.Mutex
}
```

### Workflow
Firstly, create map tasks. In lab1, the input of map-reduce is a set of files. I create a map task per file and store the (filename, filecontent) into a task structure. The createMapTask will be invoked before the RPC server setup.

```golang
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
```

Secondly, setup the RPC server, then the coordinator will listen for the workers' requests. There are two RPC methods available for workers: 

1. `AllocateTask()` . AllocateTask will return a task to the caller. It will firtly look into `Coordinator.Tasks` to find map tasks. If all map tasks are completed, it will allocate a reduce task, otherwise it will return a nil task.

2. `SubmitTask()` . SubmitTask will receive a task from caller, update its state in `Coordinator.Tasks`. Most importantly, it will gather all intermediate files with the same hashidx into corresponding reduce task (Every reduce task only contains intermediate that shares the same hashidx).

This two method will operate the `Coordinator.Tasks`. So a mutex lock is needed to prevent race.


## Design of worker
### Workflow
The worker's workflow is easy to understand, containing three steps in a loop:

1. allocate a task
2. solve the task
3. submit the task

Allocating and submitting is achieved by RPC. Solving a task is similar to mrsequential.go. But after solving a map task, intermediate keyvalues should be marshal to json file; before solving a reduce task, intermediate keyvalues should be unmarshaled from json files provided by the coordinator.