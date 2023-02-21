package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Marshal(intermediate *ByKey, filename string) error {
	bytes, err := json.MarshalIndent(intermediate, "", "    ")
	if err != nil {
		fmt.Println("Faied to marshal.")
		return err
	}
	f, err := os.Create(filename)
	if err != nil {
		fmt.Println("Failed to create marshal file")
		return err
	}
	// fmt.Fprint(f, bytes)
	_, err = f.Write(bytes)
	if err != nil {
		fmt.Println("Failed to write to file.")
		return err
	}
	return nil
}

func Unmarshal(filename string) (ByKey, error) {
	blob, err := os.ReadFile(filename)
	if err != nil {
		fmt.Println("Failed to read file for unmarshal")
		return nil, err
	}

	keyvalues := ByKey{}
	err = json.Unmarshal(blob, &keyvalues)
	if err != nil {
		fmt.Println("Failed to unmarshal")
		return nil, err
	}
	return keyvalues, nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for true {
		task := CallAllocateTask()
		if task == nil {
			return
		}
		SolveTask(task, mapf, reducef)
		CallSubmitTask(task)
		// CallExample()
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func SolveTask(task *Task, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) error {

	if task.Tasktype == TYPE_MAP {
		intermediate := ByKey{}
		for i, _ := range task.KeyValues {
			kva := mapf(task.KeyValues[i].Key, task.KeyValues[i].Value)
			intermediate = append(intermediate, kva...)
		}

		sort.Sort(intermediate)
		intermediates := map[int]ByKey{}
		for i, _ := range intermediate {
			hashidx := ihash(intermediate[i].Key) % task.R
			intermediates[hashidx] = append(intermediates[hashidx], intermediate[i])
		}

		for k, v := range intermediates {
			// fmt.Println(k)
			if len(v) > 0 {
				ofilename := "/tmp/mrwoker-inter-"
				ofilename = ofilename + strconv.Itoa(os.Getpid()) + "-" + strconv.Itoa(task.Id) + "-"
				ofilename = ofilename + strconv.Itoa(k)
				err := Marshal(&v, ofilename)
				if err != nil {
					fmt.Println("Failed to solve map task")
				}
				task.IntermediateFiles = append(task.IntermediateFiles, ofilename)
			}
		}
	}
	if task.Tasktype == TYPE_REDUCE {
		/* firstly, merge all files to get intermediate */
		intermediate := ByKey{}
		for _, filename := range task.IntermediateFiles {
			intermediateFromFile, err := Unmarshal(filename)
			if err != nil {
				fmt.Println("Failed to unmarshal.")
			}
			intermediate = append(intermediate, intermediateFromFile...)
		}

		sort.Sort(intermediate)

		/* secondly, get values together for reducef */
		oname := "mr-out-" + strconv.Itoa(os.Getpid()) + "-" + strconv.Itoa(task.Id)
		ofile, _ := os.Create(oname)
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
			i = j
		}

		ofile.Close()
	}
	return nil
}

func CallAllocateTask() *Task {
	task := Task{}
	args := VArgs{}
	ok := call("Coordinator.AllocateTask", &args, &task)
	if ok {
		// fmt.Printf("Successfully allocated task. Type: %v. \n", task.Tasktype)
		return &task
	} else {
		return nil
	}
}

func CallSubmitTask(task *Task) error {
	reply := VReply{}
	ok := call("Coordinator.SubmitTask", task, &reply)
	if ok {
		// fmt.Println("Successfully submit task.")
		return nil
	}
	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
