package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

type Task struct {
	TaskType   TaskType
	TaskId     int
	ReducerNum int
	FileSlice  []string
}

// TaskArgs for RPC
type TaskArgs struct{}

//Task Type
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask
	ExitTask
)

//Map-Reduce Phase
type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

// Task State
type State int

const (
	Working State = iota
	Waiting
	Done
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	keepFlag := true
	for keepFlag {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				doMapTask(mapf, &task)
				callDone(&task)
			}
		case ReduceTask:
			{
				doReduceTask(reducef, &task)
				callDone(&task)
			}
		case WaitingTask:
			{
				//fmt.Println("All tasks are in progress, please wait...")
				time.Sleep(time.Second * 5)
			}
		case ExitTask:
			{
				time.Sleep(time.Second)
				//fmt.Println("All tasks are Done ,will be exiting...")
				keepFlag = false
			}
		default:
			{
				keepFlag = false
			}
		}
		time.Sleep(time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func GetTask() Task {
	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.PollTask", &args, &reply)

	if ok {
		//fmt.Println(reply)
	} else {
		fmt.Printf("call failed!\n")
		reply.TaskType = ExitTask
	}
	return reply

}

//
//Do Map Task
//
func doMapTask(mapf func(string, string) []KeyValue, response *Task) {
	var intermediate []KeyValue
	filename := response.FileSlice[0]

	// read input file and pass to Map
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate = mapf(filename, string(content))

	// partition into NxM buckets
	reducerNum := response.ReducerNum
	hashedKV := make([][]KeyValue, reducerNum)
	for _, kv := range intermediate {
		hashedKV[ihash(kv.Key)%reducerNum] = append(hashedKV[ihash(kv.Key)%reducerNum], kv)
	}
	for i := 0; i < reducerNum; i++ {
		oname := "mr-tmp-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range hashedKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}

}

//
//Do Reduce Task
//
func doReduceTask(reducef func(string, []string) string, response *Task) {
	reduceFileNum := response.TaskId
	intermediate := shuffle(response.FileSlice)
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	fn := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), fn)
}

func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, f := range files {
		file, _ := os.Open(f)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}

//
// callDone Call RPC to mark the task as completed
//
func callDone(f *Task) Task {

	args := f
	reply := Task{}
	ok := call("Coordinator.MarkFinished", &args, &reply)

	if ok {
		//fmt.Println("worker finish :taskId[", args.TaskId, "]")
	} else {
		fmt.Printf("Coordinator.MarkFinished() call failed!\n")
	}
	return reply

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
