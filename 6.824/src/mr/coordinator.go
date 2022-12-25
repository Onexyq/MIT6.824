package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
)

var mu sync.Mutex

type Coordinator struct {
	// Your definitions here.
	TaskChannelMap    chan *Task
	TaskChannelReduce chan *Task
	ReducerNum        int
	MapNum            int
	MrPhase           Phase
	TaskId            int
	taskMetaHolder    TaskMetaHolder
	files             []string
}

// TaskMetaHolder storing meta data of all tasks
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo
}

type TaskMetaInfo struct {
	state   State
	TaskAdr *Task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	mu.Lock()
	defer mu.Unlock()

	if c.MrPhase == AllDone {
		fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		return true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReducerNum:        nReduce,
		MrPhase:           MapPhase,
		TaskChannelMap:    make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce), // total num of tasks = files + reducer
		},
	}
	c.makeMapTasks(files)

	os.Mkdir("MapOut/", os.ModePerm)
	os.Mkdir("ReduceOut/", os.ModePerm)

	c.server()
	return &c
}

//
//Map Tasks Generation
//
func (c *Coordinator) makeMapTasks(files []string) {
	for _, v := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			FileSlice:  []string{v},
		}

		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskAdr: &task,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		fmt.Println("built a map task:", &task)
		c.TaskChannelMap <- &task
	}
}

//
//Reduce Tasks Generation
//
func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskType:   ReduceTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			FileSlice:  selectReduceName(i),
		}

		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskAdr: &task,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		fmt.Println("built a reduce task:", &task)
		c.TaskChannelReduce <- &task
	}
}

func (c *Coordinator) generateTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

func (t *TaskMetaHolder) acceptMeta(TaskInfo *TaskMetaInfo) bool {
	taskId := TaskInfo.TaskAdr.TaskId
	meta := t.MetaMap[taskId]
	if meta != nil {
		fmt.Println("meta contains task which id = ", taskId)
		return false
	} else {
		t.MetaMap[taskId] = TaskInfo
	}
	return true
}

func selectReduceName(reduceNum int) []string {
	var s []string
	dir, _ := os.Getwd()
	path := dir + "/MapOut/"
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

//Task Dispatch
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	// mutex to prevent race between workers
	mu.Lock()
	defer mu.Unlock()

	if c.MrPhase == MapPhase {
		if len(c.TaskChannelMap) > 0 {
			*reply = *<-c.TaskChannelMap
			if !c.taskMetaHolder.judgeState(reply.TaskId) {
				fmt.Printf("taskid[ %d ] is running\n", reply.TaskId)
			}
		} else {
			reply.TaskType = WaitingTask
			if c.taskMetaHolder.checkTaskDone() {
				c.toNextPhase()
			}
			return nil
		}

	} else if c.MrPhase == ReducePhase {
		if len(c.TaskChannelReduce) > 0 {
			*reply = *<-c.TaskChannelReduce
			if !c.taskMetaHolder.judgeState(reply.TaskId) {
				fmt.Printf("taskid[ %d ] is running\n", reply.TaskId)
			}
		} else {
			reply.TaskType = WaitingTask
			if c.taskMetaHolder.checkTaskDone() {
				c.toNextPhase()
			}
			return nil
		}
	} else {
		reply.TaskType = ExitTask
	}

	return nil
}

func (c *Coordinator) toNextPhase() {
	if c.MrPhase == MapPhase {
		c.makeReduceTasks()
		c.MrPhase = ReducePhase
	} else if c.MrPhase == ReducePhase {
		c.MrPhase = AllDone
	}
}

func (t *TaskMetaHolder) checkTaskDone() bool {

	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)

	for _, v := range t.MetaMap {
		if v.TaskAdr.TaskType == MapTask {
			if v.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskAdr.TaskType == ReduceTask {
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}

	}
	// fmt.Printf("map tasks are finished %d/%d, reduce task are finished %d/%d \n",
	// mapDoneNum, mapDoneNum+mapUnDoneNum, reduceDoneNum, reduceDoneNum+reduceUnDoneNum)

	// change phase to next if all Map tasks are done, or all Reduce tasks are done
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else {
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}

	return false

}

func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.state != Waiting {
		return false
	}
	taskInfo.state = Working
	return true
}

func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	if args.TaskType == MapTask {

		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
		//prevent a duplicated work which returned from another worker
		if ok && meta.state == Working {
			meta.state = Done
			fmt.Printf("Map task Id[%d] finished.\n", args.TaskId)
		} else {
			fmt.Printf("Map task Id[%d] finished already ! ! !\n", args.TaskId)
		}
	} else if args.TaskType == ReduceTask {
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
		//prevent a duplicated work which returned from another worker
		if ok && meta.state == Working {
			meta.state = Done
			fmt.Printf("Reduce task Id[%d] is finished.\n", args.TaskId)
		} else {
			fmt.Printf("Reduce task Id[%d] is finished,already ! ! !\n", args.TaskId)
		}
	} else {
		panic("The task type is undefined ! ! !")
	}
	return nil

}
