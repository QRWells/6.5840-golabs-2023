package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	Idle = iota
	InProgress
	Completed // unused
)

type MapTask struct {
	Status   int
	FileName string
}

type ReduceTask struct {
	Status int
	MapIds []int
}

type Coordinator struct {
	// Your definitions here.
	lock        sync.Mutex
	nReduce     int
	mapTasks    map[int]*MapTask
	reduceTasks map[int]*ReduceTask
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	reply.TaskId = -1
	if len(c.mapTasks) > 0 {
		for id, mapTask := range c.mapTasks {
			if mapTask.Status == Idle {
				mapTask.Status = InProgress
				reply.TaskType = "map"
				reply.TaskId = id
				reply.Filename = mapTask.FileName
				reply.NReduce = c.nReduce
				break
			}
		}

		if reply.Filename != "" {
			go func(id int) {
				time.Sleep(5 * time.Second)
				c.lock.Lock()
				if c.mapTasks[id] != nil {
					c.mapTasks[id].Status = Idle
				}
				c.lock.Unlock()
			}(reply.TaskId)
		}

	} else if len(c.reduceTasks) > 0 {
		for id, reduceTask := range c.reduceTasks {
			if reduceTask.Status == Idle {
				reduceTask.Status = InProgress
				reply.TaskType = "reduce"
				reply.TaskId = id
				reply.MapIds = reduceTask.MapIds
				break
			}
		}

		if reply.TaskId != -1 {
			go func(id int) {
				time.Sleep(5 * time.Second)
				c.lock.Lock()
				if c.reduceTasks[reply.TaskId] != nil {
					c.reduceTasks[reply.TaskId].Status = Idle
				}
				c.lock.Unlock()
			}(reply.TaskId)
		}
	} else {
		reply.TaskType = "exit"
	}

	return nil
}

func (c *Coordinator) ReportMapTask(args *ReportMapTaskArgs, reply *ReportMapTaskReply) error {
	// Your code here.
	c.lock.Lock()
	defer c.lock.Unlock()
	// remove map task
	delete(c.mapTasks, args.TaskId)

	for _, i := range args.ReduceIds {
		if c.reduceTasks[i] == nil {
			c.reduceTasks[i] = &ReduceTask{Status: Idle, MapIds: []int{}}
		}
		task := c.reduceTasks[i]
		task.MapIds = append(task.MapIds, args.TaskId)
	}

	return nil
}

func (c *Coordinator) ReportReduceTask(args *ReportReduceTaskArgs, reply *ReportReduceTaskReply) error {
	// Your code here.
	c.lock.Lock()
	defer c.lock.Unlock()
	// remove reduce task
	delete(c.reduceTasks, args.TaskId)
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	mapTasks := make(map[int]*MapTask)
	for id, file := range files {
		mapTasks[id] = &MapTask{Idle, file}
	}

	reduceTasks := make(map[int]*ReduceTask)

	c.nReduce = nReduce
	c.mapTasks = mapTasks
	c.reduceTasks = reduceTasks
	c.lock = sync.Mutex{}

	c.server()
	return &c
}
