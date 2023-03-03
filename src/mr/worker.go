package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"

	"github.com/google/uuid"
)

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	workerId := uuid.New()
	for {
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}
		ok := call("Coordinator.RequestTask", &args, &reply)
		if ok {
			if reply.TaskType == "map" {
				// do map task
				ids := doMapTask(mapf, reply.TaskId, reply.Filename, reply.NReduce)
				args := ReportMapTaskArgs{workerId, reply.TaskId, ids}
				reply := ReportMapTaskReply{}
				call("Coordinator.ReportMapTask", &args, &reply)
			}
			if reply.TaskType == "reduce" {
				// do reduce task
				doReduceTask(reducef, reply.TaskId, reply.MapIds)
				args := ReportReduceTaskArgs{workerId, reply.TaskId}
				reply := ReportReduceTaskReply{}
				call("Coordinator.ReportReduceTask", &args, &reply)
			}
			if reply.TaskType == "exit" {
				break
			}
		} else {
			fmt.Printf("call failed!\n")
		}
	}
}

func doMapTask(mapf func(string, string) []KeyValue, taskId int, filename string, nReduce int) []int {
	reduceIds := make([]int, 0)
	reduceFile := map[int]*os.File{}

	file, _ := os.Open(filename)
	content, _ := os.ReadFile(file.Name())
	file.Close()

	kv := mapf(filename, string(content))

	for _, value := range kv {
		reduce := ihash(value.Key) % nReduce
		if reduceFile[reduce] == nil {
			reduceFile[reduce], _ = os.Create(fmt.Sprintf("mr-%d-%d", taskId, reduce))
		}
		fmt.Fprintf(reduceFile[reduce], "%v %v\n", value.Key, value.Value)
	}

	for r, o := range reduceFile {
		o.Close()
		reduceIds = append(reduceIds, r)
	}

	return reduceIds
}

func doReduceTask(reducef func(string, []string) string, reduceId int, mapIds []int) {
	intermediate := []KeyValue{}
	for _, mapId := range mapIds {
		file, _ := os.Open(fmt.Sprintf("mr-%d-%d", mapId, reduceId))
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			kv := KeyValue{}
			fmt.Sscanf(line, "%s %s", &kv.Key, &kv.Value)
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// sort by key.
	sort.Sort(ByKey(intermediate))

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	output, _ := os.CreateTemp(".", "mr-out-*")

	defer output.Close()
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
		result := reducef(intermediate[i].Key, values)

		fmt.Fprintf(output, "%v %v\n", intermediate[i].Key, result)

		i = j
	}

	os.Rename(output.Name(), fmt.Sprintf("mr-out-%d", reduceId))
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
