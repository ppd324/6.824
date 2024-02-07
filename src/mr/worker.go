package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"

	"github.com/google/uuid"
)

var (
	WorkerId string
)

func init() {
	WorkerId = uuid.New().String()
}

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	ok := Register()
	for ok {
		task, err := GetTask()
		if err != nil {
			fmt.Println("woker exit")
			break
		}
		switch task.TaskType {
		case "map":
			MapFunc(task.MapFilename, task.NReduce, mapf)
		case "reduce":
			ReduceFunc(task.ReduceFiles, task.ReduceIndex, reducef)
		case "done":
			return
		}
	}

}

func MapFunc(filename string, nReduce int, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("woker failed to open this file,", err)
		ReportTaskStatus(&ReportTaskStatusAgs{
			TaskType:   "map",
			Filename:   filename,
			IsFinished: false,
			Status:     Idle,
		})
		return err
	}
	fmt.Printf("mapping:%v  -------------------\n", filename)
	content, err := io.ReadAll(file)
	if err != nil {
		log.Println("cannot read :", filename)
		ReportTaskStatus(&ReportTaskStatusAgs{
			TaskType:   "map",
			Filename:   filename,
			IsFinished: false,
			Status:     Idle,
		})
		return err
	}
	kvArray := mapf(filename, string(content))
	buckets := Partition(kvArray, nReduce)
	tempFiles := make([][]string, len(buckets))
	for i, _ := range buckets {
		//create temp file
		if len(buckets[i]) == 0 {
			continue
		}
		filename := "mr-map-" + uuid.NewString() + "-" + strconv.Itoa(i)
		bytes, _ := json.Marshal(buckets[i])
		os.WriteFile(filename, bytes, 0644)
		tempFiles[i] = append(tempFiles[i], filename)
	}
	return ReportTaskStatus(&ReportTaskStatusAgs{
		TaskType:   "map",
		Filename:   filename,
		IsFinished: true,
		TempFile:   tempFiles,
		Status:     Idle,
	})

}

func ReduceFunc(fileList []string, AllocatedIndex int, reducef func(string, []string) string) {
	kvArray := make([]KeyValue, 0)
	for _, v := range fileList {
		contents, err := os.ReadFile(v)
		if err != nil {
			ReportTaskStatus(&ReportTaskStatusAgs{
				TaskType:    "reduce",
				ReduceIndex: AllocatedIndex,
				IsFinished:  false,
			})
			return
		}
		var kvs []KeyValue
		err = json.Unmarshal(contents, &kvs)
		if err != nil {
			ReportTaskStatus(&ReportTaskStatusAgs{
				TaskType:    "reduce",
				ReduceIndex: AllocatedIndex,
				IsFinished:  false,
			})
			return
		}
		kvArray = append(kvArray, kvs...)

	}
	sort.Sort(ByKey(kvArray))
	oname := "mr-out-" + strconv.Itoa(AllocatedIndex)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(kvArray) {
		j := i + 1
		for j < len(kvArray) && kvArray[j].Key == kvArray[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvArray[k].Value)
		}
		output := reducef(kvArray[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kvArray[i].Key, output)
		i = j
	}
	ofile.Close()
	ReportTaskStatus(&ReportTaskStatusAgs{
		TaskType:    "reduce",
		IsFinished:  true,
		ReduceIndex: AllocatedIndex,
	})

}
func Partition(kvArray []KeyValue, nReduce int) [][]KeyValue {
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kvArray {
		bucketIndex := ihash(kv.Key) % nReduce
		buckets[bucketIndex] = append(buckets[bucketIndex], kv)
	}
	return buckets

}

// example function to show how to make an RPC call to the Coordinator.
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func Register() bool {
	args := WorkerRigisterArgs{
		Id: WorkerId,
	}
	reply := WorkerRegisterReply{}
	call("Coordinator.Register", &args, &reply)
	fmt.Printf("reply results %v\n", reply.Status)
	return reply.Status == "ok"
}

func GetTask() (*GetTaskReply, error) {
	args := GetTaskArgs{
		Status: Idle,
	}
	reply := GetTaskReply{}
	if call("Coordinator.GetTask", &args, &reply) {
		fmt.Printf("get task results: %v\n", reply)
		return &reply, nil
	}
	return nil, errors.New("rpc call failed")

}

func ReportTaskStatus(req *ReportTaskStatusAgs) error {
	reply := ReportTaskStatusResply{}
	if call("Coordinator.ReportTaskStatus", req, &reply) {
		return nil
	}
	return errors.New("rpc call failed")
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.


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
