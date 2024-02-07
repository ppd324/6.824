package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type HandleStatus int

const (
	Maping HandleStatus = iota
	Reducing
	Done
)

type FileStatus int

const (
	UnAllocated FileStatus = iota
	Allocated
	Finished
)

type WorkerStatus int

const (
	Idle WorkerStatus = iota
	Map
	Reduce
	Crash
)

type Coordinator struct {
	// Your definitions here.
	Files             []string
	HandleProgress    HandleStatus
	AllFilesStatus    map[string]FileStatus
	ReduceIndexStatus map[int]FileStatus
	WorkerStatus      map[string]WorkerStatus
	ReduceFiles       [][]string
	fileChans         map[string]chan struct{}
	mapCheckChan      chan struct{}
	mapDoneCount      int
	reduceDoneCount   int
	reduceCheckChan   chan struct{}
	mapFileChan       chan string
	reduceIndexChan   chan int
	NReduce           int
	Lock              *sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
BEGIN:
	for {
		m.Lock.RLock()
		currentProgress := m.HandleProgress
		m.Lock.RUnlock()
		switch currentProgress {
		case Maping:
			select {
			case filename := <-m.mapFileChan:
				reply.MapFilename = filename
				reply.TaskType = "map"
				reply.NReduce = m.NReduce
				m.Lock.Lock()
				m.AllFilesStatus[filename] = Allocated
				m.fileChans[filename] = make(chan struct{}, 1)
				m.Lock.Unlock()
				go m.timerForWorker("map", filename)
				return nil
			default:
				time.Sleep(time.Millisecond * 50)
				goto BEGIN
			}
		case Reducing:
			select {
			case index := <-m.reduceIndexChan:
				reply.TaskType = "reduce"
				reply.NReduce = m.NReduce
				reply.ReduceFiles = m.ReduceFiles[index]
				reply.ReduceIndex = index
				m.Lock.Lock()
				m.ReduceIndexStatus[index] = Allocated
				m.Lock.Unlock()
				m.fileChans[strconv.Itoa(index)] = make(chan struct{}, 1)
				go m.timerForWorker("reduce", strconv.Itoa(index))
				return nil
			default:
				time.Sleep(time.Millisecond * 50)
				goto BEGIN
			}
		case Done:
			reply.TaskType = "done"
			reply.NReduce = m.NReduce
			return nil
		}
	}
	// select {
	// case filename := <-m.mapFileChan:
	// 	reply.MapFilename = filename
	// 	reply.TaskType = "map"
	// 	reply.NReduce = m.NReduce
	// 	m.Lock.Lock()
	// 	m.AllFilesStatus[filename] = Allocated
	// 	m.fileChans[filename] = make(chan struct{}, 1)
	// 	m.Lock.Unlock()
	// 	go m.timerForWorker("map", filename)
	// 	return nil
	// case index := <-m.reduceIndexChan:
	// 	reply.TaskType = "reduce"
	// 	reply.NReduce = m.NReduce
	// 	reply.ReduceFiles = m.ReduceFiles[index]
	// 	reply.ReduceIndex = index
	// 	m.Lock.Lock()
	// 	m.ReduceIndexStatus[index] = Allocated
	// 	m.Lock.Unlock()
	// 	m.fileChans[strconv.Itoa(index)] = make(chan struct{}, 1)
	// 	go m.timerForWorker("reduce", strconv.Itoa(index))
	// 	return nil
	// default:
	// 	reply.TaskType = "done"
	// 	reply.NReduce = m.NReduce
	// 	return nil
	// }
}

func (m *Coordinator) ReportTaskStatus(args *ReportTaskStatusAgs, reply *ReportTaskStatusResply) error {
	if args == nil {
		return errors.New("call failed")
	}
	switch args.TaskType {
	case "map":
		if args.Filename != "" {
			m.Lock.Lock()
			defer m.Lock.Unlock()
			if args.IsFinished {
				m.AllFilesStatus[args.Filename] = Finished
				m.mapDoneCount++
				if m.mapDoneCount >= len(m.Files)-1 {
					m.mapCheckChan <- struct{}{}
				}
				m.fileChans[args.Filename] <- struct{}{}
				if args.TempFile != nil {
					for i := range args.TempFile {
						m.ReduceFiles[i] = append(m.ReduceFiles[i], args.TempFile[i]...)
					}
				}
			} else {
				m.AllFilesStatus[args.Filename] = UnAllocated
				m.fileChans[args.Filename] <- struct{}{}
			}
		}
	case "reduce":
		m.Lock.Lock()
		defer m.Lock.Unlock()
		if args.IsFinished {
			m.ReduceIndexStatus[args.ReduceIndex] = Finished
			m.reduceDoneCount++
			if m.reduceDoneCount >= m.NReduce-1 {
				m.reduceCheckChan <- struct{}{}
			}
			m.fileChans[strconv.Itoa(args.ReduceIndex)] <- struct{}{}
		} else {
			m.ReduceIndexStatus[args.ReduceIndex] = UnAllocated
			m.fileChans[strconv.Itoa(args.ReduceIndex)] <- struct{}{}
		}
	}
	return nil

}

// worker register
func (m *Coordinator) Register(args *WorkerRigisterArgs, reply *WorkerRegisterReply) error {
	if args.Id == "" {
		reply.Status = "failed"
		return errors.New("register failed")
	}
	m.Lock.Lock()
	m.WorkerStatus[args.Id] = Idle
	m.Lock.Unlock()
	reply.Status = "ok"
	fmt.Printf("worker:%v register successful\n", args.Id)
	return nil
}

// start a thread that listens for RPCs from worker.go


// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func (m *Coordinator) timerForWorker(taskType, identify string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	defer close(m.fileChans[identify])
	defer delete(m.fileChans, identify)
	for {
		select {
		case <-ticker.C:
			if taskType == "map" {
				m.Lock.RLock()
				defer m.Lock.RUnlock()
				if m.AllFilesStatus[identify] != Finished {
					fmt.Printf("map %v handle timeout,reschedule\n",identify)
					m.mapFileChan <- identify
				}
			} else if taskType == "reduce" {
				index, _ := strconv.Atoi(identify)
				m.Lock.RLock()
				defer m.Lock.RUnlock()
				if m.ReduceIndexStatus[index] != Finished {
					fmt.Printf("reduce %v handle timeout,reschedule\n",identify)
					m.reduceIndexChan <- index
				}
			}
			return
		case <-m.fileChans[identify]:
			if taskType == "map" {
				m.Lock.RLock()
				defer m.Lock.RUnlock()
				if m.AllFilesStatus[identify] != Finished {
					fmt.Printf("map %v handle failed,reschedule\n",identify)
					m.mapFileChan <- identify
				}
				return
			} else if taskType == "reduce" {
				index, _ := strconv.Atoi(identify)
				m.Lock.RLock()
				defer m.Lock.RUnlock()
				if m.ReduceIndexStatus[index] != Finished {
					fmt.Printf("reduce %v handle failed,reschedule\n",identify)
					m.reduceIndexChan <- index
				}
				return
			}
		}

	}
}

func (m *Coordinator) checkMapAllFinished() bool {
	m.Lock.Lock()
	defer m.Lock.Unlock()
	for _, v := range m.AllFilesStatus {
		if v != Finished {
			return false
		}
	}
	return true
}

func (m *Coordinator) checkReduceAllFinished() bool {
	m.Lock.Lock()
	defer m.Lock.Unlock()
	for _, v := range m.ReduceIndexStatus {
		if v != Finished {
			return false
		}
	}
	return true
}

func (m *Coordinator) generateTask() {
	fmt.Println("map-----------------------------------------------------------------------")
	for k, v := range m.AllFilesStatus {
		if v == UnAllocated {
			m.mapFileChan <- k
		}
	}
	for {
		<-m.mapCheckChan
		if m.checkMapAllFinished() {
			break
		}

	}
	m.Lock.Lock()
	m.HandleProgress = Reducing
	m.Lock.Unlock()
	fmt.Println("reduce-----------------------------------------------------------------------")
	for k, v := range m.ReduceIndexStatus {
		if v == UnAllocated {
			m.reduceIndexChan <- k
		}
	}
	for {
		<-m.reduceCheckChan
		if m.checkReduceAllFinished() {
			break
		}
	}
	fmt.Println("done-----------------------------------------------------------------------")
	m.Lock.Lock()
	m.HandleProgress = Done
	m.Lock.Unlock()

}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	t := make(map[string]FileStatus, len(files))
	for _, name := range files {
		t[name] = UnAllocated
	}
	r := make(map[int]FileStatus, nReduce)
	for i := 0; i < nReduce; i++ {
		r[i] = UnAllocated
	}
	m := Coordinator{
		Files:             files,
		AllFilesStatus:    t,
		ReduceIndexStatus: r,
		mapDoneCount:      0,
		reduceDoneCount:   0,
		WorkerStatus:      make(map[string]WorkerStatus),
		ReduceFiles:       make([][]string, nReduce),
		HandleProgress:    Maping,
		mapFileChan:       make(chan string, len(files)),
		mapCheckChan:      make(chan struct{}, len(files)),
		reduceCheckChan:   make(chan struct{}, nReduce),
		reduceIndexChan:   make(chan int, nReduce),
		fileChans:         make(map[string]chan struct{}),
		NReduce:           nReduce,
		Lock:              new(sync.RWMutex),
	}
	go m.generateTask()

	// Your code here.

	m.server()
	return &m
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Coordinator) server() {
	rpc.Register(m)
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

