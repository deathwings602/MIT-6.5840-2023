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

type taskStatus int

const (
	NotStarted taskStatus = iota
	InProgress
	Completed
)

const timeout = 10 * time.Second

type Coordinator struct {
	// Your definitions here.
	mu                sync.Mutex
	nReduce           int
	nMap              int
	inputFilenames    []string
	mapTasksStatus    []taskStatus
	numMapFinished    int
	reduceTasksStatus []taskStatus
	numReduceFinished int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.numMapFinished < c.nMap {
		allocateTaskId := -1
		for i, status := range c.mapTasksStatus {
			if status == NotStarted {
				allocateTaskId = i
				c.mapTasksStatus[i] = InProgress
				break
			}
		}
		if allocateTaskId != -1 {
			reply.TaskType = TaskMap
			reply.MapTaskId = allocateTaskId
			reply.MapFilename = c.inputFilenames[allocateTaskId]
			reply.NMap = c.nMap
			reply.NReduce = c.nReduce

			go func() {
				time.Sleep(timeout)
				c.mu.Lock()
				defer c.mu.Unlock()
				if c.mapTasksStatus[allocateTaskId] == InProgress {
					c.mapTasksStatus[allocateTaskId] = NotStarted
				}
			}()
		} else {
			reply.TaskType = TaskWait
		}
	} else if c.numMapFinished == c.nMap && c.numReduceFinished < c.nReduce {
		allocateTaskId := -1
		for i, status := range c.reduceTasksStatus {
			if status == NotStarted {
				allocateTaskId = i
				c.reduceTasksStatus[i] = InProgress
				break
			}
		}
		if allocateTaskId != -1 {
			reply.TaskType = TaskReduce
			reply.ReduceTaskId = allocateTaskId
			reply.NMap = c.nMap
			reply.NReduce = c.nReduce

			go func() {
				time.Sleep(timeout)
				c.mu.Lock()
				defer c.mu.Unlock()
				if c.reduceTasksStatus[allocateTaskId] == InProgress {
					c.reduceTasksStatus[allocateTaskId] = NotStarted
				}
			}()
		} else {
			reply.TaskType = TaskWait
		}
	} else {
		reply.TaskType = TaskExit
	}

	return nil
}

func (c *Coordinator) FinishMapTask(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapTasksStatus[args.MapTaskId] != Completed {
		c.mapTasksStatus[args.MapTaskId] = Completed
		c.numMapFinished++
	}
	return nil
}

func (c *Coordinator) FinishReduceTask(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reduceTasksStatus[args.ReduceTaskId] != Completed {
		c.reduceTasksStatus[args.ReduceTaskId] = Completed
		c.numReduceFinished++
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
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	ret = c.numReduceFinished == c.nReduce

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.inputFilenames = files
	c.nReduce = nReduce
	c.nMap = len(files)
	c.mapTasksStatus = make([]taskStatus, c.nMap)
	for i := range c.mapTasksStatus {
		c.mapTasksStatus[i] = NotStarted
	}
	c.reduceTasksStatus = make([]taskStatus, c.nReduce)
	for i := range c.reduceTasksStatus {
		c.reduceTasksStatus[i] = NotStarted
	}

	c.server()
	return &c
}
