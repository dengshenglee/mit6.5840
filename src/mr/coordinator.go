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

type Coordinator struct {
	// Your definitions here.

	// Protect Coordinator state
	// from concurrent access
	mu sync.Mutex

	// Allow Coordinator to wait to assign reduce tasks until map tasks have finished,
	// or when all tasks are signed and are running.
	// The Coordinator is woken up either when a task has finished, or if a timeout has expired.
	cond *sync.Cond

	mapFiles     []string
	nMapTasks    int
	nReduceTasks int

	// Keep track of the state of each task when they are assigned to workers
	mapTasksFinished    []bool
	mapTasksIssued      []time.Time
	reduceTasksFinished []bool
	reduceTasksIssued   []time.Time

	//set to true when all reduce tasks are finished
	isDone bool
}

// Your code here -- RPC handlers for the worker to call.

// Handle GetTask RPC from worker
func (c *Coordinator) HandleGetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NReduceTasks = c.nReduceTasks
	reply.NMapTasks = c.nMapTasks

	// Issued map tasks until there are no map tasks left
	for {
		mapDone := true
		for m, done := range c.mapTasksFinished {
			if !done {
				// assigned a task if it's either never been issued,
				// or if it's been too long since it was issued so the worker may have crashed.
				// Note: if task has never been issued, time is initialized to 0 UTC.
				if c.mapTasksIssued[m].IsZero() ||
					time.Since(c.mapTasksIssued[m]).Seconds() > 10 {
					reply.TaskType = Map
					reply.TaskNum = m
					reply.MapFile = c.mapFiles[m]
					c.mapTasksIssued[m] = time.Now()
					return nil
				} else {
					mapDone = false
				}
			}
		}

		// if all maps are in progress and haven't time out, wait to give another task
		if !mapDone {
			c.cond.Wait()
		} else {
			// we're done with all map tasks
			break
		}
	}

	// All map tasks are done, issued reduce task now!
	for {
		redDone := true
		for r, done := range c.reduceTasksFinished {
			if !done {
				// assign a task if it's either never been issued, or if it's been too long
				if c.reduceTasksIssued[r].IsZero() ||
					time.Since(c.reduceTasksIssued[r]).Seconds() > 10 {
					reply.TaskType = Reduce
					reply.TaskNum = r
					c.reduceTasksIssued[r] = time.Now()
					return nil
				} else {
					redDone = false
				}
			}
		}

		if !redDone {
			c.cond.Wait()
		} else {
			break
		}
	}

	// if all map and reduce tasks are done, send the querying worker a Done task, and set isDone to true
	reply.TaskType = Done
	c.isDone = true

	return nil
}

func (c *Coordinator) HandleFinishedTask(args *FinishedTaskArgs, reply *FinishedTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case Map:
		c.mapTasksFinished[args.TaskNum] = true
	case Reduce:
		c.reduceTasksFinished[args.TaskNum] = true
	default:
		log.Fatalf("Bad finished task? %d", args.TaskType)
	}

	//wake up the GetTask handler loop: a task has finished, so we might be able to assign another one
	c.cond.Broadcast()

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

	return c.isDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.cond = sync.NewCond(&c.mu)
	c.nMapTasks = len(files)
	c.mapTasksFinished = make([]bool, len(files))
	c.mapTasksIssued = make([]time.Time, len(files))
	c.mapFiles = files

	c.nReduceTasks = nReduce
	c.reduceTasksFinished = make([]bool, nReduce)
	c.reduceTasksIssued = make([]time.Time, nReduce)

	//wake up the GetTask handler thread every once in awhile to check if some task hasn't
	//finished, so we can know to reissue it
	go func() {
		for {
			c.mu.Lock()
			c.cond.Broadcast()
			c.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()

	c.server()
	return &c
}
