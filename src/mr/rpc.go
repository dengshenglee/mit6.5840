package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

/*
* Define a type and constants for the task type
 */
type TaskType int

const (
	Map TaskType = iota
	Reduce
	Done //There are no pending tasks
)

/*
* GetTaskArgs are sent from an idle worker to the coordinator to ask for the next task to perform
 */

// No arguments are needed for GetTask
type GetTaskArgs struct{}

type GetTaskReply struct {
	// The type of task to perform
	TaskType TaskType

	// task number of either map or reduce task
	TaskNum int

	// needed for Map (to know which file to write)
	NReduceTasks int

	// needed for Map (to know which file to read)
	MapFile string

	// needed for Reduce (to know which files to read)
	NMapTasks int
}

/*
* FinishedTaskArgs RPCs are sent from an idle worker to coordinator to indicate that the worker has finished a task.
*
 */
type FinishedTaskArgs struct {
	// what type of task was the worker assigned
	TaskType TaskType
	//which task was it
	TaskNum int
}

type FinishedTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
