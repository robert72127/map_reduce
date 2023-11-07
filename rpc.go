package mr

//
// RPC definitions.
//

import (
	"os"
	"strconv"
)

type AssignArgs struct {
}

type AssignReply struct {
	FileName   string // will return itoa of index in case of reduce
	Task       string
	Task_nr    int
	Task_count int //how many tasks there are in total, to split map output into distinct files
}

type FinishArgs struct {
	FileName string
	Task     string
	Task_nr  int
}

type FinishReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
