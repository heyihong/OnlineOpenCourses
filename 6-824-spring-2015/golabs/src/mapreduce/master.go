package mapreduce

import "container/list"
import "fmt"
import "sync/atomic"
import "runtime"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	nMap := mr.nMap
	nReduce := mr.nReduce

	doJobArgsChannel := make(chan DoJobArgs)

	mapJobsDone := make(chan bool)
	reduceJobsDone := make(chan bool)

	var nFinishedMapJob int32 = 0
	var nFinishedReduceJob int32 = 0

	// Manage register workers
	go func() {
		for worker := range mr.registerChannel {
			go func(workerName string) {
				for args := range doJobArgsChannel {
					var reply DoJobReply
					ok := call(workerName, "Worker.DoJob", args, &reply)
					if ok {
						switch args.Operation {
						case Map:
							atomic.AddInt32(&nFinishedMapJob, 1)
							if atomic.LoadInt32(&nFinishedMapJob) == int32(nMap) {
								mapJobsDone <- true
							}
						case Reduce:
							atomic.AddInt32(&nFinishedReduceJob, 1)
							if atomic.LoadInt32(&nFinishedReduceJob) == int32(nReduce) {
								reduceJobsDone <- true
							}
						}
						runtime.Gosched()
					} else {
						doJobArgsChannel <- args
						break
					}
				}
			}(worker)
		}
	}()

	done := make(chan bool)
	// Generate map and reduce jobs
	go func() {
		for i := 0; i < nMap; i++ {
			doJobArgs := DoJobArgs{
				File:          mr.file,
				Operation:     Map,
				JobNumber:     i,
				NumOtherPhase: nReduce}
			doJobArgsChannel <- doJobArgs
		}
		// Wait until all map jobs finished
		<-mapJobsDone
		for i := 0; i < nReduce; i++ {
			doJobArgs := DoJobArgs{
				File:          mr.file,
				Operation:     Reduce,
				JobNumber:     i,
				NumOtherPhase: nMap}
			doJobArgsChannel <- doJobArgs
		}
		// Wait until all reduce jobs finished
		<-reduceJobsDone
		done <- true
	}()
	<-done
	return mr.KillWorkers()
}
