package mapreduce

import (
	"container/list"
)
import "fmt"

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
	go func(mr *MapReduce) {
		for worker := range mr.registerChannel {
			workerInfo := &WorkerInfo{worker}
			mr.Workers[worker] = workerInfo
			mr.idleWorker <- worker
		}
	}(mr)

	go func(mr *MapReduce) {
		count := 0
		for range mr.finishedMapJob {
			count++
			if count == mr.nMap-1 {
				mr.doneMapChannel <- true
				return
			}
		}
	}(mr)

	go func(mr *MapReduce) {
		count := 0
		for range mr.finishedReduceJob {
			count++
			if count == mr.nReduce-1 {
				mr.doneReduceChannel <- true
				return
			}
		}
	}(mr)

	for i := 0; i < mr.nMap; i++ {
		go func(index int, mr *MapReduce) {
			for {
				idle := <-mr.idleWorker
				args := DoJobArgs{
					File:          mr.file,
					Operation:     JobType("Map"),
					JobNumber:     index,
					NumOtherPhase: mr.nReduce,
				}
				var reply DoJobReply
				if call(idle, "Worker.DoJob", args, &reply) {
					mr.finishedMapJob <- 1
					mr.idleWorker <- idle
					return
				} else {
					fmt.Printf("Failed to process. Retrying...")
				}
			}
		}(i, mr)
	}

	<-mr.doneMapChannel
	for i := 0; i < mr.nReduce; i++ {
		go func(index int, mr *MapReduce) {
			for {
				idle := <-mr.idleWorker
				args := DoJobArgs{
					File:          mr.file,
					Operation:     JobType("Reduce"),
					JobNumber:     index,
					NumOtherPhase: mr.nMap,
				}
				var reply DoJobReply
				if call(idle, "Worker.DoJob", args, &reply) {
					mr.finishedReduceJob <- 1
					mr.idleWorker <- idle
					return
				} else {
					fmt.Printf("Failed to process. Retrying...")
				}
			}
		}(i, mr)
	}
	<-mr.doneReduceChannel
	return mr.KillWorkers()
}
