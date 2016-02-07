package mapreduce

import "container/list"
import (
	"fmt"
	"sync/atomic"
)


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
	idleWorkers := make(chan *WorkerInfo)

	// handle map jobs
	mapJobs := make(chan int)
	mapDone := make(chan bool)
	var mapCnt int32 = 0

	go func () {
    for i := range mapJobs {
			if (mr.IdleWorkers.Len() == 0) {
				select {
				case worker := <- idleWorkers:
					mr.IdleWorkers.PushBack(worker)
				case workerAddress := <- mr.registerChannel:
					worker := &WorkerInfo{workerAddress}
					mr.Workers[workerAddress] = worker
					mr.IdleWorkers.PushBack(worker)
				}
			}

      args := &DoJobArgs{mr.file, Map, i, mr.nReduce}
      var reply DoJobReply
      frontElement := mr.IdleWorkers.Front()
      worker := mr.IdleWorkers.Remove(frontElement).(*WorkerInfo)

      go func () {
        success := call(worker.address, "Worker.DoJob", args, &reply)

				if (success) {
					atomic.AddInt32(&mapCnt, 1)
					if (atomic.LoadInt32(&mapCnt) == int32(mr.nMap)) {
						mapDone <- true
					}

					idleWorkers <- worker
				} else {
					mapJobs <- i
				}
      }()
    }
  }()

	for i := 0; i < mr.nMap; i++ {
		mapJobs <- i
	}

	<- mapDone
	close(mapJobs)

	// handle reduce jobs
	reduceJobs := make(chan int)
	reduceDone := make(chan bool)
	var reduceCnt int32 = 0

	go func () {
		for i := range reduceJobs {
			if (mr.IdleWorkers.Len() == 0) {
				select {
				case worker := <- idleWorkers:
					mr.IdleWorkers.PushBack(worker)
				case workerAddress := <- mr.registerChannel:
					worker := &WorkerInfo{workerAddress}
					mr.Workers[workerAddress] = worker
					mr.IdleWorkers.PushBack(worker)
				}
			}

			args := &DoJobArgs{mr.file, Reduce, i, mr.nMap}
			var reply DoJobReply
			frontElement := mr.IdleWorkers.Front()
			worker := mr.IdleWorkers.Remove(frontElement).(*WorkerInfo)

			go func () {
				success := call(worker.address, "Worker.DoJob", args, &reply)

				if (success) {
					atomic.AddInt32(&reduceCnt, 1)
					if (atomic.LoadInt32(&reduceCnt) == int32(mr.nReduce)) {
						reduceDone <- true
					}

					idleWorkers <- worker
				} else {
					reduceJobs <- i
				}
			}()
		}
	}()

	for i := 0; i < mr.nReduce; i++ {
		reduceJobs <- i
	}

	<- reduceDone
	close(reduceJobs)

	return mr.KillWorkers()
}
