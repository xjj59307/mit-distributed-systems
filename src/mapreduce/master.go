package mapreduce

import "container/list"
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
	// Your code here
  worker := <- mr.registerChannel
	mapDone := make(chan bool, mr.nMap)

	for i := 0; i < mr.nMap; i++ {
		args := &DoJobArgs{mr.file, Map, i, mr.nReduce}
		var reply DoJobReply

		go func () {
			mapDone <- call(worker, "Worker.DoJob", args, &reply)
		}()
	}

	for i := 0; i < mr.nMap; i++ {
    if <- mapDone == false {
      fmt.Println("DoJob: RPC %s call error\n", worker)
    }
  }

	reduceDone := make(chan bool, mr.nReduce)

	for i := 0; i < mr.nReduce; i++ {
		args := &DoJobArgs{mr.file, Reduce, i, mr.nMap}
		var reply DoJobReply

		go func () {
			reduceDone <- call(worker, "Worker.DoJob", args, &reply)
		}()
	}

	for i := 0; i < mr.nReduce; i++ {
		if <- reduceDone == false {
			fmt.Println("DoJob: RPC %s call error\n", worker)
		}
	}

	return mr.KillWorkers()
}
