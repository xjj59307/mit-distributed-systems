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
	mapDone := make(chan int)
  idle := make(chan string, 100)

  for i := 0; i < mr.nMap; i++ {
    taskId := i
    go func() {
      // retry the task until it succeeds
      for {
        select {
        case worker := <- idle:
          fmt.Println(taskId, "started on", worker)
          args := &DoJobArgs{mr.file, Map, taskId, mr.nReduce}
          var reply DoJobReply
          success := call(worker, "Worker.DoJob", args, &reply)
          idle <- worker
          if success {
            mapDone <- taskId
            return
          }
        case worker := <- mr.registerChannel:
          fmt.Println(worker, "registered")
          idle <- worker
        }
      }
    }()
  }

	for i := 0; i < mr.nMap; i++ {
    fmt.Println(<- mapDone, "map succeeded")
  }

	reduceDone := make(chan int)

  for i := 0; i < mr.nReduce; i++ {
    taskId := i
    go func() {
      // retry the task until it succeeds
      for {
        select {
        case worker := <- idle:
          fmt.Println(taskId, "started on", worker)
          args := &DoJobArgs{mr.file, Reduce, taskId, mr.nMap}
          var reply DoJobReply
          success := call(worker, "Worker.DoJob", args, &reply)
          idle <- worker
          if success {
            reduceDone <- taskId
            return
          }
        case worker := <- mr.registerChannel:
          fmt.Println(worker, "registered")
          idle <- worker
        }
      }
    }()
  }

  for i := 0; i < mr.nReduce; i++ {
    fmt.Println(<- reduceDone, "reduce succeeded")
  }

	return mr.KillWorkers()
}
