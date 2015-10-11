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
    var mapChan, reduceChan = make(chan int, mr.nMap), make(chan int, mr.nReduce)

    var send_map = func(worker string, job int) bool {
        var jobArgs DoJobArgs
        var jobReply DoJobReply
        jobArgs.File = mr.file
        jobArgs.Operation = Map
        jobArgs.JobNumber = job
        jobArgs.NumOtherPhase = mr.nReduce
        return call(worker, "Worker.DoJob", jobArgs, &jobReply)
    }

    var send_reduce = func(worker string, job int) bool {
        var jobArgs DoJobArgs
        var jobReply DoJobReply
        jobArgs.File = mr.file
        jobArgs.Operation = Reduce
        jobArgs.JobNumber = job
        jobArgs.NumOtherPhase = mr.nMap
        return call(worker, "Worker.DoJob", jobArgs, &jobReply)
    }

    for i := 0; i < mr.nMap; i++ {
        go func(job int) {
            for {
                var worker string
                var ok bool = false
                select {
                case worker = <-mr.idleChannel:
                    ok = send_map(worker, job)
                case worker = <-mr.registerChannel:
                    ok = send_map(worker, job)
                }
                if ok {
                    mapChan<-job
                    mr.idleChannel<-worker
                    return
                }
            }
        }(i)
    }

    // wait for map jobs done
    for i := 0; i < mr.nMap; i++ {
        <- mapChan
    }
    fmt.Println("Map jobs done.")

    for i := 0; i < mr.nReduce; i++ {
        go func(job int) {
            for {
                var worker string
                var ok bool = false
                select {
                case worker = <-mr.idleChannel:
                    ok = send_reduce(worker, job)
                case worker = <-mr.registerChannel:
                    ok = send_reduce(worker, job)
                }
                if ok {
                    reduceChan<-job
                    mr.idleChannel<-worker
                    return
                }
            }
        }(i)
    }

    // wait for reduce jobs done
    for i := 0; i < mr.nReduce; i++ {
        <-reduceChan
    }
    fmt.Println("Reduce jobs done.")
	return mr.KillWorkers()
}
