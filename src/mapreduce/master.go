package mapreduce

import "container/list"
import "fmt"
import "log"
import "time"
import "strconv"
import "sync"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	busy      bool
	bad       bool
	failTimes int
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		log.Printf("DoWork: shutdown %s\n", w.address)
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

func SingleWorker(wk *WorkerInfo, m *sync.Mutex, jobList *list.List, done chan bool, inProgress *int) {
	for wk.bad == false {
		m.Lock()
		if jobList.Len() == 0 {
			m.Unlock()
			break
		}
		job := jobList.Front()
		jobList.Remove(job)
		(*inProgress)++ // keep tracking number of in-progress jobs.
		m.Unlock()

		var reply DoJobReply
		ok := call(wk.address, "Worker.DoJob", job.Value.(*DoJobArgs), &reply)
		if ok == false {
			wk.failTimes++
			if wk.failTimes >= 10 {
				wk.bad = true
			}
			fmt.Printf("DoWork: RPC %s Do%s error\n", wk.address, job.Value.(*DoJobArgs).Operation)
			// re-assign the job
			m.Lock()
			jobList.PushFront(job.Value.(*DoJobArgs))
			m.Unlock()
		}

		m.Lock()
		(*inProgress)--
		if jobList.Len() == 0 {
			if *inProgress == 0 {
				defer func() {
					fmt.Printf("Job Done.\n")
					done <- true
				}()
			}
			m.Unlock()
			break
		}
		m.Unlock()
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	timeout := time.After(3000 * time.Millisecond)
	flag := true
	finish := make(chan int)
	mapMutex := &sync.Mutex{}
	reduceMutex := &sync.Mutex{}
	wkMutex := &sync.Mutex{}
	inProgressMap := 0
	inProgressReduce := 0

	go func() {
		<-mr.DoneMapChan
		mr.doneMap = true
		wkMutex.Lock()
		for _, wk := range mr.Workers {
			go SingleWorker(wk, reduceMutex, mr.JobListReduce, mr.DoneReduceChan, &inProgressReduce)
		}
		wkMutex.Unlock()
	}()

	go func() {
		for i := 0; flag; i += 1 {
			select {
			case <-mr.DoneReduceChan:
				flag = false
				finish <- 1
			case <-timeout:
				flag = false
			case address := <-mr.registerChannel:
				// keep listening for worker registration while doing job.
				timeout = time.After(3000 * time.Millisecond)

				workername := "worker" + strconv.Itoa(i)
				wkinfo := new(WorkerInfo)
				wkinfo.address = address
				wkMutex.Lock()
				mr.Workers[workername] = wkinfo
				wkMutex.Unlock()

				log.Printf("worker %d:%s", i, address)
				if mr.doneMap == false {
					go SingleWorker(wkinfo, mapMutex, mr.JobListMap, mr.DoneMapChan, &inProgressMap)
				} else {
					go SingleWorker(wkinfo, reduceMutex, mr.JobListReduce, mr.DoneReduceChan, &inProgressReduce)
				}
			}
		}
	}()

	select {
	// if two <-mr.DoneReduceChan exists at same time, (the other at line 101)
	// finish (line 103) will be emitted to ensure the close.
	case <-finish:
	case <-mr.DoneReduceChan:
	}
	return mr.KillWorkers()
}
