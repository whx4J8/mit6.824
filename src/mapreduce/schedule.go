package mapreduce

import (
	"fmt"
)

type Callback struct {
	name string
	callbackChannel chan string
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	if(mr.stats == nil){
		mr.stats = make([]int,0)
	}

	callback := &Callback {
		name : "callback",
		callbackChannel : make(chan string),
	}

	workdone := make(chan string)
	faildone := make(chan int,100)

	handleRegisterEvent(mr, callback)	//单独handle注册事件
	handleFailTaskEvent(mr,faildone,workdone,phase,nios,callback)	//捕捉worker执行失败的事件

	for i:=0 ; i<ntasks ; i++ {					//先分配map任务给当前空闲的worker
		go doTask(i,faildone,workdone,phase,mr,nios,callback)
	}

	for i:=0 ; i < ntasks ; i++ {			//等待所有worker完成
		fmt.Println("worker complete ", (i+1) )
		<- workdone
	}

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
 	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}

func handleFailTaskEvent(mr *Master,
			faildone chan int,
			workdone chan string,
			phase jobPhase,
			nios int,
			callback *Callback,){
	go func(){
		for true {
			failTaskIndex := <-faildone
			go doTask(failTaskIndex,faildone,workdone,phase,mr,nios,callback)
		}
	}()

}

func doTask(taskIndex int,
		faildone chan int,
		workdone chan string,
		phase jobPhase,
		mr *Master,
		nios int,
		callback *Callback, ){

	var taskArgs *DoTaskArgs
	if phase == mapPhase{
		taskArgs = &DoTaskArgs{
			JobName : mr.jobName,
			File : mr.files[taskIndex],
			Phase:mapPhase,
			TaskNumber:taskIndex,
			NumOtherPhase:nios,
		}
	}else {
		taskArgs = &DoTaskArgs{
			JobName : mr.jobName,
			Phase:reducePhase,
			TaskNumber:taskIndex,
			NumOtherPhase:nios,
		}
	}

	ok,worker := callWorkerDoTask(mr,taskArgs,callback)
	if(ok){
		workdone <- "work done"
	} else {
		go func(){
			for i,v := range mr.workers  {
				if worker == v {
					mr.stats[i] = 2
				}
			}

			faildone <- taskIndex
		}()
		fmt.Println("do " + taskArgs.Phase + " error, put to failFiles , task index ",taskArgs.TaskNumber," workers : ",mr.workers, mr.stats)
	}


}

func callWorkerDoTask(mr *Master,
		taskArgs *DoTaskArgs,
		callback *Callback)(bool,string){

	w,index := idleWorker(mr,callback)		//阻塞,等待可用worker

	reply := new(struct{})

	ok := call(w,"Worker.DoTask",taskArgs,reply)

	mr.stats[index] = 0		//完成任务

	select {
	case callback.callbackChannel <- "done":
	default :
	}

	return ok,w
}



/**
	获取空闲的worker
 */
func idleWorker(mr *Master,callback *Callback) (idleWorker string,index int){

	//fmt.Printf("before getidle worker : %s stats : %s\n",mr.workers, mr.stats)
	for true {
		idleWorker,index = getIdleWorker(mr)

		if idleWorker == "" {						//等待新事件
			event := <- callback.callbackChannel
			if event == "register" {
				//fmt.Println("handle new register worker event")
			}else {
				//fmt.Println("handle new done worker event")
			}
		} else{								//获取到woker
			break;
		}

	}

	//fmt.Printf("after getidle worker : %s stats %d\n:",mr.workers, mr.stats)
	return idleWorker,index
}

/**
	获取可用状态的worker
 */
func getIdleWorker(mr *Master)(idleWorker string,index int){
	mr.Lock()
	defer mr.Unlock()
	for i,_ := range mr.stats {
		if(mr.stats[i] == 0 ){					//有可用的worker
			idleWorker = mr.workers[i]
			index = i
		}
	}
	if idleWorker != "" {
		mr.stats[index] = 1
	}

	return
}

/**
	handle 注册事件
		1.将注册的worker添加到状态队列
		2.通知事件callback

 */
func handleRegisterEvent(mr *Master,callback *Callback){

	go func(mr *Master){
		for true{
			<- mr.registerChannel				//wait 新的注册事件
			mr.Lock()
			mr.stats = append(mr.stats,0)			//初始化可用状态
			callback.callbackChannel <- "register"		//通知callback
			mr.Unlock()
		}

	}(mr)

}
