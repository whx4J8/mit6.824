package mapreduce

import (
	"os"
	"fmt"
	"encoding/json"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, 	// the name of the whole MapReduce job
	reduceTaskNumber int, 	// which reduce task this is
	nMap int, 		// the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {


	kvs := make(map[string]([]string),0)

	for i := 0; i< nMap; i++ {

		name := reduceName(jobName,i,reduceTaskNumber)
		fmt.Println("doReduce : read %s\n",name)
		file,err := os.Open(name)
		if err != nil {
			fmt.Println("doMap : ",err)
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = decoder.Decode(&kv)
			if err != nil {
				break
			}
			_,ok := kvs[kv.Key]
			if !ok {
				kvs[kv.Key] = make([]string,0)
			}
			kvs[kv.Key] = append(kvs[kv.Key],kv.Value)
		}

	}

	var keys []string
	for k,_ := range kvs {
		keys = append(keys,k)
	}
	sort.Strings(keys)

	fileName := mergeName(jobName,reduceTaskNumber)
	err := ifExistRemoveFile(fileName)
	if err != nil {
		fmt.Println("remove file error")
		return
	}
	file,err :=createOrOpenFile(fileName)
	if err != nil {
		fmt.Println("file create or open file error")
		return
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	for _,k := range keys {
		outValue := reduceF(k,kvs[k])
		enc.Encode(KeyValue{k,outValue})
	}

	//partFileNames := make([]string,0)
	//for i:=0 ; i < nMap; i++ {
	//	partFileName := reduceName(jobName,i,reduceTaskNumber)
	//	partFileNames = append(partFileNames,partFileName)
	//}
	//
	//kvs := mergeWithPartFile(partFileNames)
	//outKeyValues := make([]KeyValue,0)
	//
	//for k,v := range kvs {
	//	outValue := reduceF(k,v)
	//	outKeyValue := &KeyValue{Key:k,Value:outValue}
	//	outKeyValues = append(outKeyValues,*outKeyValue)
	//}
	//
	//fileName := mergeName(jobName,reduceTaskNumber)
	//writePartOut2Disk(outKeyValues,fileName)

	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()


}

//func writePartOut2Disk(kvs []KeyValue,fileName string ){
//
//	err := ifExistRemoveFile(fileName)
//	if err != nil {
//		fmt.Println("remove file error")
//		return
//	}
//
//	file,err :=createOrOpenFile(fileName)
//	if err != nil {
//		fmt.Println("file create or open file error")
//		return
//	}
//
//	defer file.Close()
//	//for i,v := range kvs{
//	//	var line string
//	//	if i != len(kvs)-1 {
//	//		line = v.Key + ": " + v.Value + "\n"
//	//	} else {
//	//		line = v.Key + ": " + v.Value
//	//	}
//	//	file.WriteString(line)
//	//}
//
//	enc := json.NewEncoder(file)
//	for _,kv := range kvs {
//		err := enc.Encode(&kv)
//		if err != nil {
//			fmt.Println("serializing data error", err)
//			return
//		}
//	}
//
//}


//func mergeWithPartFile(partFileNames []string)	map[string]([]string){
//
//	kvs := make(map[string]([]string),0)
//
//	for i:=0 ; i<len(partFileNames) ; i++ {
//
//		file,err := os.Open(partFileNames[i])
//		if err != nil {
//			fmt.Println("open file error")
//			return nil
//		}
//
//		defer file.Close()
//		decoder := json.NewDecoder(file)
//		for {
//			var kv KeyValue
//			err := decoder.Decode(&kv)
//			if err != nil {
//				break;
//			}
//			values, exists := kvs[kv.Key]
//			if exists {
//				kvs[kv.Key] = append(values, kv.Value)
//			} else {
//				kvs[kv.Key] = make([]string, 0)
//			}
//		}
//	}
//
//	return kvs
//}
