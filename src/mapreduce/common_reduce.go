package mapreduce

import (
	"os"
	"fmt"
	"encoding/json"
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


	partFileNames := make([]string,0)
	for i:=0 ; i < nMap; i++ {
		partFileName := reduceName(jobName,i,reduceTaskNumber)
		partFileNames = append(partFileNames,partFileName)
	}


	kvs := mergeWithPartFile(partFileNames)
	outKeyValues := make([]KeyValue,0)

	for k,v := range kvs {
		outkey := reduceF(k,v)
		outKeyValue := &KeyValue{Key:outkey}
		outKeyValues = append(outKeyValues,*outKeyValue)
	}

	fileName := mergeName(jobName,reduceTaskNumber)
	writePartOut2Disk(outKeyValues,fileName)

	//for i:=0 ; i < nMap; i++ {
	//	mapFileName := "mrtmp.map-res-" + strconv.Itoa(i)
	//	runReduceWithFile(mapFileName,reduceFileName,reduceF)
	//}

	//outs := make(map[string]([]string),0)
	//
	//file,err := os.Open("data/intermediate_datasets")
	//if err != nil {
	//	fmt.Println("read file error")
	//	return
	//}
	//defer file.Close()
	//
	//buf := bufio.NewReader(file)
	//
	//for ; ;  {
	//	line,err := buf.ReadString('\n')
	//
	//	if err != nil {
	//		if err == io.EOF {
	//			break
	//		}
	//		fmt.Println("read line from file error",err)
	//	}
	//
	//	keyValues := strings.Split(line,".")
	//	for _,v := range keyValues{
	//		keyValue := strings.Split(v,",")
	//		var key,value string
	//
	//		if len(keyValue) == 1 {
	//			key = keyValue[0]
	//			value = ""
	//		} else {
	//			key = keyValue[0]
	//			value = keyValue[1]
	//		}
	//
	//		values,exists := outs[key]
	//		if exists {
	//			outs[key]=append(values,value)
	//		} else {
	//			outs[key] = make([]string,0)
	//		}
	//	}
	//}
	//outKeyValues := make(map[string]string,0)
	//for key,value := range outs{
	//	//TODO outKey,outValue := reduceF(key,value)
	//	outKey := reduceF(key,value)
	//	outKeyValues[outKey]= ""
	//}
	//saveReduceOutPutToDisk(outKeyValues)

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

func writePartOut2Disk(kvs []KeyValue,fileName string ){

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
	for _,kv := range kvs {
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Println("serializing data error", err)
			return
		}
	}

}


func mergeWithPartFile(partFileNames []string)	map[string]([]string){

	kvs := make(map[string]([]string),0)

	for i:=0 ; i<len(partFileNames) ; i++ {

		file,err := os.Open(partFileNames[i])
		if err != nil {
			fmt.Println("open file error")
			return nil
		}

		defer file.Close()
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {
				break;
			}
			values, exists := kvs[kv.Key]
			if exists {
				kvs[kv.Key] = append(values, kv.Value)
			} else {
				kvs[kv.Key] = make([]string, 0)
			}
		}
	}

	return kvs
}

//func readFromMapOutFile(partFileName string) []KeyValue{
//
//	outs := make(map[string]([]string),0)
//
//	file,err := os.Open(partFileName)
//	if err != nil {
//		fmt.Println("open file error")
//		return
//	}
//
//	defer file.Close()
//	decoder := json.NewDecoder(file)
//	for {
//		var kv KeyValue
//		err := decoder.Decode(&kv)
//		if err != nil {
//			break;
//		}
//		values,exists := outs[kv.Key]
//		if exists {
//			outs[kv.Key]=append(values,kv.Value)
//		} else {
//			outs[kv.Key] = make([]string,0)
//		}
//	}
//
//	reduceRes := make([]KeyValue,0)
//	for key,value := range outs {
//		key := reduceF(key,value)
//		kvRes := &KeyValue{Key:key,Value:""}
//		reduceRes = append(reduceRes,*kvRes)
//	}
//
//}

//func mergeAndSortData(partDatas map[string]([]KeyValue)) ([]KeyValue){
//
//}

func runReduceWithFile(mapFileName string , reduceFilename string ,
			reduceF func(key string, values []string) string){

	outs := make(map[string]([]string),0)

	file,err := os.Open(mapFileName)
	if err != nil {
		fmt.Println("open file error")
		return
	}

	defer file.Close()
	decoder := json.NewDecoder(file)
	for {
		var kv KeyValue
		err := decoder.Decode(&kv)
		if err != nil {
			break;
		}
		values,exists := outs[kv.Key]
		if exists {
			outs[kv.Key]=append(values,kv.Value)
		} else {
			outs[kv.Key] = make([]string,0)
		}
	}

	reduceRes := make([]KeyValue,0)
	for key,value := range outs {
		key := reduceF(key,value)
		kvRes := &KeyValue{Key:key,Value:""}
		reduceRes = append(reduceRes,*kvRes)
	}

	//serializingToDisk(reduceFilename,reduceRes)

	//	outKey := reduceF(key,value)
	//	outKeyValues[outKey]= ""
	//}

	//enc := json.NewEncoder(mergeFile)

		// for key in ... {
		// 	enc.Encode(KeyValue{key, reduceF(...)})
		// }
		// file.Close()


}


func saveReduceOutPutToDisk(outKeyValues map[string]string) error{

	filename := "data/out_put"
	err := ifExistRemoveFile(filename)
	if err != nil {
		fmt.Println("remove file error")
		return err
	}

	file,err :=createOrOpenFile(filename)
	if err != nil {
		fmt.Println("file create or open file error")
		return err
	}

	defer file.Close()

	for key,value := range outKeyValues {
		file.WriteString(key + ",")
		file.WriteString(value + ".\n")
	}
	return nil

}