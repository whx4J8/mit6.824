package mapreduce

import (
	"hash/fnv"
	"fmt"
	"os"
	"bufio"
	"io"
	"bytes"
	"encoding/json"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, 	// map reduce 任务名
	mapTaskNumber int, 	// 任务序号
	inFile string, 		// 文件名
	nReduce int, 		// reduce的任务个数 == 分区个数
	mapF func(file string, contents string) []KeyValue,	//map 函数
) {

	file,err := os.Open(inFile)
	if err != nil{
		fmt.Println("open file error",err)
		return
	}
	defer file.Close()

	buf := bufio.NewReader(file)
	var strBuf bytes.Buffer

	for ; ;  {
		line,err := buf.ReadString('\n')
		if err != nil{
			if err == io.EOF {
				break
			}
			fmt.Println("read line from file error",err)
			return
		}
		strBuf.WriteString(line)
	}
	fileString := strBuf.String()
	keyValues := mapF(inFile,fileString)

	serializingToDisk(keyValues,mapTaskNumber,nReduce,jobName)

	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
}


func serializingToDisk(keyValues []KeyValue,mapTaskNumber,nReduce int,jobName string){

	fileNames := make([]string,0)
	for i := 0; i<nReduce; i++ {

		fileName := reduceName(jobName, mapTaskNumber, i)
		ifExistRemoveFile(fileName)
		file,err := createOrOpenFile(fileName)
		if err != nil {
			fmt.Println("open file error",err)
			return
		}
		defer file.Close()

		enc := json.NewEncoder(file)
		for _,v := range keyValues {
			kv := v
			if iHash(kv.Key) % uint32(nReduce) == uint32(i) {
				err := enc.Encode(&kv)
				if err != nil {
					fmt.Println("doMap : marshall ",err)
				}
			}
		}
		fileNames = append(fileNames,fileName)
	}
	//fmt.Println("doMap : complete write out key values to file " , fileNames)

}

//func serializingToDisk0(fileName string,KeyValues []KeyValue){
//
//	ifExistRemoveFile(fileName)
//	file,err := createOrOpenFile(fileName)
//	if err != nil {
//		fmt.Println("open file error",err)
//		return
//	}
//	defer file.Close()
//
//	enc := json.NewEncoder(file)
//	for _,kv := range KeyValues {
//		err := enc.Encode(&kv)
//		if err != nil {
//			fmt.Println("serializing data error",err)
//			return
//		}
//	}
//}

/**
	如果存在则删除文件
 */
func ifExistRemoveFile(filename string) error{
	if exist(filename) {
		err := os.Remove(filename)
		if err != nil {
			return err
		}
	}
	return nil
}

/**
	创建或打开文件
 */
func createOrOpenFile(filename string) (*os.File,error){
	if exist(filename) {
		file,err := os.Open(filename)
		if err != nil {
			fmt.Println("open file error")
			return nil,err
		}
		return file,err
	} else {
		file,err := os.Create(filename)
		if err != nil {
			fmt.Println("create file error")
			return nil,err
		}
		return file,err
	}

}


func exist(filename string) bool {
	_,err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

func iHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}


