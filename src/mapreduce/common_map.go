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
	//allKeyValues := make([]KeyValue,0)
	//file,err := os.Open(inFile)
	//defer file.Close()
	//if err != nil {
	//	fmt.Println("open file error",err)
	//	return
	//}
	//
	//buf := bufio.NewReader(file)
	//for i:=0 ; ; i++ {
	//	line,err := buf.ReadString('\n')
	//	if err != nil {
	//		if err == io.EOF {
	//			break
	//		}
	//		fmt.Println("read line from file error",err)
	//	}
	//
	//	keyValues := mapF(inFile,line)
	//	allKeyValues = append(allKeyValues,keyValues...)
	//}
	//
	//saveToDisk(inFile,allKeyValues)

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


func serializingToDisk(keyValues []KeyValue,mapTaskNumber,partition int,jobName string){

	partLength := len(keyValues)/partition
	for j := 0; j < partition; j++ {
		partKeyValues := keyValues[(0+j)*partLength:(1+j)*partLength]
		partFileName := reduceName(jobName, mapTaskNumber, j)
		serializingToDisk0(partFileName,partKeyValues)
	}
}

func serializingToDisk0(fileName string,KeyValues []KeyValue){

	ifExistRemoveFile(fileName)
	file,err := createOrOpenFile(fileName)
	if err != nil {
		fmt.Println("open file error",err)
		return
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	for _,kv := range KeyValues {
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Println("serializing data error",err)
			return
		}
	}
}

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
	保存到硬盘
 */
func saveToDisk(inFile string,keyValues []KeyValue) error{

	filename := "data/intermediate_datasets"
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

	for i,v := range keyValues {
		if i%10 == 0 {
			file.WriteString(v.Key + ",")
			file.WriteString(v.Value + ".\n")
		}else{
			file.WriteString(v.Key + ",")
			file.WriteString(v.Value + ".")
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


