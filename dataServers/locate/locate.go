package locate

import (
	"Distributed-object-storage-golang/rabbitmq"
	"Distributed-object-storage-golang/types"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// 对象是否存在
var objects = make(map[string]int)
var mutex sync.Mutex

// 检测文件是否存在,若存在返回分片id
func Locate(hash string) int {
	mutex.Lock()
	id, ok := objects[hash]
	mutex.Unlock()
	if !ok {
		return -1
	}
	return id
}

func Add(hash string, id int) {
	mutex.Lock()
	objects[hash] = id
	mutex.Unlock()
}
func Delete(hash string) {
	mutex.Lock()
	delete(objects, hash)
	mutex.Unlock()
}

func StratLocate() {
	mq := rabbitmq.New(os.Getenv("RABBITMQ_SERVER"))
	defer mq.Close()

	// 绑定数据服务消息队列
	mq.Bind("dataServers")

	ch := mq.Consume()
	// 判断访问的对象是否存在
	for msg := range ch {
		hash, err := strconv.Unquote(string(msg.Body))
		if err != nil {
			panic(err)
		}
		id := Locate(hash)
		if id != -1 {
			mq.Send(msg.ReplyTo,types.LocateMessage{Addr: os.Getenv("LISTEN_ADDRESS"),Id: id})

		}
	}
}

// 每次启动只运行一次
// 从磁盘中读取文件名，添加到objects中
func CollectObjects() {
	files, _ := filepath.Glob(os.Getenv("STORAGE_ROOT") + "/objects/*")
	for i := range files {
		file := strings.Split(filepath.Base(files[i]),".")
		if len(file) != 3{
			panic(files[i])
		}
		hash := file[0]
		id,err := strconv.Atoi(file[1])
		if err != nil{
			panic(err)
		}
		objects[hash] = id
	}
}
