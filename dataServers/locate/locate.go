package locate

import (
	"Distributed-object-storage-golang/rabbitmq"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// 对象是否存在
var objects = make(map[string]struct{})
var mutex sync.Mutex

// 检测文件是否存在
func Locate(hash string) bool {
	mutex.Lock()
	_, ok := objects[hash]
	mutex.Unlock()
	return ok
}

func Add(hash string) {
	mutex.Lock()
	objects[hash] = struct{}{}
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
		if Locate(hash) {
			mq.Send(msg.ReplyTo, os.Getenv("LISTEN_ADDRESS"))
		}
	}
}

// 每次启动只运行一次
// 从磁盘中读取文件名，添加到objects中
func CollectObjects() {
	files, _ := filepath.Glob(os.Getenv("STORAGE_ROOT") + "/objects/*")
	for i := range files {
		hash := filepath.Base(files[i])
		objects[hash] = struct{}{}
	}
}
