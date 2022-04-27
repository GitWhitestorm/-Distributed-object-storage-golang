package locate

import (
	"Distributed-object-storage-golang/rabbitmq"
	"os"
	"strconv"
)

// 检测文件是否存在
func Locate(name string) bool {
	_, err := os.Stat(name)

	return !os.IsNotExist(err)
}

func StratLocate() {
	mq := rabbitmq.New(os.Getenv("RABBITMQ_SERVER"))
	defer mq.Close()

	// 绑定数据服务消息队列
	mq.Bind("dataServers")

	ch := mq.Consume()
	// 判断访问的对象是否存在
	for msg := range ch {
		object, err := strconv.Unquote(string(msg.Body))
		if err != nil {
			panic(err)
		}
		// 如果存在，则发送该服务地址
		if Locate(os.Getenv("STORAGE_ROOT") + "/objects/" + object) {
			mq.Send(msg.ReplyTo, os.Getenv("LISTEND_ADDRESS"))
		}
	}
}
