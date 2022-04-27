package heartbeat

import (
	"Distributed-object-storage-golang/rabbitmq"
	"os"
	"time"
)

// 数据服务的心跳服务
func StartHeartbeat() {
	mq := rabbitmq.New(os.Getenv("RABBITMQ_SERVER"))
	defer mq.Close()

	for {
		mq.Publish("apiServers", os.Getenv("LISTEN_ADDRESS"))
		time.Sleep(5 * time.Second)
	}
}
