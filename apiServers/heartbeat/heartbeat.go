package heartbeat

import (
	"Distributed-object-storage-golang/rabbitmq"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

var dataServers = make(map[string]time.Time)

var mutex sync.Mutex

func ListenHeartbeat() {
	// 创建新的rabbitmq结构体
	mq := rabbitmq.New(os.Getenv("RABBITMQ_SERVER"))
	defer mq.Close()

	// 绑定api交换机
	mq.Bind("apiServers")

	// 消费信息
	ch := mq.Consume()

	go removeExpiredDataServer()

	// 添加节点
	for msg := range ch {
		dataServer, e := strconv.Unquote(string(msg.Body))

		if e != nil {
			panic(e)
		}

		mutex.Lock()
		dataServers[dataServer] = time.Now()
		mutex.Unlock()
	}

}

// 移除超时的数据服务点解
func removeExpiredDataServer() {
	for {
		time.Sleep(5 * time.Second)
		// 加锁，map不支持多线程
		mutex.Lock()

		// 移除超时的节点
		for ser, t := range dataServers {
			if t.Add(10 * time.Second).Before(time.Now()) {
				delete(dataServers, ser)
			}
		}

		mutex.Unlock()
	}
}

// 获取数据服务节点
func GetDataServers() []string {
	mutex.Lock()
	defer mutex.Unlock()

	ds := make([]string, 0)

	for ser := range dataServers {
		ds = append(ds, ser)
	}

	return ds
}

// 随机选择一个节点
func ChooseRandomDataServer() string {
	ds := GetDataServers()
	n := len(ds)
	if n == 0 {
		return ""
	}
	return ds[rand.Intn(n)]
}
