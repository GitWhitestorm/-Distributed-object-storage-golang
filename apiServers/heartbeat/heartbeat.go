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

// 随机选择一组节点
// n表示需要随机多少个数据服务节点
// exclude表示返回的随机数据节不能包括哪些节点
// 因为当手机收到的反馈信息不足6个，此时我们需要进行数据修复
// 即根据目前已有的分片，将丢失的分片复原出来并在此上传到数据服务
// 所以，很显然的我们需要将已有的分片所在数据服务节点排除
func ChooseRandomDataServer(n int, exclude map[int]string) (ds []string) {

	candidates := make([]string, 0)
	reverseExcludeMap := make(map[string]int)
	for id, addr := range exclude {
		reverseExcludeMap[addr] = id
	}
	servers := GetDataServers()
	for i := range servers {
		s := servers[i]
		_, excluded := reverseExcludeMap[s]
		if !excluded {
			candidates = append(candidates, s)
		}
	}
	length := len(candidates)
	if length < n {
		return
	}
	p := rand.Perm(length)
	for i := 0; i < n; i++ {
		ds = append(ds, candidates[p[i]])
	}
	return
}
