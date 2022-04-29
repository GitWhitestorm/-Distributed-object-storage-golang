package objects

import (
	"Distributed-object-storage-golang/apiServers/heartbeat"
	"Distributed-object-storage-golang/apiServers/locate"
	"Distributed-object-storage-golang/apiServers/objectstream"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	m := r.Method
	if m == http.MethodGet {
		get(w, r)
		return
	}
	if m == http.MethodPut {
		put(w, r)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func put(w http.ResponseWriter, r *http.Request) {
	object := strings.Split(r.URL.EscapedPath(), "/")[2]
	code, err := storeObject(r.Body, object)

	if err != nil {
		log.Println(err)
	}

	w.WriteHeader(code)

}

// 存储对象，第一个参数为要存储的对象，第二个参数为对象的名字
func storeObject(r io.Reader, object string) (int, error) {
	stream, err := putStream(object)

	if err != nil {
		return http.StatusServiceUnavailable, err
	}

	// 复制
	io.Copy(stream, r)

	err = stream.Close()

	if err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

// 以object为参数，返回PutStream对象
func putStream(object string) (*objectstream.PutStream, error) {

	// 选取随机一个在线服务节点
	server := heartbeat.ChooseRandomDataServer()
	// 没有服务节点可用
	if server == "" {
		return nil, fmt.Errorf("cannot find any dataServer")
	}

	return objectstream.NewPutStream(server, object), nil
}

func get(w http.ResponseWriter, r *http.Request) {
	object := strings.Split(r.URL.EscapedPath(), "/")[2]

	stream, err := getStream(object)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	io.Copy(w, stream)
}
func getStream(object string) (io.Reader, error) {
	server := locate.Locate(object)

	if server == "" {
		return nil, fmt.Errorf("object %s locate fail", object)
	}

	return objectstream.NewGetStream(server, object)
}
