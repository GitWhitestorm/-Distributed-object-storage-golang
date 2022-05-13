package objects

import (
	"Distributed-object-storage-golang/apiServers/heartbeat"
	"Distributed-object-storage-golang/apiServers/locate"
	"Distributed-object-storage-golang/apiServers/objectstream"
	"Distributed-object-storage-golang/elasticsearch"
	"Distributed-object-storage-golang/utils"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
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

	if m == http.MethodDelete {
		del(w, r)
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func put(w http.ResponseWriter, r *http.Request) {
	hash := utils.GetHashFromHeader(r.Header)

	if hash == "" {
		log.Println("missing object hash in digest header")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// 存储对象
	code, err := storeObject(r.Body, url.PathEscape(hash))

	if err != nil {
		log.Println(err)
		w.WriteHeader(code)
		return
	}

	if code != http.StatusOK {
		w.WriteHeader(code)
		return
	}

	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	size := utils.GetSizeFromHeader(r.Header)
	log.Println("objectsEnv:" + os.Getenv("ES_SERVER"))
	// 在es中存储对象元数据
	err = elasticsearch.AddVersion(name, hash, size)

	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
	}

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
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	versionId := r.URL.Query()["version"]
	version := 0

	var err error
	if len(versionId) != 0 {
		// 获取版本号
		version, err = strconv.Atoi(versionId[0])

		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	// 获取元数据
	metadata, err := elasticsearch.GetMetadata(name, version)

	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if metadata.Hash == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	object := url.PathEscape(metadata.Hash)
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

func del(w http.ResponseWriter, r *http.Request) {
	name := strings.Split(r.URL.EscapedPath(), "/")[2]

	// 获取对象最新版本号
	metadata, err := elasticsearch.SearchLatestVersion(name)

	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// 删除标记
	err = elasticsearch.PutMetadata(name, metadata.Version+1, 0, "")

	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

}
