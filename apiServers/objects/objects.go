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
	size := utils.GetSizeFromHeader(r.Header)

	// 存储对象
	code, err := storeObject(r.Body, url.PathEscape(hash), size)

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

	log.Println("objectsEnv:" + os.Getenv("ES_SERVER"))
	// 在es中存储对象元数据
	err = elasticsearch.AddVersion(name, hash, size)

	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
	}

}

// 存储对象，第一个参数为要存储的对象，第二个参数为对象的名字
func storeObject(r io.Reader, hash string, size int64) (int, error) {
	// 定位object是否存在
	if locate.Exist(url.PathEscape(hash)) {
		return http.StatusOK, nil
	}

	stream, err := putStream(url.PathEscape(hash), size)

	if err != nil {
		return http.StatusServiceUnavailable, err
	}
	// 类型Unix的tee命令，读取reader相当于读取r，同时也写入stream
	reader := io.TeeReader(r, stream)
	d := utils.CalculateHash(reader)
	if d != hash {
		// 取消提交
		stream.Commit(false)
		return http.StatusBadRequest, fmt.Errorf("object hash mismatch calculated=%s,requested=%s", d, hash)
	}
	stream.Commit(true)

	return http.StatusOK, nil
}

// 以object为参数，返回PutStream对象
func putStream(hash string, size int64) (*objectstream.TempPutStream, error) {

	// 选取随机一个在线服务节点
	server := heartbeat.ChooseRandomDataServer()
	// 没有服务节点可用
	if server == "" {
		return nil, fmt.Errorf("cannot find any dataServer")
	}

	return objectstream.NewTempPutStream(server, hash, size)
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
