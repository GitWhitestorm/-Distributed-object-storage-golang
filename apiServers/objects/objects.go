package objects

import (
	"Distributed-object-storage-golang/apiServers/heartbeat"
	"Distributed-object-storage-golang/apiServers/locate"
	"Distributed-object-storage-golang/elasticsearch"
	"Distributed-object-storage-golang/rs"
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
	if m == http.MethodPost {
		post(w, r)
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
func putStream(hash string, size int64) (*rs.RSPutStream, error) {

	// 选取随机一组在线服务节点
	server := heartbeat.ChooseRandomDataServer(rs.ALL_SHARDS, nil)
	// 没有足够的服务节点可用
	if len(server) != rs.ALL_SHARDS {
		return nil, fmt.Errorf("cannot find enough dataServer")
	}

	return rs.NewRSPutStream(server, hash, size)
}

func get(w http.ResponseWriter, r *http.Request) {
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	versionId := r.URL.Query()["version"]
	version := 0
	var err error
	if len(versionId) != 0 {
		version, err = strconv.Atoi(versionId[0])
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}
	meta, err := elasticsearch.GetMetadata(name, version)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if meta.Hash == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	hash := url.PathEscape(meta.Hash)
	stream, err := getStream(hash, meta.Size)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	offset := utils.GetOffsetFromHeader(r.Header)
	if offset != 0 {
		stream.Seek(offset, io.SeekCurrent)
		w.Header().Set("content-range", fmt.Sprintf("bytes%d-%d/%d", offset, meta.Size-1, meta.Size))

	}
	io.Copy(w, stream)
	stream.Close()
}
func getStream(hash string, size int64) (*rs.RSGetStream, error) {
	locateInfo := locate.Locate(hash)
	if len(locateInfo) < rs.DATA_SHARDS {
		return nil, fmt.Errorf("object %s locate fail,result %v", hash, locateInfo)
	}
	dataServers := make([]string, 0)

	if len(locateInfo) != rs.ALL_SHARDS {
		dataServers = heartbeat.ChooseRandomDataServer(rs.ALL_SHARDS-len(locateInfo), locateInfo)
	}

	return rs.NewRsGetStream(locateInfo, dataServers, hash, size)
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

func post(w http.ResponseWriter, r *http.Request) {
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	size, err := strconv.ParseInt(r.Header.Get("size"), 0, 64)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusForbidden)
		return
	}
	hash := utils.GetHashFromHeader(r.Header)
	if hash == "" {
		log.Println("missing object hash in digest header")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if locate.Exist(url.PathEscape(hash)) {
		err := elasticsearch.AddVersion(name, hash, size)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		return
	}
	ds := heartbeat.ChooseRandomDataServer(rs.ALL_SHARDS, nil)
	if len(ds) != rs.ALL_SHARDS {
		log.Println("cannot find enough dataServer")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	stream, err := rs.NewRSResumablePutStream(ds, name, hash, size)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("location", "/temp/"+url.PathEscape(stream.ToToken()))
	w.WriteHeader(http.StatusCreated)

}
