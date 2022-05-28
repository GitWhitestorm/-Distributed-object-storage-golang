package temp

import (
	"Distributed-object-storage-golang/dataServers/locate"
	"Distributed-object-storage-golang/utils"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	uuid "github.com/satori/go.uuid"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	method := r.Method
	if method == http.MethodPut {
		put(w, r)
		return
	}
	if method == http.MethodPatch {
		patch(w, r)
		return
	}
	if method == http.MethodPost {
		post(w, r)
		return
	}
	if method == http.MethodDelete {
		delete(w, r)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

type tempInfo struct {
	Uuid string
	Name string
	Size int64
}

// 接口服务请求创建临时存储对象
func post(w http.ResponseWriter, r *http.Request) {
	uv4 := uuid.NewV4()
	uuid := uv4.String()
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	size, err := strconv.ParseInt(r.Header.Get("size"), 0, 64)

	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	t := &tempInfo{uuid, name, size}

	err = t.writeToFile()
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	file, err := os.Create(os.Getenv("STORAGE_ROOT") + "/temp/" + t.Uuid + ".dat")
	if err != nil {
		log.Println(err)
		return
	}
	defer file.Close()
	w.Write([]byte(uuid))
}
func (t *tempInfo) writeToFile() error {
	file, err := os.Create(os.Getenv("STORAGE_ROOT") + "/temp/" + t.Uuid)
	if err != nil {
		return err
	}
	defer file.Close()
	b, _ := json.Marshal(t)
	file.Write(b)
	return nil
}

// 从流中追加文件
func patch(w http.ResponseWriter, r *http.Request) {
	uuid := strings.Split(r.URL.EscapedPath(), "/")[2]
	tempinfo, err := readFromFile(uuid)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	infoFile := os.Getenv("STORAGE_ROOT") + "/temp/" + uuid
	datFile := infoFile + ".dat"
	file, err := os.OpenFile(datFile, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer file.Close()

	_, err = io.Copy(file, r.Body)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	info, err := file.Stat()
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	actual := info.Size()
	if actual > tempinfo.Size {
		os.Remove(datFile)
		os.Remove(infoFile)
		log.Println("actual size", actual, "exceeds", tempinfo.Size)
		w.WriteHeader(http.StatusInternalServerError)
	}

}

// 从文件中读取tempinfo结构体
func readFromFile(uuid string) (*tempInfo, error) {
	file, err := os.Open(os.Getenv("STORAGE_ROOT") + "/temp/" + uuid)

	if err != nil {
		return nil, err
	}
	defer file.Close()

	b, _ := ioutil.ReadAll(file)
	var info tempInfo
	json.Unmarshal(b, &info)
	return &info, nil
}

// 将临时文件转正
func put(w http.ResponseWriter, r *http.Request) {
	uuid := strings.Split(r.URL.EscapedPath(), "/")[2]
	tempinfo, err := readFromFile(uuid)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	infoFile := os.Getenv("STORAGE_ROOT") + "/temp/" + uuid
	datFile := infoFile + ".dat"
	file, err := os.Open(datFile)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	info, err := file.Stat()
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	actual := info.Size()
	err = os.Remove(infoFile)
	if err != nil {
		log.Println(err)
	}
	if actual != tempinfo.Size {
		os.Remove(datFile)
		log.Println("actual size mismatch,expect", tempinfo.Size, "actual", actual)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	file.Close()
	commitTempObject(datFile, tempinfo)
}
func (t *tempInfo)hash() string{
	s := strings.Split(t.Name,".")
	return s[0]
}
func (t *tempInfo)id()int{
	s := strings.Split(t.Name,".")
	id, _ := strconv.Atoi(s[1])
	return id
}

func commitTempObject(datFile string, tempinfo *tempInfo) {
	file,_ := os.Open(datFile)
	d := url.PathEscape(utils.CalculateHash(file))
	file.Close()
	err := os.Rename(datFile, os.Getenv("STORAGE_ROOT")+"/objects/"+tempinfo.Name+"."+d)
	if err != nil {
		log.Println(err)
	}
	locate.Add(tempinfo.hash(),tempinfo.id())
}

func delete(w http.ResponseWriter, r *http.Request) {
	uuid := strings.Split(r.URL.EscapedPath(), "/")[2]
	infoFile := os.Getenv("STORAGE_ROOT") + "/temp/" + uuid
	datFile := infoFile + ".dat"
	os.Remove(infoFile)
	os.Remove(datFile)
}
