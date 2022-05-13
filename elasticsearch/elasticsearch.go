package elasticsearch

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
)

type Metadata struct {
	Name    string
	Version int
	Size    int64
	Hash    string
}

// 通过name和verison获取元数据
func getMetedata(name string, versionId int) (meat Metadata, err error) {
	// 通过http访问获取数据
	url := fmt.Sprintf("http://%s/metadata/objects/%s_%d/_source",
		os.Getenv("ES_SERVER"), name, versionId)
	response, err := http.Get(url)
	if err != nil {
		return
	}

	if response.StatusCode != http.StatusOK {
		err = fmt.Errorf("fail to get %s_%d:%d", name, versionId, response.StatusCode)
		return
	}
	// 读取并解析数据
	result, _ := ioutil.ReadAll(response.Body)

	json.Unmarshal(result, &meat)
	return
}

type hit struct {
	Source Metadata `json:"_source"`
}

type searchResult struct {
	Hits struct {
		Total int
		Hits  []hit
	}
}

// 通过名字获取最新版本
func SearchLatestVersion(name string) (meta Metadata, err error) {
	url := fmt.Sprintf("http://%s/metadata/_search?q=name:%s&size=1&sort=version:desc",
		os.Getenv("ES_SERVER"), url.PathEscape(name))

	response, err := http.Get(url)
	if err != nil {
		return
	}
	if response.StatusCode != http.StatusOK {
		err = fmt.Errorf("fail to search latest metadata:%d", response.StatusCode)
		return
	}

	result, _ := ioutil.ReadAll(response.Body)

	var sr searchResult
	json.Unmarshal(result, &sr)

	if len(sr.Hits.Hits) != 0 {
		meta = sr.Hits.Hits[0].Source
	}
	return
}

func GetMetadata(name string, version int) (Metadata, error) {
	if version == 0 {
		return SearchLatestVersion(name)
	}
	return getMetedata(name, version)
}

func PutMetadata(name string, version int, size int64, hash string) error {
	doc := fmt.Sprintf(`{"name":"%s","version":%d,"size":%d,"hash":"%s"}`, name, version, size, hash)

	client := http.Client{}

	url := fmt.Sprintf("http://%s/metadata/_doc/%s_%d?op_type=create", os.Getenv("ES_SERVER"), name, version)

	request, _ := http.NewRequest("POST", url, strings.NewReader(doc))
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)

	if err != nil {
		return err
	}
	// 409 PUT的资源存在冲突，请求无法完成，更新版本
	if response.StatusCode == http.StatusConflict {
		return PutMetadata(name, version+1, size, hash)
	}
	// 不成功
	if response.StatusCode != http.StatusCreated {
		result, _ := ioutil.ReadAll(response.Body)
		return fmt.Errorf("fail to put metadata: %d %s", response.StatusCode, string(result))
	}
	return nil
}

// 在最新的版本+1
func AddVersion(name, hash string, size int64) error {
	log.Println("ESEnv:" + os.Getenv("ES_SERVER"))
	metadata, err := SearchLatestVersion(name)
	if err != nil {
		return err
	}
	return PutMetadata(name, metadata.Version+1, size, hash)
}

func SearchAllVersions(name string, from, size int) ([]Metadata, error) {
	url := fmt.Sprintf("http://%s/metadata/_search?sort=version:desc&from=%d&size=%d", os.Getenv("ES_SERVER"), from, size)

	if name != "" {
		url += "&q=name:" + name
	}

	response, err := http.Get(url)

	if err != nil {
		return nil, err
	}

	metas := make([]Metadata, 0)

	result, _ := ioutil.ReadAll(response.Body)

	var sr searchResult

	json.Unmarshal(result, &sr)

	for i := range sr.Hits.Hits {
		metas = append(metas, sr.Hits.Hits[i].Source)
	}
	return metas, nil
}
