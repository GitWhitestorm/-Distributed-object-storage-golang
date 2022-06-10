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
	url := fmt.Sprintf("http://%s/metadata/_doc/%s_%d/_source",
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

type Bucket struct {
	Key         string
	Doc_count   int
	Min_version struct {
		Value float32
	}
}
type aggregateResult struct {
	Aggregations struct {
		Group_by_name struct {
			Buckets []Bucket
		}
	}
}

func SearchVersionStatus(min_doc_count int) ([]Bucket, error) {
	client := http.Client{}
	url := fmt.Sprintf("http://%s/metadata/_search", os.Getenv("ES_SERVER"))
	body := fmt.Sprintf(`
	{
		"size":0,
		"aggs":{
			"group_by_name":{
				"terms":{
					"field":"name",
					"min_doc_count": %d
				},
				"aggs":{
					"min_version":{
						"min":{
							"field":"version"
						}
					}
				}
			}
		}
	}`, min_doc_count)
	request, _ := http.NewRequest("GET", url, strings.NewReader(body))
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	b, _ := ioutil.ReadAll(response.Body)
	var ar aggregateResult
	json.Unmarshal(b, &ar)
	return ar.Aggregations.Group_by_name.Buckets, nil
}
func DelMetadata(name string, version int) {
	client := http.Client{}
	url := fmt.Sprintf("http://%s/metadata/object/%s_%d", os.Getenv("ES_SERVER"), name, version)
	request, _ := http.NewRequest("DELETE", url, nil)
	client.Do(request)

}
func HasHash(hash string) (bool, error) {
	url := fmt.Sprintf("http://%s/metadata/_search?q=hash:%s&size=0", os.Getenv("ES_SERVER"), hash)
	response, err := http.Get(url)
	if err != nil {
		return false, err
	}
	b, _ := ioutil.ReadAll(response.Body)
	var sr searchResult
	json.Unmarshal(b, &sr)
	return sr.Hits.Total != 0, nil
}
func SearchHashSize(hash string) (size int64, err error) {
	url := fmt.Sprintf("http://%s/metadata/_search?q=hash:%s&size=1", os.Getenv("ES_SERVER"), hash)
	response, err := http.Get(url)
	if err != nil {
		return
	}
	if response.StatusCode != http.StatusOK {
		err = fmt.Errorf("fail to search hash size:%d", response.StatusCode)
		return
	}
	result, _ := ioutil.ReadAll(response.Body)
	var sr searchResult
	json.Unmarshal(result, &sr)
	if len(sr.Hits.Hits) != 0 {
		size = sr.Hits.Hits[0].Source.Size
	}
	return
}
