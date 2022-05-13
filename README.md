# Distributed-object-storage-golang
## 单机版存储服务架构图

![image-20220427214141254](https://raw.githubusercontent.com/GitWhitestorm/blog-image/master/img/image-20220427214141254.png)

## 分布式存储服务


## 元数据服务

### elasticsearch
#### 创建索引

``` http
PUT http://127.0.0.1:9200/metadata/
{
    "mappings": {
        "properties": {
            "name": {
                "type": "text",
                "index": "false"
            },
            "version": {
                "type": "integer"
            },
            "size": {
                "type": "integer"
            },
            "hash": {
                "type": "text"
            }
        }
    }
}
```

#### 添加文档

``` http
POST http://127.0.0.1:9200/metadata/_doc/test_1?op_type=create
{
   "name":"test",
   "version":1,
   "size":0,
   "hash":""
}
```

#### 查找文档

``` http
GET http://127.0.0.1:9200/metadata/_search?&q=name:#{name}&sort=version:desc&from=0&size=2
```

