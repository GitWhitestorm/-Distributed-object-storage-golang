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

## 数据校验和去重

> 数据校验：客户端发送的数据不一定正确，hash有可能是错误的,所以我们需要验证客户端提供的hash值跟自己根据对象计算出来的hash值是否一定，如不一定则拒绝存储

> 去重：数据在存储之前先定位，如果有，则只需要存储元数据即可，不需要存储实体

### 数据校验的难点

- 由于对象的传输是流传输的，我们可以在数据服务上对整个对象进行完整的校验
- 但由于后期系统的完善，数据服务上存储的对象可能跟用户的上传的对象不相同

### 实现数据校验的方法

- 在数据服务上提供对象的缓存功能
- 在接口服务将对象传输到数据服务时同时计算其散列值
- 对比散列值，如果一致，接口服务需要通知数据服务将临时对象转换为正式对象，不一致则删除

## 数据冗余和即时修复

### RS纠删码码冗余策略

   **编码：**给定n个数据块（Data block）D1、D2……Dn，和一个正整数m，RS根据n个数据块生成m个编码块（Code block），C1、C2……Cm。
    **解码：**对于任意的n和m，从n个原始数据块和m个编码块中任取n块就能解码出原始数据，即RS最多容忍m个数据块或者编码块同时丢失。

### PUT对象

![image-20220528092423634](https://raw.githubusercontent.com/GitWhitestorm/blog-image/master/img/image-20220528092423634.png)

### GET对象

![image-20220528092533534](https://raw.githubusercontent.com/GitWhitestorm/blog-image/master/img/image-20220528092533534.png)

## 断点续传

