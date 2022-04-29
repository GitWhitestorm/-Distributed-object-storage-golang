# Distributed-object-storage-golang
## 单机版存储服务架构图

![image-20220427214141254](https://raw.githubusercontent.com/GitWhitestorm/blog-image/master/img/image-20220427214141254.png)

## 分布式存储服务

<iframe   src="https://carbon.now.sh/embed?bg=rgba%28171%2C+184%2C+195%2C+1%29&t=seti&wt=none&l=text%2Fx-go&width=680&ds=true&dsyoff=20px&dsblur=68px&wc=true&wa=true&pv=56px&ph=56px&ln=false&fl=1&fm=Hack&fs=14px&lh=133%25&si=false&es=2x&wm=false&code=const%2520pluckDeep%2520%253D%2520key%2520%253D%253E%2520obj%2520%253D%253E%2520key.split%28%27.%27%29.reduce%28%28accum%252C%2520key%29%2520%253D%253E%2520accum%255Bkey%255D%252C%2520obj%29%250A%250Aconst%2520compose%2520%253D%2520%28...fns%29%2520%253D%253E%2520res%2520%253D%253E%2520fns.reduce%28%28accum%252C%2520next%29%2520%253D%253E%2520next%28accum%29%252C%2520res%29%250A%250Aconst%2520unfold%2520%253D%2520%28f%252C%2520seed%29%2520%253D%253E%2520%257B%250A%2520%2520const%2520go%2520%253D%2520%28f%252C%2520seed%252C%2520acc%29%2520%253D%253E%2520%257B%250A%2520%2520%2520%2520const%2520res%2520%253D%2520f%28seed%29%250A%2520%2520%2520%2520return%2520res%2520%253F%2520go%28f%252C%2520res%255B1%255D%252C%2520acc.concat%28%255Bres%255B0%255D%255D%29%29%2520%253A%2520acc%250A%2520%2520%257D%250A%2520%2520return%2520go%28f%252C%2520seed%252C%2520%255B%255D%29%250A%257D"   style="width: 875px; height: 391px; border:0; transform: scale(1); overflow:hidden;"   sandbox="allow-scripts allow-same-origin"> </iframe>

