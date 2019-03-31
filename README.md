## 项目名：cirrus  
#### 简介：基于zookeeper对微服务进行注册以及对thrift进行上层封装,实现微服务之间rpc调用的分布式框架  
#### 环境要求
* python版本不低于2.7, linux或mac os操作系统尤佳
#### 准备知识
* python2.7基础语法
* thrift
* zookeeper
* 操作系统知识：进程、线程、信号、socket套接字、I/O多路复用epoll等
#### 环境配置
1. 下载后配置python虚拟环境  
2. 安装相关依赖  
   `pip install -r requirements.txt`
#### 项目打包及安装
如果想把该项目安装到python虚拟环境下，可执行如下步骤：  
1. 打包项目  
  `python setup.py bdist_egg`
2. 安装项目  
  `python setup.py install`
#### 项目主要模块
---
##### server端：cirrus_server.py文件CirrusServer类  
CirrusServer类初始化参数如下：  

|参数名|解释|必选|默认值|
|---|---|---|---|
|thrift_module|thrift模块|是|--|
|handler|处理类对象|是|--|
|port|指定进程启动端口，当值为0时则随机选择一个端口|否|0|
|protocol_factory|thrift协议工厂|否|TBinaryProtocolAcceleratedFactory|
|thrift_listen_queue_size|thrift底层socket监听队列大小|否|128|
|worker_process_number|工作进程数，工作进程用于处理客户端请求|否|4|
|harikiri|处理客户端单个请求超时时间(秒)|否|5|
|tag|服务端实例标签，客户端若指定带标签的服务端实例，则会在带指定标签的服务端实例中选择|否|空|
|weight|权重值，存在多个服务的实例时，若客户端采用权重值选择服务实例时，则权重值越大的实例被选中的概率越大|否|100|
|event_queue_size|请求事件缓冲池大小，当有很多个客户端请求过来时，缓冲池的作用时减少服务端处理请求的压力|否|100|
|thrift_recv_timeout|thrift接收数据超时时间(豪秒)|否|50000|

##### client端: cirrus_client.py文件CirrusClient类  
CirrusClient类初始化参数如下：

|参数名|解释|必选|默认值|
|---|---|---|---|
|thrift_module|thrift模块|是|--|
|tag|指定服务端实例标签，若值不为空，则会在带指定标签的服务端实例中选择|否|空|
|pool_size|客户端连接池大小，客户端采用连接池技术，避免大量请求一次性发送到服务端|否|1|
|req_timeout|客户端请求超时时间(毫秒)|否|5000|
|socket_connection_timeout|socket请求连接超时时间(毫秒)|否|1000|
|pool_acquire_client_timeout|连接池获取到连接超时时间(毫秒)|否|1000|
|retry_count|客户端连接服务端尝试次数|否|3|
|protocol_factory|thrift协议工厂|否|TBinaryProtocolAcceleratedFactory|
|use_weight_host_selector|使用权重值选择服务端实例host|否|True|

---

#### 样例测试
1. 进入到test目录下  
   `cd test`
2. 生成thrift代码  
   `thrift --gen py calculator.thrift`
3. 选中生成的gen-py文件夹，右键Make Directory as -> Sources Root  
4. 在根目录下，启动zk watcher实时将zookeeper节点数据复制到本地  
   `python cirrus_zk_watcher.py`
5. 在test目录下，启动服务端(可启动多个服务端)  
   `python mock_server.py`
6. 在test目录下，启动客户端  
   `python mock_client.py`
7. 若需要关闭服务端，Ctrl + C即可  




