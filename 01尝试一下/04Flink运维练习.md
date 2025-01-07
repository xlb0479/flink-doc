# Flink运维练习

Flink的部署和运维，在不同环境中有不同的方法。但不论怎么变，一个Flink集群的基本结构是不变的，用的都是相似的运维法则。

## 环境介绍

这次练习要做的包括一个长期运行的[Flink会话集群](../03概念/04术语.md#flink会话集群)和一个Kafka集群。

一个Flink集群包括一个[JobManager](../03概念/04术语.md#jobmangager)和一个或多个[TaskManager](../03概念/04术语.md#taskmanager)。JobManager负责处理[Job](../03概念/04术语.md#flink作业job)的提交、监管以及资源管理。TaskManager则担任工作进程，负责执行真正的[Task](../03概念/04术语.md#task)，Job也就是由Task组成的。本次练习中，一开始你只弄一个TaskManager，但紧接着就会扩展出多个TaskManager。本次练习还会搞一个专门的*客户端*容器，我们一开始会用它来提交Flink Job，后面继续用它来搞各种运维动作。*客户端*容器并不属于Flink集群本身，只是拿来简化操作。

Kafka集群包括一个Zookeeper和一个Kafka Broker。

![01尝试一下-04Flink运维练习-01.png](01尝试一下-04Flink运维练习-01.png)

练习环境启动后，一个名为*Flink Event Count*的Job会提交到JobManager。还会创建两个Kafka Topic，一个*input*，一个*output*。

![01尝试一下-04Flink运维练习-02.png](01尝试一下-04Flink运维练习-02.png)

我们的Job从*input*消费`ClickEvent`，每条事件包含一个`timestamp`和一个`page`。然后这些事件以`page`为key进行分区，并且划出一个15秒的[窗口](../04应用开发/02DataStream%20API/06算子/02窗口.md)。计算结果写出到*output*中。

## 开始操练

这个练习环境几步就可以搭起来。我们带你操作这些命令，还会告诉你如何验证操作的正确性。

我们假设你已经安装了[Docker](https://docs.docker.com/)（1.12+）和[docker-compose](https://docs.docker.com/compose/)（2.1+）。

需要的配置文件都在[flink-playgrounds](https://github.com/apache/flink-playgrounds)这个代码仓库中。首先要把代码拉下来然后构建docker镜像。

```shell
git clone https://github.com/apache/flink-playgrounds.git
cd flink-playgrounds/operations-playground
docker-compose build
```

启动环境：

```shell
docker-compose up -d
```

用下面的命令看一下启动的Docker容器：

```shell
docker-compose ps

                    Name                                  Command               State                   Ports                
-----------------------------------------------------------------------------------------------------------------------------
operations-playground_clickevent-generator_1   /docker-entrypoint.sh java ...   Up       6123/tcp, 8081/tcp                  
operations-playground_client_1                 /docker-entrypoint.sh flin ...   Exit 0                                       
operations-playground_jobmanager_1             /docker-entrypoint.sh jobm ...   Up       6123/tcp, 0.0.0.0:8081->8081/tcp    
operations-playground_kafka_1                  start-kafka.sh                   Up       0.0.0.0:9094->9094/tcp              
operations-playground_taskmanager_1            /docker-entrypoint.sh task ...   Up       6123/tcp, 8081/tcp                  
operations-playground_zookeeper_1              /bin/sh -c /usr/sbin/sshd  ...   Up       2181/tcp, 22/tcp, 2888/tcp, 3888/tcp
```

此时你的客户端容器已经把Job（`Exit 0`）成功提交了，所有集群组件以及数据生成器都起来了（`up`）

执行下面的命令可以停止练习环境：

```shell
docker-compose down -v
```