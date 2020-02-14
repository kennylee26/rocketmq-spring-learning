# RocketMQ-Spring-Learning

RocketMQ分别在spring和springboot架构下的实践。

两个实践项目虽然都在springboot基础架构之下，但 `rocketmq-spring` 项目配置与springboot无关，而 `rocketmq-springboot` 则使用RocketMQ对springboot依赖支持。

see also: https://github.com/apache/rocketmq-spring/blob/master/rocketmq-spring-boot-samples

## 快速搭建RocketMQ

安装docker和docker-compose的同学可以直接使用以下命令:

```
# 启动rocketmq容器
sh run-docker-rocketmq.sh
# 清理rocketmq容器
sh clear-docker-rocketmq.sh
```

## 实践总结

1. 若使用docker部署进行部署，请注意网络问题，因为在NameServer返回的RocketMQ访问方式是属于内部的，应用跟docker不处于同一个网络的话，无法进行访问。需要在部署broker的时候指定好 `brokerIP1` 和 `listenPort`。
2. 消费者`未必能“马上”`收到信息，也许是由于其集群特性，所以单元测试中发送后进行收信断言小概率失败。（留坑，未确定原因）

