# RocketMQ-Spring-Learning

RocketMQ分别在spring和springboot架构下的实践。

## 说明

两个实践项目虽然都在springboot基础架构之下，区别：

* [rocketmq-spring](./rocketmq-spring): 项目配置与SpringBoot无关，使用[RocketMQ Client](https://mvnrepository.com/artifact/org.apache.rocketmq/rocketmq-client)。
* [rocketmq-springboot](./rocketmq-springboot): 则使用RocketMQ对Springboot依赖支持，使用[rocketmq-spring-boot-starter](https://mvnrepository.com/artifact/org.apache.rocketmq/rocketmq-spring-boot-starter)。

测试

```
sh run-all-tests.sh
```

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
2. 使用`rocketmq-spring-boot-starter`的应用中若消费者出现`groupName`相同的话，会抛出异常异常，消费者的`groupName`必须不同。而仅仅使用`rocketmq-client`的时候，允许消费者使用相同的`groupName`，大概可直接实现负载均衡(嗯，未测试呢)。

## 参考

* [rocketmq-spring-boot-samples](https://github.com/apache/rocketmq-spring/blob/master/rocketmq-spring-boot-samples)


