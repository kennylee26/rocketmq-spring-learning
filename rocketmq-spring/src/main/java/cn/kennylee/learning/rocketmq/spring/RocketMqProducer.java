package cn.kennylee.learning.rocketmq.spring;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * <p> Producer工具类 </p>
 * <p>Created on 13/2/2020.</p>
 *
 * @author kennylee
 */
@Slf4j
public class RocketMqProducer implements InitializingBean, DisposableBean {
    @Getter
    private final DefaultMQProducer producer;
    static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    /**
     * @param producerGroup 生产者组名
     * @param nameSrvAddr   mq服务发现服务器地址
     * @param instanceName  可为null，会生成UUID作为instanceNaME
     */
    public RocketMqProducer(@NonNull String producerGroup, @NonNull String nameSrvAddr, @Nullable String instanceName) {
        super();

        DefaultMQProducer o = new DefaultMQProducer(producerGroup);
        o.setInstanceName(Objects.isNull(instanceName) ? UUID.randomUUID().toString() : instanceName);
        o.setNamesrvAddr(nameSrvAddr);

        o.setRetryTimesWhenSendFailed(2);
        o.setRetryTimesWhenSendAsyncFailed(2);
        // 信息发送默认超时
        o.setSendMsgTimeout(3000);

        this.producer = o;
    }

    public RocketMqProducer(@NonNull String producerGroup, @NonNull String nameSrvAddr) {
        this(producerGroup, nameSrvAddr, null);
    }

    /**
     * <p>同步发送消息</p>
     *
     * @param topic   一级分类
     * @param tags    二级分类
     * @param keys    主键
     * @param payload 信息
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @NonNull
    public SendResult syncSend(@NonNull final String topic, @NonNull final String tags, @NonNull final String keys, @NonNull final String payload)
            throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        final Message message = new Message(topic, tags, keys, payload.getBytes(DEFAULT_CHARSET));
        return this.getProducer().send(message);
    }

    /**
     * <p>发送即发即失消息（不关心发送结果）</p>
     *
     * @param topic   一级分类
     * @param tags    二级分类
     * @param keys    主键
     * @param payload 信息
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    public void sendOneWay(@NonNull final String topic, @NonNull final String tags,
                           @NonNull final String keys, @NonNull final String payload)
            throws InterruptedException, RemotingException, MQClientException {
        final Message message = new Message(topic, tags, keys, payload.getBytes(DEFAULT_CHARSET));
        this.getProducer().sendOneway(message);
    }

    /**
     * <p>异步发送消息</p>
     *
     * @param topic        一级分类
     * @param tags         二级分类
     * @param keys         主键
     * @param payload      信息
     * @param sendCallback 异步回调事件
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    public void asyncSend(@NonNull final String topic, @NonNull final String tags,
                          @NonNull final String keys, @NonNull final String payload, @NonNull SendCallback sendCallback)
            throws RemotingException, MQClientException, InterruptedException {
        final Message message = new Message(topic, tags, keys, payload.getBytes(DEFAULT_CHARSET));
        this.getProducer().send(message, sendCallback);
    }

    @Override
    public void destroy() {
        if (Objects.nonNull(this.producer)) {
            this.producer.shutdown();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (Objects.nonNull(this.producer)) {
            this.producer.start();
        }
    }
}
