package cn.kennylee.learning.rocketmq.spring;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import javax.annotation.Resource;
import java.util.Date;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

@Slf4j
@SpringBootTest
class RocketmqSpringApplicationTests {
    private static final long MAX_WAIT = 5000;

    @Resource
    private RocketMqClientProperties rocketMqClientProperties;
    @Resource
    private RocketMqProducer rocketMqProducer;
    private static final Set<MessageExt> consumerMessageExts1 = RocketmqSpringApplication.MESSAGE_EXTS_1;
    private static final Set<MessageExt> consumerMessageExts2 = RocketmqSpringApplication.MESSAGE_EXTS_2;

    @Test
    void contextLoads() {
    }

    /**
     * 测试发送（不管结果）和接收。
     * <p>注：可能由于集群特性，单元测试中的新消费者未必都能马上收到服务器给的新投递消息，所以可能会造成单元测试失败。</p>
     */
    @Test
    public void testRocketMqProducer_oneWay() throws InterruptedException, MQClientException, RemotingException {
        final String key = UUID.randomUUID().toString();
        final String payload = "Hello World@" + new Date().getTime();

        this.rocketMqProducer.sendOneWay(getTopic(), RocketmqSpringApplication.CONSUMER1_TAGS, key, payload);
        assertMatch(key, payload);
    }

    /**
     * 测试发送并验证结果和测试接收。
     * <p>注：可能由于集群特性，单元测试中的新消费者未必都能马上收到服务器给的新投递消息，所以可能会造成单元测试失败。</p>
     */
    @Test
    public void testRocketMqProducer_syncSend() throws InterruptedException, MQClientException, RemotingException, MQBrokerException {
        final String key = UUID.randomUUID().toString();
        final String payload = "Hello World@" + new Date().getTime();

        SendResult result = this.rocketMqProducer.syncSend(getTopic(), RocketmqSpringApplication.CONSUMER1_TAGS, key, payload);
        Assertions.assertEquals(SendStatus.SEND_OK, result.getSendStatus());
        assertMatch(key, payload);
    }

    private static void assertMatch(String key, String payload) throws InterruptedException {
        long count = 0;
        long interval = 500;
        boolean flag = false;
        while (count < MAX_WAIT) {
            if (contains(consumerMessageExts1, key)) {
                flag = true;
                break;
            }
            Thread.sleep(interval);
            count += interval;
        }
        if (contains(consumerMessageExts1, key) && contains(consumerMessageExts2, key)) {
            throw new RuntimeException("consumerMessageExts2 must not has key: " + key);
        }

        Assertions.assertTrue(flag, "cant find key: " + key);

        MessageExt receiveMessage = getByKey(consumerMessageExts1, key);
        Assertions.assertNotNull(receiveMessage);
        Assertions.assertEquals(payload, new String(receiveMessage.getBody(), RocketMqProducer.DEFAULT_CHARSET));
    }

    private static boolean contains(@NonNull Set<MessageExt> consumerMessageContainer, @NonNull String keys) {
        return Objects.nonNull(getByKey(consumerMessageContainer, keys));
    }

    @Nullable
    private static MessageExt getByKey(@NonNull Set<MessageExt> consumerMessageContainer, @NonNull String keys) {
        return consumerMessageContainer.stream()
                .filter(o -> StringUtils.equals(o.getKeys(), keys)
                ).findFirst().orElse(null);
    }

    private String getTopic() {
        return rocketMqClientProperties.getRocketmq().getTopic();
    }

}
