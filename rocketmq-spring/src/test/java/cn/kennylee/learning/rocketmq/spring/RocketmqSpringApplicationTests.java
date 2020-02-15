package cn.kennylee.learning.rocketmq.spring;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
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
     */
    @RepeatedTest(5)
    public void testRocketMqProducer_oneWay() throws Exception {
        final String key = UUID.randomUUID().toString();
        final String payload = "Hello World@" + new Date().getTime();

        this.rocketMqProducer.sendOneWay(
                rocketMqClientProperties.getRocketmq().getTopic(),
                RocketmqSpringApplication.TAGS_1,
                key, payload);
        assertMatch(key, payload);
    }

    /**
     * 测试发送并验证结果和测试接收。
     */
    @RepeatedTest(5)
    public void testRocketMqProducer_syncSend() throws Exception {
        final String key = UUID.randomUUID().toString();
        final String payload = "Hello World@" + new Date().getTime();

        SendResult result = this.rocketMqProducer.syncSend(
                rocketMqClientProperties.getRocketmq().getTopic(),
                RocketmqSpringApplication.TAGS_1,
                key, payload);
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

}
