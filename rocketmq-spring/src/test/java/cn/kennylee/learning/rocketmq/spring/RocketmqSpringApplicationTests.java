package cn.kennylee.learning.rocketmq.spring;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.SendCallback;
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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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

    @Test
    void contextLoads() {
    }

    /**
     * 测试发送（不管结果）和接收，信息体Bean。
     */
    @RepeatedTest(5)
    public void testRocketMqProducer_oneWay_bean() throws Exception {
        final String key = UUID.randomUUID().toString();
        final String content = "Hello World@" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("YYYYMMddHHmmss"));

        final RocketmqSpringApplication.OrderInfo payload = RocketmqSpringApplication.OrderInfo.builder()
                .key(key)
                .content(content)
                .build();

        this.rocketMqProducer.sendOneWay(
                rocketMqClientProperties.getRocketmq().getTopic(),
                RocketmqSpringApplication.TAGS_1,
                key, payload);
        assertMatch(RocketmqSpringApplication.MESSAGE_1, key, payload);
    }

    /**
     * 测试发送并验证结果和测试接收，信息体Bean。
     */
    @RepeatedTest(5)
    public void testRocketMqProducer_syncSend_bean() throws Exception {
        final String key = UUID.randomUUID().toString();
        final String content = "Hello World@" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("YYYYMMddHHmmss"));

        final RocketmqSpringApplication.OrderInfo payload = RocketmqSpringApplication.OrderInfo.builder()
                .key(key)
                .content(content)
                .build();

        SendResult result = this.rocketMqProducer.syncSend(
                rocketMqClientProperties.getRocketmq().getTopic(),
                RocketmqSpringApplication.TAGS_1,
                key, payload);

        Assertions.assertEquals(SendStatus.SEND_OK, result.getSendStatus());
        assertMatch(RocketmqSpringApplication.MESSAGE_1, key, payload);
    }

    /**
     * 测试泛型定义信息类型为String的消费者时，接收是否String和内容是否正常。
     */
    @Test
    public void testRocketMqProducer_asyncSend_string() throws Exception {
        final String key = UUID.randomUUID().toString();
        final String content = "Hello World@" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("YYYYMMddHHmmss"));

        final Boolean[] isSuccess = {false};
        final int maxWaitSend = 2000;
        this.rocketMqProducer.asyncSend(
                rocketMqClientProperties.getRocketmq().getTopic(),
                RocketmqSpringApplication.TAGS_2,
                key,
                content, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                            isSuccess[0] = true;
                        } else {
                            Assertions.fail("Fail by SendResult status: " + sendResult.getSendStatus().toString());
                        }
                    }

                    @Override
                    public void onException(Throwable e) {
                        Assertions.fail("Fail by SendResult: " + e.getMessage());
                    }
                });

        int waitCount = 0;
        while (waitCount < maxWaitSend) {
            if (isSuccess[0]) {
                break;
            }
            Thread.sleep(100);
            waitCount += 100;
        }
        Assertions.assertTrue(isSuccess[0]);
        Assertions.assertTrue(RocketmqSpringApplication.MESSAGE_2.stream().anyMatch(s -> StringUtils.equals(content, s)));
    }

    /**
     * 测试未泛型定义信息类型的消费者时，接收是否MessageExt。
     */
    @Test
    public void testRocketMqProducer_syncSend_noGenerics() throws Exception {
        final String key = UUID.randomUUID().toString();
        final String content = "Hello World@" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("YYYYMMddHHmmss"));
        SendResult result = this.rocketMqProducer.syncSend(
                rocketMqClientProperties.getRocketmq().getTopic(),
                RocketmqSpringApplication.TAGS_3,
                key, content);
        Assertions.assertEquals(SendStatus.SEND_OK, result.getSendStatus());
        assertMatch(RocketmqSpringApplication.MESSAGE_3, key, content);
    }

    /**
     * 测试泛型定义信息类型为object的消费者时，接收是否MessageExt。
     */
    @Test
    public void testRocketMqProducer_syncSend_object() throws Exception {
        final String key = UUID.randomUUID().toString();
        final String content = "Hello World@" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("YYYYMMddHHmmss"));
        SendResult result = this.rocketMqProducer.syncSend(
                rocketMqClientProperties.getRocketmq().getTopic(),
                RocketmqSpringApplication.TAGS_4,
                key, content);
        Assertions.assertEquals(SendStatus.SEND_OK, result.getSendStatus());
        assertMatch(RocketmqSpringApplication.MESSAGE_4, key, content);
    }

    private static <T> void assertMatch(Set<?> result, String key, T payload) throws InterruptedException {
        long count = 0;
        long interval = 500;
        boolean flag = false;
        while (count < MAX_WAIT) {
            if (contains(result, key)) {
                flag = true;
                break;
            }
            Thread.sleep(interval);
            count += interval;
        }

        // 若是在检查 message1的回调，添加检查message2也收到同样信息的验证。正确情况下，不应该message2也会有的
        if (!result.isEmpty() && !RocketmqSpringApplication.MESSAGE_1.isEmpty() &&
                Objects.equals(
                        result.stream().findFirst().orElse(null),
                        RocketmqSpringApplication.MESSAGE_1.stream().findFirst().orElse(null))) {
            if (contains(RocketmqSpringApplication.MESSAGE_1, key) && contains(RocketmqSpringApplication.MESSAGE_2, key)) {
                throw new RuntimeException("consumerMessageExts2 must not has key: " + key);
            }
        }

        Assertions.assertTrue(flag, "cant find key: " + key);

        Object receiveMessage = getByKey(result, key);
        Assertions.assertNotNull(receiveMessage);
        if (receiveMessage instanceof RocketmqSpringApplication.OrderInfo) {
            Assertions.assertTrue(payload instanceof RocketmqSpringApplication.OrderInfo);
            RocketmqSpringApplication.OrderInfo o = (RocketmqSpringApplication.OrderInfo) payload;
            Assertions.assertEquals(o.getContent(), ((RocketmqSpringApplication.OrderInfo) receiveMessage).getContent());
        } else if (receiveMessage instanceof MessageExt) {
            Assertions.assertTrue(payload instanceof String);
            String str = (String) payload;
            Assertions.assertEquals(str, new String(((MessageExt) receiveMessage).getBody(), RocketMqProducer.DEFAULT_CHARSET));
        } else {
            Assertions.fail();
        }
    }

    private static <T> boolean contains(@NonNull Set<T> consumerMessageContainer, @NonNull String keys) {
        return Objects.nonNull(getByKey(consumerMessageContainer, keys));
    }

    @Nullable
    private static <T> T getByKey(@NonNull Set<T> consumerMessageContainer, @NonNull String keys) {
        return consumerMessageContainer.stream()
                .filter(o -> {
                            if (o instanceof RocketmqSpringApplication.OrderInfo) {
                                return StringUtils.equals(((RocketmqSpringApplication.OrderInfo) o).getKey(), keys);
                            } else if (o instanceof MessageExt) {
                                return StringUtils.equals(((MessageExt) o).getKeys(), keys);
                            } else {
                                throw new UnsupportedOperationException();
                            }
                        }
                ).findFirst().orElse(null);
    }

}
