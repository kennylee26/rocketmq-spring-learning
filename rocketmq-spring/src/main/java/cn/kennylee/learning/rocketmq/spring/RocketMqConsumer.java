package cn.kennylee.learning.rocketmq.spring;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.util.Objects;
import java.util.UUID;

/**
 * <p> 消费者抽象类 </p>
 * <p>Created on 13/2/2020.</p>
 *
 * @author kennylee
 */
@Slf4j
public class RocketMqConsumer implements InitializingBean, DisposableBean {
    @Getter
    private final DefaultMQPushConsumer consumer;

    public RocketMqConsumer(@NonNull String group, @NonNull String nameSrvAddr,
                            @Nullable String instanceName,
                            @NonNull AbstractMessageHandler messageHandler) {
        super();

        DefaultMQPushConsumer o = new DefaultMQPushConsumer(group);
        o.setNamesrvAddr(nameSrvAddr);
        o.setInstanceName(Objects.isNull(instanceName) ? UUID.randomUUID().toString() : instanceName);
        o.setMessageModel(messageHandler.getMessageModel());
        o.setAccessChannel(messageHandler.getAccessChannel());
        o.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        try {
            // 订阅
            o.subscribe(messageHandler.getTopic(), messageHandler.getTags());
        } catch (MQClientException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e.getErrorMessage());
        }

        switch (messageHandler.getConsumeMode()) {
            case CONCURRENTLY:
                o.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {

                    for (MessageExt messageExt : msgs) {
                        if (log.isDebugEnabled()) {
                            log.debug("consume {} , topic {}, tags {}, keys {}\n>>> body -> \n {} ", messageExt.getMsgId(),
                                    messageExt.getTopic(), messageExt.getTags(),
                                    messageExt.getKeys(),
                                    new String(messageExt.getBody(), RocketMqProducer.DEFAULT_CHARSET));
                        }

                        try {
                            messageHandler.onMessage(messageExt);
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                            context.setDelayLevelWhenNextConsume(messageHandler.getDelayLevelWhenNextConsume());
                            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        }
                    }

                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                });
                break;
            case ORDERLY:
                o.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
                    for (MessageExt messageExt : msgs) {
                        if (log.isDebugEnabled()) {
                            log.debug("consume {} , topic {}, tags {}, keys {}\n>>> body -> \n {} ", messageExt.getMsgId(),
                                    messageExt.getTopic(), messageExt.getTags(),
                                    messageExt.getKeys(),
                                    new String(messageExt.getBody(), RocketMqProducer.DEFAULT_CHARSET));
                        }

                        try {
                            messageHandler.onMessage(messageExt);
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                            context.setSuspendCurrentQueueTimeMillis(messageHandler.getSuspendCurrentQueueTimeMillis());
                            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                        }
                    }

                    return ConsumeOrderlyStatus.SUCCESS;
                });
                break;
            default:
                throw new UnsupportedOperationException("unsupported yet!");
        }

        this.consumer = o;
    }

    public RocketMqConsumer(@NonNull String producerGroup, @NonNull String nameSrvAddr,
                            @NonNull AbstractMessageHandler messageHandler) {
        this(producerGroup, nameSrvAddr, null, messageHandler);
    }

    @Override
    public void destroy() {
        if (Objects.nonNull(this.consumer)) {
            this.consumer.shutdown();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (Objects.nonNull(this.consumer)) {
            this.consumer.start();
        }
    }
}
