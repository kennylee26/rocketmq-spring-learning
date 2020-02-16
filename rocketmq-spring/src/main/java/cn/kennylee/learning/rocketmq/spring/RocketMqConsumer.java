package cn.kennylee.learning.rocketmq.spring;

import com.google.gson.Gson;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;
import java.util.UUID;

/**
 * <p> 消费者抽象类 </p>
 * <p>Created on 13/2/2020.</p>
 *
 * @author kennylee
 */
@Slf4j
public class RocketMqConsumer<T> implements InitializingBean, DisposableBean {
    @Getter
    private final DefaultMQPushConsumer consumer;
    private Type messageType;

    public RocketMqConsumer(@NonNull String group, @NonNull String nameSrvAddr,
                            @Nullable String instanceName,
                            @NonNull AbstractMessageHandler<T> messageHandler) {
        super();
        this.messageType = getGenericType(messageHandler.getClass(), 0);

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
                            messageHandler.onMessage(convertMessage(messageExt));
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
                            messageHandler.onMessage(convertMessage(messageExt));
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

    @Nullable
    @SuppressWarnings("unchecked")
    private static <T> Class<T> getGenericType(Class<?> clazz, int index) {
        Type parameterizedType = clazz.getGenericSuperclass();
        // CGLUB subclass target object(泛型在父类上)
        if (!(parameterizedType instanceof ParameterizedType)) {
            parameterizedType = clazz.getSuperclass().getGenericSuperclass();
        }
        if (!(parameterizedType instanceof ParameterizedType)) {
            return null;
        }
        Type[] actualTypeArguments = ((ParameterizedType) parameterizedType)
                .getActualTypeArguments();
        if (actualTypeArguments == null || actualTypeArguments.length == 0) {
            return null;
        }
        return (Class<T>) actualTypeArguments[index];
    }

    @SuppressWarnings("unchecked")
    private T convertMessage(MessageExt messageExt) {
        log.debug("messageType {}", messageType != null ? messageType.getTypeName() : "null");
        if (this.messageType == null ||
                StringUtils.equals(messageType.getTypeName(), Object.class.getTypeName()) ||
                StringUtils.equals(messageType.getTypeName(), MessageExt.class.getTypeName())) {
            return (T) messageExt;
        } else {
            String str = new String(messageExt.getBody(), RocketMqProducer.DEFAULT_CHARSET);
            if (StringUtils.equals(messageType.getTypeName(), String.class.getTypeName())) {
                return (T) str;
            } else {
                // If msgType not string, use objectMapper change it.
                try {
                    return new Gson().fromJson(str, this.messageType);
                } catch (Exception e) {
                    log.info("convert failed. str:{}, msgType:{}", str, messageType);
                    throw new RuntimeException("cannot convert message to " + messageType, e);
                }
            }
        }
    }

    public RocketMqConsumer(@NonNull String producerGroup, @NonNull String nameSrvAddr,
                            @NonNull AbstractMessageHandler<T> messageHandler) {
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
