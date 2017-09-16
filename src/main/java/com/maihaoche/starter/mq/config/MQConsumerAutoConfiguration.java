package com.maihaoche.starter.mq.config;

import com.maihaoche.starter.mq.annotation.MQConsumer;
import com.maihaoche.starter.mq.base.AbstractMQConsumer;
import com.maihaoche.starter.mq.base.AbstractMQPullConsumer;
import com.maihaoche.starter.mq.base.AbstractMQPushConsumer;
import java.lang.reflect.Field;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by suclogger on 2017/6/28.
 * 自动装配消息消费者
 */
@Slf4j
@Configuration
@ConditionalOnBean(MQBaseAutoConfiguration.class)
public class MQConsumerAutoConfiguration extends MQBaseAutoConfiguration {
    @PostConstruct
    public void init() throws Exception {
        Map<String, Object> beans = applicationContext.getBeansWithAnnotation(MQConsumer.class);
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            publishConsumer(entry.getKey(), entry.getValue());
        }
    }

    private void publishConsumer(String beanName, Object bean) throws Exception {
        MQConsumer mqConsumer = applicationContext.findAnnotationOnBean(beanName, MQConsumer.class);
        if(StringUtils.isEmpty(mqProperties.getNameServerAddress())) {
            throw new RuntimeException("name server address must be defined");
        }
        final Field consumerGroupField = FieldUtils.getDeclaredField(bean.getClass(), "consumerGroup", true);
        final Field topicField = FieldUtils.getDeclaredField(bean.getClass(), "topic", true);
        String consumerGroup = null,topic = null;
        //优先使用@MQConsumer注解定义的consumerGroup值
        if(StringUtils.isBlank(consumerGroup = mqConsumer.consumerGroup())) {
            //再次使用AbstractMQPullConsumer,AbstractMQPushConsumer子类实例的consumerGroup字段值
            if(null != consumerGroupField){
                if(!String.class.isAssignableFrom(consumerGroupField.getType())){
                    throw new RuntimeException("consumer's field which named consumerGroup must be String");
                }
                consumerGroup = (String)consumerGroupField.get(bean);
                if(StringUtils.isBlank(consumerGroup)){
                    throw new RuntimeException("consumer's consumerGroup field value can not be blank");
                }
            }
        }
        if(StringUtils.isBlank(topic = mqConsumer.topic())) {
            //再次使用AbstractMQPullConsumer,AbstractMQPushConsumer子类实例的consumerGroup字段值
            if(null != topicField){
                if(!String.class.isAssignableFrom(topicField.getType())){
                    throw new RuntimeException("consumer's field which named topic must be String");
                }
                topic = (String)topicField.get(bean);
                if(StringUtils.isBlank(topic)){
                    throw new RuntimeException("consumer's topic field value can not be blank");
                }
            }
        }
        if(!AbstractMQPushConsumer.class.isAssignableFrom(bean.getClass())
                && !AbstractMQPullConsumer.class.isAssignableFrom(bean.getClass())) {
            throw new RuntimeException(bean.getClass().getName() + " - consumer未实现AbstractMQPushConsumer或AbstractMQPullConsumer抽象类");
        }
        //最后从环境变量获取消费者的消费组以及主题的配置
        if(StringUtils.isBlank(consumerGroup)) {
            consumerGroup = applicationContext.getEnvironment()
                .getProperty(mqConsumer.consumerGroup());
        }
        if(StringUtils.isBlank(topic)) {
            topic = applicationContext.getEnvironment().getProperty(mqConsumer.topic());
        }

        if(StringUtils.isBlank(consumerGroup)) {
            throw new RuntimeException("consumer's consumerGroup not defined please check,hint:you can define it in @MQConsumer annotation or use field which named consumerGroup");
        }

        if(StringUtils.isBlank(topic)) {
            throw new RuntimeException("consumer's topic not defined please check,hint:you can define it in @MQConsumer annotation or use field which named topic");
        }

        // 配置push consumer
        if(AbstractMQPushConsumer.class.isAssignableFrom(bean.getClass())) {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
            consumer.setNamesrvAddr(mqProperties.getNameServerAddress());
            consumer.setMessageModel(MessageModel.valueOf(mqConsumer.messageMode()));
            consumer.subscribe(topic, StringUtils.join(mqConsumer.tag(),"||"));
            consumer.setInstanceName(UUID.randomUUID().toString());
            AbstractMQPushConsumer abstractMQPushConsumer = (AbstractMQPushConsumer) bean;
            if(mqConsumer.consumeMode().equals("CONCURRENTLY")) {
                consumer.registerMessageListener((List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) ->
                        abstractMQPushConsumer.dealMessage(list, consumeConcurrentlyContext));
            } else if(mqConsumer.consumeMode().equals("ORDERLY")) {
                consumer.registerMessageListener((List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) ->
                        abstractMQPushConsumer.dealMessage(list, consumeOrderlyContext));
            } else {
                throw new RuntimeException("unknown consume mode ! only support CONCURRENTLY and ORDERLY");
            }
            abstractMQPushConsumer.setConsumer(consumer);
            consumer.start();
        } else if (AbstractMQPullConsumer.class.isAssignableFrom(bean.getClass())) {

            // 配置pull consumer

            AbstractMQPullConsumer abstractMQPullConsumer = AbstractMQPullConsumer.class.cast(bean);

            DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(consumerGroup);
            consumer.setNamesrvAddr(mqProperties.getNameServerAddress());
            consumer.setMessageModel(MessageModel.valueOf(mqConsumer.messageMode()));
            consumer.setInstanceName(UUID.randomUUID().toString());
            consumer.start();

            abstractMQPullConsumer.setTopic(topic);
            abstractMQPullConsumer.setConsumer(consumer);
            abstractMQPullConsumer.startInner();
        }

        log.info(String.format("%s is ready to subscribe message", bean.getClass().getName()));
    }
}