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
        //优先使用@MQConsumer注解定义的consumerGroup值
        String consumerGroup = mqConsumer.consumerGroup();
        if(StringUtils.isEmpty(consumerGroup)) {
            //再次使用AbstractMQPullConsumer,AbstractMQPushConsumer子类实例的consumerGroup字段值
            Object consumerGroupFieldValue = null;
            if(null == consumerGroupField || null == (consumerGroupFieldValue = consumerGroupField.get(bean))){
                throw new RuntimeException("consumer's consumerGroup not defined in @MQConsumer(use consumerGroup) annotation or not defined field which named consumerGroup(e.g. use ${\"rocketmq.demoConsumer.consumerGroup\"})");
            }
            if(!String.class.isAssignableFrom(consumerGroupField.getType())){
                throw new RuntimeException("consumer's field which named consumerGroup must be String");
            }
            consumerGroup = (String)consumerGroupFieldValue;
        }
        final Field topicField = FieldUtils.getDeclaredField(bean.getClass(), "topic", true);
        //优先使用@MQConsumer注解定义的topic值
        String topic = mqConsumer.topic();
        if(StringUtils.isEmpty(topic)) {
            //再次使用AbstractMQPullConsumer,AbstractMQPushConsumer子类实例的consumerGroup字段值
            Object topicFieldValue = null;
            if(null == topicField || null == (topicFieldValue = topicField.get(bean))){
                throw new RuntimeException("consumer's topic not defined in @MQConsumer(use topic) annotation or not defined field which named topic(e.g. use ${\"rocketmq.demoConsumer.topic\"})");
            }
            if(!String.class.isAssignableFrom(topicField.getType())){
                throw new RuntimeException("consumer's field which named topic must be String");
            }
            topic = (String)topicFieldValue;
        }
        if(!AbstractMQPushConsumer.class.isAssignableFrom(bean.getClass())
                && !AbstractMQPullConsumer.class.isAssignableFrom(bean.getClass())) {
            throw new RuntimeException(bean.getClass().getName() + " - consumer未实现AbstractMQPushConsumer或AbstractMQPullConsumer抽象类");
        }
        //环境变量有对consumerGroup的配置，则取环境变量的配置（环境变量的key为@MQConsumer的consumerGroup或消费者实例consumerGroup字段的值，注意不是字段的名称）
        String consumerGroupEnv = applicationContext.getEnvironment().getProperty(consumerGroup);
        if(StringUtils.isNotEmpty(consumerGroupEnv)) {
            consumerGroup = consumerGroupEnv;
        }
        //环境变量有对topic的配置，则取环境变量的配置（环境变量的key为@MQConsumer的topic或消费者实例topic字段的值，注意不是字段的名称）
        String topicEnv = applicationContext.getEnvironment().getProperty(topic);
        if(StringUtils.isNotEmpty(topicEnv)) {
            topic = topicEnv;
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