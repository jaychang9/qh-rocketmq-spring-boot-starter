package com.maihaoche.starter.mq.config;

import com.maihaoche.starter.mq.annotation.MQProducer;
import com.maihaoche.starter.mq.base.AbstractMQProducer;
import java.lang.reflect.Field;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.Map;
import org.springframework.util.CollectionUtils;

/**
 * Created by yipin on 2017/6/29.
 * 自动装配消息生产者
 */
@Slf4j
@Configuration
@ConditionalOnBean(MQBaseAutoConfiguration.class)
public class MQProducerAutoConfiguration extends MQBaseAutoConfiguration {

    private DefaultMQProducer producer;

    @PostConstruct
    public void init() throws Exception {
        Map<String, Object> beans = applicationContext.getBeansWithAnnotation(MQProducer.class);
        //对于仅仅存在消费者的项目，无需初始化producer实例
        if(CollectionUtils.isEmpty(beans)){
            return;
        }
        if(producer == null) {
            if(StringUtils.isEmpty(mqProperties.getProducerGroup())) {
                throw new RuntimeException("producer group must be defined");
            }
            if(StringUtils.isEmpty(mqProperties.getNameServerAddress())) {
                throw new RuntimeException("name server address must be defined");
            }
            producer = new DefaultMQProducer(mqProperties.getProducerGroup());
            producer.setNamesrvAddr(mqProperties.getNameServerAddress());
            producer.start();
        }
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            publishProducer(entry.getKey(), entry.getValue());
        }
    }

    private void publishProducer(String beanName, Object bean) throws Exception {
        if(!AbstractMQProducer.class.isAssignableFrom(bean.getClass())) {
            throw new RuntimeException(beanName + " - producer未继承AbstractMQProducer");
        }
        AbstractMQProducer abstractMQProducer = (AbstractMQProducer) bean;
        abstractMQProducer.setProducer(producer);
        // begin build producer level topic
        MQProducer mqProducer = applicationContext.findAnnotationOnBean(beanName, MQProducer.class);

        //优先使用MQProducer注解指定的topic值
        String topic = mqProducer.topic();

        if(StringUtils.isEmpty(topic)) {
            //其次使用生产者实例的topic字段值
            final Field topicField = FieldUtils.getDeclaredField(bean.getClass(),"topic",true);
            Object topicFieldValue = topicField.get(bean);
            if(null != topicField || null != topicFieldValue){
                if(!String.class.isAssignableFrom(topicField.getType())){
                    throw new RuntimeException("producer's field which named topic must be String");
                }
                topic = (String)topicFieldValue;
            }
        }
        //如果环境变量有设置，则使用环境变量，但环境变量的key取决于@MQProducer的topic值或生产者的topic字段值
        if(StringUtils.isNotEmpty(topic)) {
            // 取topic环境变量
            String topicEnv = applicationContext.getEnvironment().getProperty(topic);
            topic = StringUtils.isEmpty(topicEnv) ? topic : topicEnv;
        }
        abstractMQProducer.setTopic(topic);
        // begin build producer level tag
        String tag = mqProducer.tag();
        if(StringUtils.isNotEmpty(tag)) {
            String transTag = applicationContext.getEnvironment().getProperty(tag);
            tag = StringUtils.isEmpty(transTag) ? tag : transTag;
        }
        abstractMQProducer.setTag(tag);
        log.info(String.format("%s is ready to produce message", beanName));
    }
}