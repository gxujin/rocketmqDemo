package com.cc.demo.base.mq;

import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Component
public class MessageSender {

    @Value("${rocketmq.client.logUseSlf4j}")
    private String logUseSlf4j;

    @Value("${rocketmq.client.logRoot}")
    private String logRoot;

    @Value("${rocketmq.client.logFileMaxIndex}")
    private String logFileMaxIndex;

    @Value("${rocketmq.client.logFileMaxSize}")
    private String logFileMaxSize;

    @Value("${rocketmq.client.logLevel}")
    private String logLevel;

    /**
     * NameServer 地址
     */
    @Value("${rocketmq.name-server}")
    private String namesrvAddr;

    /**
     *  生产组
     */
    @Value("${rocketmq.producer.group}")
    private String producerGroup;

    @Value("${rocketmq.producer.topic.test}")
    private String testTopic;



    private DefaultMQProducer producer = null;

    @PostConstruct
    private void initMQProducer() {
        //rocketmq日志配置
        System.setProperty(ClientLogger.CLIENT_LOG_USESLF4J, logUseSlf4j);
        if("false".equals(logUseSlf4j)){
            if(logRoot!=null && logRoot.length()>0){
                System.setProperty("rocketmq.client.logRoot", logRoot);
            }
            if(logFileMaxIndex!=null && logFileMaxIndex.length()>0){
                System.setProperty("rocketmq.client.logFileMaxIndex", logFileMaxIndex);
            }
            if(logFileMaxSize!=null && logFileMaxSize.length()>0){
                System.setProperty("rocketmq.client.logFileMaxSize", logFileMaxSize);
            }
            if(logLevel!=null && logLevel.length()>0){
                System.setProperty("rocketmq.client.logLevel", logLevel);
            }
        }
        producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(namesrvAddr);
        try {
            producer.start();
            System.out.println("test生产者启动成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @PreDestroy
    private void shutDownProducer() {
        if (producer != null) {
            producer.shutdown();
        }
    }

    public boolean sendMq(String json){
        return sendMq(null, json);
    }

    public boolean sendMq(String tag, String json){
        tag = tag == null ? "" : tag;
        try {
            Message message = new Message(testTopic, tag, json.getBytes("utf-8"));
            producer.send(message);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
