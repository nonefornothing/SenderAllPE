package com.microservices.confluentKafka.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.Serializable;

@RestController
public class ConsumerRetryActivation implements Serializable {

    private final Logger logger = LoggerFactory.getLogger(ConsumerRetryActivation.class);

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Value("${failed.consumer.client.id}")
    private String failedConsumerClientId;

//    @Value()
//    public int partitionFailed;

    private String topicName;

    public String gettopicName(){
        return this.topicName;
    }

//    @Bean
//    public ProducerFactory<String, String> producerFactory() {
//        return new DefaultKafkaProducerFactory<>(producerConfigs());
//    }

    @GetMapping("/activate/{topicName}")
    public String activate(@PathVariable String topicName) {

//        Topic topic = new Topic();
//        topic.setTopicName(topicName);

//        topicNameClass = topicName;

        logger.info("TRYING TO ACTIVATE OR RESTARTED FAILED CONSUMER");

        if(!registry.getListenerContainer(failedConsumerClientId).isRunning()){
            registry.getListenerContainer(failedConsumerClientId).start();
//            registry.getListenerContainer(failedConsumerClientId).getContainerProperties().setConsumerProperties();
            registry.getListenerContainer(failedConsumerClientId).start();
            logger.info("FAILED CONSUMER SUCCESSFULLY ACTIVATED");
            return "FAILED CONSUMER SUCCESSFULLY ACTIVATED";
        }else if(registry.getListenerContainer(failedConsumerClientId).isRunning()){
            logger.info("FAILED CONSUMER STILL ACTIVATED & RESTARTED FAILED CONSUMER");
            registry.getListenerContainer(failedConsumerClientId).stop();
            registry.getListenerContainer(failedConsumerClientId).start();
            return "FAILED CONSUMER STILL ACTIVATED & RESTARTED FAILED CONSUMER";
        }else{
            logger.error("ERROR WHILE ACTIVATED FAILED CONSUMER OR RESTARTED FAILED CONSUMER");
            return "ERROR WHILE ACTIVATED FAILED CONSUMER OR RESTARTED FAILED CONSUMER";
        }
    }

    @GetMapping("/deactivate/")
    public String deactivate() {

        logger.info("TRYING TO DEACTIVATE FAILED CONSUMER");

        if(!registry.getListenerContainer(failedConsumerClientId).isRunning()){
            logger.info("FAILED CONSUMER ALREADY DEACTIVATED");
            return "FAILED CONSUMER ALREADY DEACTIVATED";
        }else if(registry.getListenerContainer(failedConsumerClientId).isRunning()){
            logger.info("FAILED CONSUMER SUCCESSFULLY DEACTIVATED");
            registry.getListenerContainer(failedConsumerClientId).stop();
            return "FAILED CONSUMER SUCCESSFULLY DEACTIVATED";
        }else{
            logger.error("ERROR WHILE DEACTIVATE FAILED CONSUMER");
            return "ERROR WHILE DEACTIVATE FAILED CONSUMER";
        }

    }



}
