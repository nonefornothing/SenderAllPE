package com.microservices.confluentKafka.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microservices.confluentKafka.services.StreamService;
import com.microservices.confluentKafka.utils.AESUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class RetryConsumerEngine implements Serializable {

    /**
     * consumer engine clean topic PE
     * - Engine health check will hit every 1 second (configurable) to destination services
     * - Engine consumer will stop automatic when health check is down/not response
     * - Engine consumer will start automatic when health check is up/response
     * - construct and decode message to get URL from message
     * - send message to destination
     * - retry re send after failed send until < retry.count
     */
    private static final long serialVersionUID = -258047285576212814L;

    private final Logger logger = LoggerFactory.getLogger(ConsumerEngine.class);

    @Autowired
    private StreamService streamService;

    @Value("${json.key.url}")
    private String jsonKeyUrl;

    @Value("${aes.secret.key}")
    private String secretKey;

    @Value("${init.retry.after.failed}")
    private long initRetryAfterFailed;

    @Value("${retry.count}")
    private int retryCount;

    @Value("${max.wait.after.failed}")
    private int maxWaitAfterFailed;


    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    @KafkaListener(id="${failed.consumer.client.id}", topics = "${kafka.consumer.failed.topic}",autoStartup="${failed.auto.startup.mode}" , containerFactory="kafkaListenerContainerFactoryString")
    public void consume(ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) throws Exception {
        String bodyReal;
        String result=null;
        bodyReal = getBody(consumerRecord,jsonKeyUrl);
        try {
            String encryptedMessage = encryptMessage(bodyReal);
            String uriDestination =getUri(consumerRecord);
            logger.info("start send data : "+ sdf.format(new java.util.Date()) +" | Offset  : " + consumerRecord.offset() + " | partition : " + consumerRecord.partition()+" | data : "+ bodyReal);
            result = sending(encryptedMessage,uriDestination);
            acknowledgment.acknowledge();
            logger.info("end send data : "+ sdf.format(new java.util.Date()) +" | Offset  : " + consumerRecord.offset() + " | partition : " + consumerRecord.partition() + " | response : " + result);
        }catch (Exception e) {
            logger.error("Retry after "+initRetryAfterFailed+" ms ....!!!");
            logger.error("Error....!!!" + e.getMessage());
            try {
                TimeUnit.MILLISECONDS.sleep(initRetryAfterFailed);
                logger.info("start retry send data to API : "+ sdf.format(new java.util.Date()) + bodyReal);
                for (int i = 2; i <= retryCount; i++) {
                    try {
                        String encryptedMessage = encryptMessage(bodyReal);
                        String uriDestination =getUri(consumerRecord);
                        logger.info("start send data : "+ sdf.format(new java.util.Date()) +" | Offset  : " + consumerRecord.offset() + " | partition : " + consumerRecord.partition()+" | data : "+ bodyReal);
                        result = sending(encryptedMessage,uriDestination);
                        acknowledgment.acknowledge();
                        logger.info("end send data : "+ sdf.format(new java.util.Date()) +" | Offset  : " + consumerRecord.offset() + " | partition : " + consumerRecord.partition() + " | response : " + result);
                        break;
                    } catch (Exception e2) {
                        TimeUnit.MILLISECONDS.sleep(initRetryAfterFailed*i);
                        logger.error("Error retry ....!!! ==> "+i+" "+" | Offset  : " + consumerRecord.offset() + " | partition : " + consumerRecord.partition() + e2.getMessage());
                    }
                }
                if(result == null) {
                    while(true){
                        TimeUnit.MINUTES.sleep(maxWaitAfterFailed);
                        try {
                            String encryptedMessage = encryptMessage(bodyReal);
                            String uriDestination =getUri(consumerRecord);
                            logger.info("start send data : "+ sdf.format(new java.util.Date()) +" | Offset  : " + consumerRecord.offset() + " | partition : " + consumerRecord.partition()+" | data : "+ bodyReal);
                            result = sending(encryptedMessage,uriDestination);
                            acknowledgment.acknowledge();
                            logger.info("end send data : "+ sdf.format(new java.util.Date()) +" | Offset  : " + consumerRecord.offset() + " | partition : " + consumerRecord.partition() + " | response : " + result);
                            break;
                        }catch (Exception e2){
                            logger.error("Error retry after wait " + maxWaitAfterFailed + "minutes ....!!! ==> "+" | Offset  : " + consumerRecord.offset() + " | partition : " + consumerRecord.partition() + e2.getMessage());
                        }
                    }
                }
            } catch (Exception e1) {
                while(true){
                    TimeUnit.MINUTES.sleep(maxWaitAfterFailed);
                    try {
                        String encryptedMessage = encryptMessage(bodyReal);
                        String uriDestination =getUri(consumerRecord);
                        logger.info("start send data : "+ sdf.format(new java.util.Date()) +" | Offset  : " + consumerRecord.offset() + " | partition : " + consumerRecord.partition()+" | data : "+ bodyReal);
                        result = sending(encryptedMessage,uriDestination);
                        acknowledgment.acknowledge();
                        logger.info("end send data : "+ sdf.format(new java.util.Date()) +" | Offset  : " + consumerRecord.offset() + " | partition : " + consumerRecord.partition() + " | response : " + result);
                        break;
                    }catch (Exception e2){
                        logger.error("Error retry after wait " + maxWaitAfterFailed + "minutes ....!!! ==> "+" | Offset  : " + consumerRecord.offset() + " | partition : " + consumerRecord.partition() + e2.getMessage());
                    }
                }
            }
        }
    }

    private String sending(String encryptedMessage,String uri) throws Exception {
        String body;
        body = "{\"data\":\""+encryptedMessage+"\"}";
        return streamService.sendData(body,uri);
    }

    private String encryptMessage(String bodyReal) {
        return AESUtils.encrypt(bodyReal, secretKey);
    }

    private void parseMessages(ObjectNode node, String jsonKeyUrl) {
        try {
            node.remove(jsonKeyUrl);
        } catch (Exception e) {
            logger.error("Error parsing message....!!! [ " + node.asText() + " ]" + e.getMessage());
        }
    }

    public String getBody(ConsumerRecord<String, String> consumerRecord,String jsonKeyUrl) throws JsonProcessingException {
        String bodyReal;
        ObjectNode node = (ObjectNode) new ObjectMapper().readTree(consumerRecord.value());
        parseMessages(node,jsonKeyUrl);
        bodyReal = node.toString();
        return bodyReal;
    }

    private String getUri (ConsumerRecord<String, String> consumerRecord) throws JsonProcessingException {
        String uri;
        ObjectNode node = (ObjectNode) new ObjectMapper().readTree(consumerRecord.value()) ;
        uri = getUri(node);
        return uri;
    }

    private String getUri(ObjectNode node) {
        String uri ="";
        try {
            uri = node.get(jsonKeyUrl).asText();
        } catch (Exception e) {
            logger.error("Error get uri from message....!!! [ "+node.asText()+" ]"+  e.getMessage());
        }
        return uri;
    }

}
