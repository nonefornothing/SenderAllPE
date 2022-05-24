package com.microservices.confluentKafka.engine;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microservices.confluentKafka.services.StreamService;
import com.microservices.confluentKafka.utils.AESUtils;

@Service
public class ConsumerEngine implements Serializable {

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
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private StreamService streamService;

	@Autowired
	private static ConsumerRetryActivation consumerRetryActivation;
	
	@Value("${json.key.url}")
	private String jsonKeyUrl;

	@Value("${json.key.partition}")
	private String jsonKeyPartition;
	
	@Value("${kafka.producer.clean.topic}")
	private String topicFailed;
	
	@Value("${aes.secret.key}")
	private String secretKey;
	
	@Value("${init.retry.after.failed}")
	private int initRetryAfterFailed;
	
	@Value("${retry.count}")
	private int retryCount;
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	private static final String topicName = consumerRetryActivation.gettopicName();

	static final String topic = topicName;

	@KafkaListener(id="${consumer.client.id}",topics = "topicFailed",autoStartup="${main.auto.startup.mode}" , containerFactory="kafkaListenerContainerFactoryString",beanRef = "")
	public void consume(ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) throws Exception {
		String bodyReal;
		String result = null;
		bodyReal = getBody(consumerRecord,jsonKeyUrl,jsonKeyPartition);
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
						TimeUnit.MILLISECONDS.sleep((long) initRetryAfterFailed *i);
						logger.error("Error retry ....!!! ==> "+i+" "+" | Offset  : " + consumerRecord.offset() + " | partition : " + consumerRecord.partition() + e2.getMessage());
					}
				}
				if(result == null) {
					String failedData = getBody(consumerRecord,jsonKeyPartition);
					String keyData = getAccountNumber(consumerRecord);
					int partition = getPartition(consumerRecord);
					logger.error("write to failed topic ...!!!"+" | Offset  : " + consumerRecord.offset() + " | partition : " + consumerRecord.partition() +" | data : "+ failedData);
					sendToRetryTopic(topicFailed,partition,keyData,bodyReal);
					acknowledgment.acknowledge();
				}
			} catch (Exception e1) {
				String failedData = getBody(consumerRecord,jsonKeyPartition);
				String keyData = getAccountNumber(consumerRecord);
				int partition = getPartition(consumerRecord);
				logger.error("write to failed topic ...!!!"+" | Offset  : " + consumerRecord.offset() + " | partition : " + consumerRecord.partition() +" | data : "+ failedData);
				sendToRetryTopic(topicFailed,partition,keyData,bodyReal);
				acknowledgment.acknowledge();
			}finally {
				acknowledgment.acknowledge();
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

	private void parseMessages(ObjectNode node, String jsonKeyUrl, String jsonKeyPartition) {
		try {
			node.remove(jsonKeyUrl);
			node.remove(jsonKeyPartition);
		} catch (Exception e) {
			logger.error("Error parsing message....!!! [ " + node.asText() + " ]" + e.getMessage());
		}
	}

	private void parseMessages(ObjectNode node, String jsonKeyPartition) {
		try {
			node.remove(jsonKeyPartition);
		} catch (Exception e) {
			logger.error("Error parsing message....!!! [ " + node.asText() + " ]" + e.getMessage());
		}
	}

	private void sendToRetryTopic(String topicName, int partition ,String keyData,String sentData){
		try{
			kafkaTemplate.send(topicName,sentData);
			kafkaTemplate.send(topicName,partition,keyData,sentData);
			logger.info("Produce Failed Data : " + sentData + " + to topic : " +  topicName);
		}catch (InterruptedException e3){
			logger.info("a");
			cuma untuk commit progress :D
		}catch (InvalidTopicException e){

		}
		catch (Exception e){
			logger.error("Error while sending data to kafka broker with " + e.getMessage());
		}
	}

	public String getBody(ConsumerRecord<String, String> consumerRecord,String jsonKeyUrl, String jsonKeyPartition) throws JsonProcessingException {
		String bodyReal;
		ObjectNode node = (ObjectNode) new ObjectMapper().readTree(consumerRecord.value());
		parseMessages(node,jsonKeyUrl,jsonKeyPartition);
		bodyReal = node.toString();
		return bodyReal;
	}

	public String getBody(ConsumerRecord<String, String> consumerRecord, String jsonKeyPartition) throws JsonProcessingException {
		String bodyReal;
		ObjectNode node = (ObjectNode) new ObjectMapper().readTree(consumerRecord.value());
		parseMessages(node,jsonKeyPartition);
		bodyReal = node.toString();
		return bodyReal;
	}

	private String getUri (ConsumerRecord<String, String> consumerRecord) throws JsonProcessingException {
		ObjectNode node = (ObjectNode) new ObjectMapper().readTree(consumerRecord.value()) ;
		return getUri(node);
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

	private String getAccountNumber(ConsumerRecord<String, String> consumerRecord) {
		String accountNumber = "";
		try {
			accountNumber = consumerRecord.key();
		} catch (Exception e) {
			logger.error("Error get account Number from message....!!! " +  e.getMessage());
		}
		return accountNumber;
	}

	private int getPartition(ConsumerRecord<String, String> consumerRecord) throws JsonProcessingException {
		int partition;
		ObjectNode node = (ObjectNode) new ObjectMapper().readTree(consumerRecord.value()) ;
		partition = getPartition(node);
		return partition;
	}


	private int getPartition(ObjectNode node) {
		int partition=0;
		try {
			partition = node.get(partition).asInt();
		} catch (Exception e) {
			logger.error("Error get partition number from message....!!! " +  e.getMessage());
		}
		return partition;
	}
	
}