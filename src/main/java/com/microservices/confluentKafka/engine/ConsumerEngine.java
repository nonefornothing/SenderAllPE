package com.microservices.confluentKafka.engine;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microservices.confluentKafka.services.StreamService;
import com.microservices.confluentKafka.utils.AESUtils;
import org.springframework.web.client.HttpClientErrorException;


@Service
public class ConsumerEngine implements Serializable {

	private static final long serialVersionUID = 3945213982982104766L;

	private final Logger logger = LoggerFactory.getLogger(ConsumerEngine.class);

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private StreamService streamService;
	
	@Value("${json.key.url}")
	private String jsonKeyUrl;

	@Value("${json.key.topicname}")
	private String jsonKeyTopicName;

	@Value("${aes.secret.key}")
	private String secretKey;

	@Value("${dir.failed.pe}")
	private String dirFailedPE;
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	@KafkaListener(id="${consumer.client.id}",topics = "${kafka.consumer.clean.topic}",autoStartup="${auto.startup.mode}" , containerFactory="kafkaListenerContainerFactoryString")
	public void consume(ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) throws Exception {
		String encryptedMessage = null;
		String uriDestination = null;
		String bodyReal = null;
		try {
			try {
				bodyReal = getBody(consumerRecord,jsonKeyUrl,jsonKeyTopicName);
				encryptedMessage = encryptMessage(bodyReal);
				uriDestination =getUri(consumerRecord);
			}catch (Exception e){
				writeToFile(consumerRecord.value());
				logger.error("Error....!!!  " + e.getMessage());
				logger.error("write data with offset " + consumerRecord.offset() + " , partition " + consumerRecord.partition() + " to file");
			}
			if (bodyReal != null) {
				logger.info("start send data : " + sdf.format(new java.util.Date()) + " | Offset  : " + consumerRecord.offset() + " | partition : " + consumerRecord.partition() + " | data : " + bodyReal);
				ResponseEntity<String> result = sending(encryptedMessage, uriDestination);
				acknowledgment.acknowledge();
				logger.info("end send data : " + sdf.format(new java.util.Date()) + " | Offset  : " + consumerRecord.offset() + " | partition : " + consumerRecord.partition() + " | response : " + result.getBody());
			}
		}catch (HttpClientErrorException ec){
			if (bodyReal != null) {
				writeToFile(bodyReal);
				logger.error("write data");
				logger.error("Error....!!! error message " + ec.getMessage() + " status code : " + ec.getStatusCode());
				logger.error("write data | " + bodyReal + " with offset " + consumerRecord.offset() + " , partition " + consumerRecord.partition() + " to file");
			}else{
				logger.error("Body null");
			}
			acknowledgment.acknowledge();
		}catch (Exception e) {
			String bodyRetryReal = getBody(consumerRecord,jsonKeyTopicName);
			String topicName = getTopicName(consumerRecord);
			String accountNumber = getAccountNumber(consumerRecord);
			kafkaTemplate.send(topicName,accountNumber,bodyRetryReal);
			acknowledgment.acknowledge();
			logger.error("Error....!!!" + e.getMessage());
			logger.error("Sent data to topic " + topicName + " with account number " + accountNumber);
		}finally {
			acknowledgment.acknowledge();
		}
	}

	private ResponseEntity<String> sending(String encryptedMessage, String uri) throws Exception {
		String body;
		body = "{\"data\":\""+encryptedMessage+"\"}";
		return streamService.sendData(body,uri);
	}
	
	private String encryptMessage(String bodyReal) {
		return AESUtils.encrypt(bodyReal, secretKey);
	}

	private void parseMessages(ObjectNode node, String jsonKeyUrl, String topicName) {
		try {
			node.remove(jsonKeyUrl);
			node.remove(topicName);
		} catch (Exception e) {
			logger.error("Error parsing message....!!! [ " + node.asText() + " ]" + e.getMessage());
		}
	}

	private void parseMessages(ObjectNode node, String topicName) {
		try {
			node.remove(topicName);
		} catch (Exception e) {
			logger.error("Error parsing message....!!! [ " + node.asText() + " ]" + e.getMessage());
		}
	}

	public String getBody(ConsumerRecord<String, String> consumerRecord,String jsonKeyUrl, String topicName) throws JsonProcessingException {
		String bodyReal;
		ObjectNode node = (ObjectNode) new ObjectMapper().readTree(consumerRecord.value());
		parseMessages(node,jsonKeyUrl,topicName);
		bodyReal = node.toString();
		return bodyReal;
	}

	public String getBody(ConsumerRecord<String, String> consumerRecord, String topicName) throws JsonProcessingException {
		String bodyReal;
		ObjectNode node = (ObjectNode) new ObjectMapper().readTree(consumerRecord.value());
		parseMessages(node,topicName);
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

	private String getTopicName (ConsumerRecord<String, String> consumerRecord) throws JsonProcessingException {
		ObjectNode node = (ObjectNode) new ObjectMapper().readTree(consumerRecord.value()) ;
		return getTopicName(node);
	}

	private String getTopicName(ObjectNode node) {
		String topicName ="";
		try {
			topicName = node.get(jsonKeyTopicName).asText();
		} catch (Exception e) {
			logger.error("Error get topicName from message....!!! [ "+node.asText()+" ]"+  e.getMessage());
		}
		return topicName;
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

	private void writeToFile(String bodyReal) {
		try {
			if(!bodyReal.isEmpty()) {
				SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
				Files.write(Paths.get(dirFailedPE+sdf1.format(new java.util.Date())+ "-failed-pe.txt"), (bodyReal+ System.lineSeparator()).getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			}
			else{
				logger.error("Error...!!! write to failed PE file ...!!! Data null" );
			}
		}catch (IOException e3) {
			logger.error("Error...!!! write to failed PE file ...!!!" + e3.getMessage());
		}

	}
	
}