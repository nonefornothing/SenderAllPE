package com.microservices.confluentKafka.factory;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;

@Configuration
@EnableKafka
public class KafkaListenerFactory {

	@Value("${kafka.consumer.broker}")
	private String bootstrapServers;
	
	@Value("${kafka.consumer.id.group}")
	private String idGroupConsumer;
	
	@Value("${kafka.consumer.auto.offset.reset}")
	private String autoOffsetReset;
	
	@Value("${kafka.schema.registry.url}")
	private String schemaRegistryUrl;
	
	@Value("${enable.auto.commit}")
	private String enableAutoCommit;
	
	@Value("${concurrent.consumer.kafka}")
	private int concurrentConsumer;
	
	
	/**
	 * @author bwx
	 * @date 12-03-2020
	 * 
	 * This controller configuration consumer for listen String message and auto commit
	 *
	 */
	@Bean
	public ConsumerFactory<String,String> consumerFactory() {
	    Map<String, Object> config = new HashMap<>();
	    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		config.put(ConsumerConfig.GROUP_ID_CONFIG, idGroupConsumer);
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.connect.replicator.offsets.ConsumerTimestampsInterceptor");
	    return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), new StringDeserializer());
	}
	
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
	    ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
	    factory.setConsumerFactory(consumerFactory());
	    return factory;
	}
	
	/**
	 * @author bwx
	 * @date 12-03-2020
	 * 
	 * This controller configuration consumer for listen String message and manual commit
	 *
	 */
	
	@Bean
	public ConsumerFactory<String,String> consumerFactoryString() {
	    Map<String, Object> config = new HashMap<>();
	    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		config.put(ConsumerConfig.GROUP_ID_CONFIG, idGroupConsumer);
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
		config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//		config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
		config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.MAX_VALUE);
//		config.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 16000000);
//		config.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.connect.replicator.offsets.ConsumerTimestampsInterceptor");
	    return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), new StringDeserializer());
	}
	
	@Bean(name = "kafkaListenerContainerFactoryString")
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactoryString() {
	    ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
	    factory.setConsumerFactory(consumerFactoryString());
	    factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
	    factory.setConcurrency(concurrentConsumer);
	    return factory;
	}

}
