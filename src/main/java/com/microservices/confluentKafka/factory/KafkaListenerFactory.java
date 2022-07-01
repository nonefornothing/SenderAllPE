package com.microservices.confluentKafka.factory;

import java.util.HashMap;
import java.util.Map;

import io.confluent.monitoring.clients.interceptor.MonitoringInterceptorConfig;
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
	
	@Value("${enable.auto.commit}")
	private String enableAutoCommit;
	
	@Value("${concurrent.consumer.kafka}")
	private int concurrentConsumer;
	
	
	/**
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
		// ConsumerTimestampsInterceptor: enables consumer applications to resume where they left off after a datacenter failover when using Confluent Replicator
		// MonitoringConsumerInterceptor: enables streams monitoring in Confluent Control Center
		config.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.connect.replicator.offsets.ConsumerTimestampsInterceptor,io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");
		config.put(MonitoringInterceptorConfig.MONITORING_INTERCEPTOR_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
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
