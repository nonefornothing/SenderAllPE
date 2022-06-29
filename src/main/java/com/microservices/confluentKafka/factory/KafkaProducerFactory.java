package com.microservices.confluentKafka.factory;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerFactory {

    @Value("${kafka.producer.broker}")
    private String bootstrapServers;

    @Value("${ack.mode}")
    private String ackMode;

    @Value("${retries.amount}")
    private int retriesAmount;

    @Value("${linger.time}")
    private int lingerTime;

    @Value("${client.ID}")
    private String clientID;

    @Value("${idempotence.mode}")
    private Boolean idempotenceMode;

    @Value("${max.in.flight.request}")
    private int maxFlightRequestConnection;

    @Value("${delivery.timeout}")
    private int deliveryTimeout;

    @Value("${buffer.memory}")
    private int bufferMemory;

    @Value("${retry.backoff}")
    private int retryBackoff;

    /**
     *
     * This controller configuration producer
     *
     */

    @Bean
    public Map<String, Object> producerConfigs(){
        Map<String, Object> config = new HashMap<>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, ackMode);
        config.put(ProducerConfig.RETRIES_CONFIG,retriesAmount);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG,16384 * 4);
        config.put(ProducerConfig.LINGER_MS_CONFIG, lingerTime);
        config.put(ProducerConfig.CLIENT_ID_CONFIG,clientID);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotenceMode);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxFlightRequestConnection);
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG,bufferMemory);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,deliveryTimeout);
        config.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,retryBackoff);
        return config;
    }


    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

}
