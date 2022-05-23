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

    @Value("${transactional.ID}")
    private String transactionalID;

    @Value("${idempotence.mode}")
    private Boolean idempotenceMode;

    @Value("${max.in.flight.request}")
    private int maxFlightRequestConnection;

    @Value("${delivery.timeout}")
    private int deliveryTimeout;

    @Value("${buffer.memory}")
    private int bufferMemory;

    /**
     * @author bwx
     * @date 12-03-2020
     *
     * This controller configuration consumer for listen String message and manual commit
     *
     */

//    @Bean
//    public ProducerFactory<String,String> producerFactoryString(){
//        Map<String, Object> config = new HashMap<>();
//
//        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        config.put(ProducerConfig.ACKS_CONFIG, ackMode);
//        config.put(ProducerConfig.RETRIES_CONFIG,retriesAmount);
//        config.put(ProducerConfig.BATCH_SIZE_CONFIG,16384 * 4);
//        config.put(ProducerConfig.LINGER_MS_CONFIG, 20);
//        config.put(ProducerConfig.CLIENT_ID_CONFIG,clientID);
//        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,transactionalID);
//        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotenceMode);
//        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxFlightRequestConnection);
//        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG,bufferMemory);
//        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,deliveryTimeout);
//
//        return new DefaultKafkaProducerFactory<String,String>(config);
//    }

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
//        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,transactionalID);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotenceMode);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxFlightRequestConnection);
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG,bufferMemory);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,deliveryTimeout);
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
