package com.microservices.demo.twitter.to.kafka.elastic.service.consumer.impl;

import com.microservices.demo.kafka.admin.client.KafkaAdminClient;
import com.microservices.demo.twitter.to.kafka.config.KafkaConfigData;
import com.microservices.demo.twitter.to.kafka.elastic.service.consumer.KafkaConsumer;
import com.microservices.kafka.avro.model.TwitterAvroModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;
@Service
public class KafkaConsumerImpl implements KafkaConsumer<Long, TwitterAvroModel> {

    private  static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerImpl.class);

    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    private final KafkaAdminClient kafkaAdminClient;

    private final KafkaConfigData kafkaConfigData;

    public KafkaConsumerImpl(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry, KafkaAdminClient kafkaAdminClient, KafkaConfigData kafkaConfigData) {
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
        this.kafkaAdminClient = kafkaAdminClient;
        this.kafkaConfigData = kafkaConfigData;
    }

    @Override
    @KafkaListener(id="twitterTopicListener", topics = "${kafka-config.topic-name}")
    public void receive(
            @Payload List<TwitterAvroModel> message,
            @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) List<Integer> keys,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
            @Header(KafkaHeaders.OFFSET) List<Long> offsets) {
        LOG.info("LALAAAA LOMM!!!!!!!!!!!!!!!!!!!!!!! {} number of message received with keys {}, partitions {} and offsets {}, " + "sending it to elastic: Thread id {}", message.size(), keys.toString(), partitions.toString(), offsets.toString(), Thread.currentThread().getId());


    }
}
