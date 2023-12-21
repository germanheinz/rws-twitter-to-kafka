package com.microservices.demo.twitter.to.kafka.elastic.service.consumer.impl;

import com.microservices.demo.elastic.client.service.ElasticIndexClient;
import com.microservices.demo.elastic.model.index.impl.TwitterIndexModel;
import com.microservices.demo.kafka.admin.client.KafkaAdminClient;
import com.microservices.demo.twitter.to.kafka.config.KafkaConfigData;
import com.microservices.demo.twitter.to.kafka.config.KafkaConsumerConfigData;
import com.microservices.demo.twitter.to.kafka.elastic.service.consumer.KafkaConsumer;
import com.microservices.demo.twitter.to.kafka.elastic.service.transformer.AvroToElasticModelTransformer;

import com.microservices.kafka.model.TwitterAvroModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

@Service
public class KafkaConsumerImpl implements KafkaConsumer<Long, TwitterAvroModel> {

    private  static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerImpl.class);

    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    private final KafkaAdminClient kafkaAdminClient;

    private final KafkaConfigData kafkaConfigData;

    private final AvroToElasticModelTransformer avroToElasticModelTransformer;

    private final ElasticIndexClient<TwitterIndexModel> elasticIndexClient;

    private final KafkaConsumerConfigData kafkaConsumerConfigData;


    public KafkaConsumerImpl(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry, KafkaAdminClient kafkaAdminClient, KafkaConfigData kafkaConfigData, AvroToElasticModelTransformer avroToElasticModelTransformer, ElasticIndexClient elasticIndexClient, KafkaConsumerConfigData kafkaConsumerConfigData) {
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
        this.kafkaAdminClient = kafkaAdminClient;
        this.kafkaConfigData = kafkaConfigData;
        this.avroToElasticModelTransformer = avroToElasticModelTransformer;
        this.elasticIndexClient = elasticIndexClient;
        this.kafkaConsumerConfigData = kafkaConsumerConfigData;
    }

    @EventListener
    public void onAppStarted(ApplicationStartedEvent event){
        kafkaAdminClient.checkTopicsCreated();
        LOG.info("Topic with name {} is ready for operations!", kafkaConfigData.getTopicNamesToCreate().toArray());
        Objects.requireNonNull(kafkaListenerEndpointRegistry
                .getListenerContainer(kafkaConsumerConfigData.getConsumerGroupId())).start();
    }
    @Override
    @KafkaListener(id="${kafka-consumer-config.consumer-group-id}", topics = "${kafka-config.topic-name}")
    public void receive(
            @Payload List<TwitterAvroModel> messages,
            @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) List<Integer> keys,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
            @Header(KafkaHeaders.OFFSET) List<Long> offsets) {
        LOG.info("LALAAAA LOMM!!!!!!!!!!!!!!!!!!!!!!! {} number of message received with keys {}, partitions {} and offsets {}, " + "sending it to elastic: Thread id {}", messages.size(), keys.toString(), partitions.toString(), offsets.toString(), Thread.currentThread().getId());

        List<TwitterIndexModel> twitterIndexModels = avroToElasticModelTransformer.getElasticModels(messages);

        List<String> documentIds = elasticIndexClient.save(twitterIndexModels);
        LOG.info("Documents saved to elasticsearch with ids {}", documentIds.toArray());
    }
}
