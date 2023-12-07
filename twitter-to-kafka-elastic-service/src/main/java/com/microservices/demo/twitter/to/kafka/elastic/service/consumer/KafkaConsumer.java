package com.microservices.demo.twitter.to.kafka.elastic.service.consumer;

import org.apache.avro.specific.SpecificRecordBase;

import java.io.Serializable;
import java.util.List;

public interface KafkaConsumer <K extends Serializable, V extends SpecificRecordBase> {
    void receive(List<V> message, List<Integer> keys, List<Integer> partitions, List<Long> offsets);

}