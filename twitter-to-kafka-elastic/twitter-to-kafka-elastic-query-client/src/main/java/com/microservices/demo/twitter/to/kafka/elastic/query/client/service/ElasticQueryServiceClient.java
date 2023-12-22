package com.microservices.demo.twitter.to.kafka.elastic.query.client.service;

import com.microservices.demo.elastic.model.index.IndexModel;

import java.util.List;

public interface ElasticQueryServiceClient<T extends IndexModel> {

    T getIndexModelById(String id);
    List<T> getIndexModelByText(String text);
    List<T> getAllIndexModels();

}
