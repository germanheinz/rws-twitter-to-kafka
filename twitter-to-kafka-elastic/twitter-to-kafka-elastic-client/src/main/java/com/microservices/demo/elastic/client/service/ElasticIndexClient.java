package com.microservices.demo.elastic.client.service;

import com.microservices.demo.elastic.model.index.IndexModel;

import java.util.List;

public interface ElasticIndexClient <T extends IndexModel> {

    List<String> save(List<T> documents);
}
