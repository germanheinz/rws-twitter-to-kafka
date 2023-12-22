package com.microservices.demo.twitter.to.kafka.elastic.query.client.service;

import com.microservices.demo.elastic.model.index.impl.TwitterIndexModel;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ElasticSearchQueryRepository extends ElasticsearchRepository<TwitterIndexModel, String> {
    List<TwitterIndexModel> findByText(String text);

}
