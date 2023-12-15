package com.microservices.demo.elastic.client.service.impl;

import com.microservices.demo.elastic.client.repository.TwitterElasticsearchIndexRepository;
import com.microservices.demo.elastic.client.service.ElasticIndexClient;
import com.microservices.demo.elastic.model.index.impl.TwitterIndexModel;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@ConditionalOnProperty(name = "elastic-config.is-repository", havingValue = "true", matchIfMissing = true)
public class TwitterElasticServiceIndexClient implements ElasticIndexClient<TwitterIndexModel> {

    private final TwitterElasticsearchIndexRepository twitterElasticsearchIndexRepository;


    public TwitterElasticServiceIndexClient(TwitterElasticsearchIndexRepository twitterElasticsearchIndexRepository) {
        this.twitterElasticsearchIndexRepository = twitterElasticsearchIndexRepository;
    }

    @Override
    public List<String> save(List<TwitterIndexModel> documents) {
        List<TwitterIndexModel> repositoryResponse = (List<TwitterIndexModel>) twitterElasticsearchIndexRepository.saveAll(documents);

        List<String> ids = repositoryResponse.stream().map(TwitterIndexModel::getId).collect(Collectors.toList());

        return ids;
    }
}
