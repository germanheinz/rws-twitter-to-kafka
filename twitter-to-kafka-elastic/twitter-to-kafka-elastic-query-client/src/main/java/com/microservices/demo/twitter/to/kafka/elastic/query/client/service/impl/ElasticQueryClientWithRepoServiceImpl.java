package com.microservices.demo.twitter.to.kafka.elastic.query.client.service.impl;


import com.microservices.demo.elastic.model.index.impl.TwitterIndexModel;
import com.microservices.demo.twitter.to.kafka.elastic.query.client.service.ElasticQueryServiceClient;
import com.microservices.demo.twitter.to.kafka.elastic.query.client.service.ElasticSearchQueryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Primary
@Service
public class ElasticQueryClientWithRepoServiceImpl implements ElasticQueryServiceClient<TwitterIndexModel> {
    private final static Logger LOG = LoggerFactory.getLogger(ElasticQueryClientWithRepoServiceImpl.class);

    private final ElasticSearchQueryRepository elasticSearchQueryRepository;

    public ElasticQueryClientWithRepoServiceImpl(ElasticSearchQueryRepository elasticSearchQueryRepository) {
        this.elasticSearchQueryRepository = elasticSearchQueryRepository;
    }

    @Override
    public TwitterIndexModel getIndexModelById(String id) {
        Optional<TwitterIndexModel> searchResult = elasticSearchQueryRepository.findById(id);
        LOG.info("Document with id {} retrieve successfully",
                searchResult.orElseThrow(() ->
                    new RuntimeException("No document found at elasticSearch with id " + id)).getId());
        return searchResult.get();
    }

    @Override
    public List<TwitterIndexModel> getIndexModelByText(String text) {
        List<TwitterIndexModel> searchResult = elasticSearchQueryRepository.findByText(text);
        LOG.info("{} of documents with text {} retrieve successfully", searchResult.size(), text);
        return searchResult;
    }

    @Override
    public List<TwitterIndexModel> getAllIndexModels() {
        Iterable<TwitterIndexModel> iterableTwitterModel = elasticSearchQueryRepository.findAll();

        List<TwitterIndexModel> twitterIndexModelList = new ArrayList<>();
        iterableTwitterModel.forEach(twitterIndexModelList::add);
        LOG.info("List of twitterIndexModel retrieve successfully: {}", twitterIndexModelList.size());
        return twitterIndexModelList;
    }
}
