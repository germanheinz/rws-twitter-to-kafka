package com.microservices.demo.elastic.query.service.business.impl;

import com.microservices.demo.elastic.model.index.impl.TwitterIndexModel;
import com.microservices.demo.elastic.query.service.business.TwitterElasticQueryService;
import com.microservices.demo.elastic.query.service.model.ElasticQueryServiceResponseModel;
import com.microservices.demo.elastic.query.service.transformer.ElasticToResponseModelTransformer;
import com.microservices.demo.twitter.to.kafka.elastic.query.client.service.ElasticQueryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
@Service
public class TwitterElasticQueryServiceImpl implements TwitterElasticQueryService {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterElasticQueryServiceImpl.class);

    private final ElasticToResponseModelTransformer elasticToResponseModelTransformer;

    private final ElasticQueryServiceClient<TwitterIndexModel> elasticsearchClient;

    public TwitterElasticQueryServiceImpl(ElasticToResponseModelTransformer elasticToResponseModelTransformer, ElasticQueryServiceClient<TwitterIndexModel> elasticsearchClient) {
        this.elasticToResponseModelTransformer = elasticToResponseModelTransformer;
        this.elasticsearchClient = elasticsearchClient;
    }

    @Override
    public ElasticQueryServiceResponseModel getDocumentById(String id) {
        LOG.info("Querying elasticSearch by id {}", id);
        return elasticToResponseModelTransformer.getResponseModel(elasticsearchClient.getIndexModelById(id));
    }

    @Override
    public List<ElasticQueryServiceResponseModel> getDocumentByText(String text) {
        LOG.info("Querying elasticSearch by id {}", text);
        return elasticToResponseModelTransformer.getResponseModels(elasticsearchClient.getIndexModelByText(text));
    }

    @Override
    public List<ElasticQueryServiceResponseModel> getAllDocuments() {
        return elasticToResponseModelTransformer.getResponseModels(elasticsearchClient.getAllIndexModels());
    }
}
