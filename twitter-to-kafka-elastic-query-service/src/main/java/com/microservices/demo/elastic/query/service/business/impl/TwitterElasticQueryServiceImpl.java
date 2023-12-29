package com.microservices.demo.elastic.query.service.business.impl;

import com.microservices.demo.elastic.model.index.impl.TwitterIndexModel;
import com.microservices.demo.elastic.query.service.assembler.ElasticQueryServiceResponseModelAssember;
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

    private final ElasticQueryServiceResponseModelAssember elasticQueryServiceResponseModelAssember;

    private final ElasticQueryServiceClient<TwitterIndexModel> elasticsearchClient;

    public TwitterElasticQueryServiceImpl(ElasticQueryServiceResponseModelAssember elasticQueryServiceResponseModelAssember, ElasticQueryServiceClient<TwitterIndexModel> elasticsearchClient) {
        this.elasticQueryServiceResponseModelAssember = elasticQueryServiceResponseModelAssember;
        this.elasticsearchClient = elasticsearchClient;
    }

    @Override
    public ElasticQueryServiceResponseModel getDocumentById(String id) {
        LOG.info("Querying elasticSearch by id {}", id);
        return elasticQueryServiceResponseModelAssember.toModel(elasticsearchClient.getIndexModelById(id));
    }

    @Override
    public List<ElasticQueryServiceResponseModel> getDocumentByText(String text) {
        LOG.info("Querying elasticSearch by id {}", text);
        return elasticQueryServiceResponseModelAssember.toModels(elasticsearchClient.getIndexModelByText(text));
    }

    @Override
    public List<ElasticQueryServiceResponseModel> getAllDocuments() {
        return elasticQueryServiceResponseModelAssember.toModels(elasticsearchClient.getAllIndexModels());
    }
}