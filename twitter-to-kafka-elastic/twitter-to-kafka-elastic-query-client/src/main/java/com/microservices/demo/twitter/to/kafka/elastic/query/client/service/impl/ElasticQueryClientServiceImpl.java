package com.microservices.demo.twitter.to.kafka.elastic.query.client.service.impl;

import com.microservices.demo.elastic.model.index.impl.TwitterIndexModel;
import com.microservices.demo.twitter.to.kafka.config.ElasticConfigData;
import com.microservices.demo.twitter.to.kafka.config.ElasticQueryConfigData;
import com.microservices.demo.twitter.to.kafka.elastic.query.client.service.ElasticQueryServiceClient;
import com.microservices.demo.twitter.to.kafka.elastic.query.client.util.ElasticQueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class ElasticQueryClientServiceImpl implements ElasticQueryServiceClient<TwitterIndexModel> {

    private final static Logger LOG = LoggerFactory.getLogger(ElasticQueryClientServiceImpl.class);
    private final ElasticQueryConfigData elasticQueryConfigData;
    private final ElasticConfigData elasticConfigData;
    private final ElasticsearchOperations elasticsearchOperations;
    private final ElasticQueryUtil<TwitterIndexModel> elasticQueryUtil;

    public ElasticQueryClientServiceImpl(ElasticQueryConfigData elasticQueryConfigData, ElasticConfigData elasticConfigData, ElasticsearchOperations elasticsearchOperations, ElasticQueryUtil<TwitterIndexModel> elasticQueryUtil) {
        this.elasticQueryConfigData = elasticQueryConfigData;
        this.elasticConfigData = elasticConfigData;
        this.elasticsearchOperations = elasticsearchOperations;
        this.elasticQueryUtil = elasticQueryUtil;
    }

    @Override
    public TwitterIndexModel getIndexModelById(String id) {
        Query searchQueryById = elasticQueryUtil.getSearchQueryById(id);
        SearchHit<TwitterIndexModel> twitterIndexModelSearchHit = elasticsearchOperations.searchOne(searchQueryById, TwitterIndexModel.class,
                IndexCoordinates.of(elasticConfigData.getIndexName()));

        if(twitterIndexModelSearchHit == null){
            LOG.info("No document found at elastic search with id {} ", id);
            throw new RuntimeException("No document found at elastic search with id " + id);
        }

        LOG.info("Document with id {} retrieve successfully ", twitterIndexModelSearchHit.getId());
        return twitterIndexModelSearchHit.getContent();
    }

    @Override
    public List<TwitterIndexModel> getIndexModelByText(String text) {
        Query searchQueryByFieldText = elasticQueryUtil.getSearchQueryByFieldText(elasticQueryConfigData.getTextField(), text);

        return search(searchQueryByFieldText, "{} of documents with text {} retrieve successfully", text);
    }

    private List<TwitterIndexModel> search(Query query, String logMessage, Object... logParmas) {
        SearchHits<TwitterIndexModel> search = elasticsearchOperations.search(
                query,
                TwitterIndexModel.class,
                IndexCoordinates.of(elasticConfigData.getIndexName()));

        LOG.info("{} of documents with text {} retrieve successfully", search.getTotalHits(), logParmas);
        return search.get().map(SearchHit::getContent).collect(Collectors.toList());
    }

    @Override
    public List<TwitterIndexModel> getAllIndexModels() {
        Query searchQueryForAll = elasticQueryUtil.getSearchQueryForAll();
        return search(searchQueryForAll, "{} of documents with text {} retrieve successfully");
    }
}
