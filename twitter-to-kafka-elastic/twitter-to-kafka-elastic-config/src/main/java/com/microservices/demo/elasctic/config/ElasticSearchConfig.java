package com.microservices.demo.elasctic.config;

import com.microservices.demo.twitter.to.kafka.config.ElasticConfigData;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.config.AbstractElasticsearchConfiguration;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Objects;

@Configuration
@EnableElasticsearchRepositories(basePackages = "com.microservices.demo.elastic")
public class ElasticSearchConfig extends AbstractElasticsearchConfiguration {

    private final ElasticConfigData elasticConfigData;

    public ElasticSearchConfig(ElasticConfigData elasticConfigData) {
        this.elasticConfigData = elasticConfigData;
    }

    @Override
    @Bean
    public RestHighLevelClient elasticsearchClient() {
        UriComponents serverUri = UriComponentsBuilder.fromHttpUrl(elasticConfigData.getConnectionUrl()).build();
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(
                                Objects.requireNonNull(serverUri.getHost()),
                                serverUri.getPort(),
                                serverUri.getScheme()
                        )).setRequestConfigCallback(
                            requestConfigBuilder ->
                                    requestConfigBuilder
                                            .setConnectTimeout(elasticConfigData.getConnectTimeoutMs())
                                            .setSocketTimeout(elasticConfigData.getSocketTimeoutMs())
                            )
                );
    }

    @Bean
    public ElasticsearchOperations elasticSearchTemplate(){
        return new ElasticsearchRestTemplate(elasticsearchClient());
    }
}
