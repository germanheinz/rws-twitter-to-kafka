package com.microservices.demo.elastic.query.service.business;

import com.microservices.demo.elastic.query.service.model.ElasticQueryServiceResponseModel;

import java.util.List;

public interface TwitterElasticQueryService {

    ElasticQueryServiceResponseModel getDocumentById(String id);

    List<ElasticQueryServiceResponseModel> getDocumentByText(String text);

    List<ElasticQueryServiceResponseModel> getAllDocuments();

}
