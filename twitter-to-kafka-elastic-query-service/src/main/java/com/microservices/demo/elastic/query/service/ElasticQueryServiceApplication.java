package com.microservices.demo.elastic.query.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"com.microservices.demo.twitter.to.kafka.elastic.query.client", "com.microservices.demo", "com.microservices.demo.twitter.to.kafka.elastic.query.client.repository.ElasticSearchQueryRepository"})
public class ElasticQueryServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(ElasticQueryServiceApplication.class, args);
    }
}
