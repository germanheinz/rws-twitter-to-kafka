package com.microservices.demo.twitter.to.kafka.elastic.service.transformer;

import com.microservices.demo.elastic.model.index.impl.TwitterIndexModel;
import com.microservices.kafka.model.TwitterAvroModel;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;


@Component
public class AvroToElasticModelTransformer {

    public List<TwitterIndexModel> getElasticModels(List<TwitterAvroModel> avroModels){


        List<TwitterIndexModel> list = avroModels.stream()
                .map(avroModel -> TwitterIndexModel
                        .builder()
                        .userId(avroModel.getUserId())
                        .id(String.valueOf(avroModel.getId()))
                        .text(avroModel.getText())
                        .createAt(LocalDateTime.ofInstant(Instant.ofEpochMilli(avroModel.getCreatedAt()),
                                ZoneId.systemDefault()))
                        .build()
                ).collect(Collectors.toList());
        return list;
    }

}
