package com.example.springkafkaflume;

import com.example.springkafkaflume.configuration.KafkaProperty;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@Slf4j
@RestController
public class ProducerController {

    @Autowired KafkaProperty kafkaProperty;
    @Autowired ProducerService producerService;


    @PostMapping("/produce")
    public Long produce(@RequestBody ActivityEvent activityEvent) throws IOException {

        producerService.send(kafkaProperty.getTopic(), activityEvent);

        return activityEvent.getActivityTime();
    }

}
