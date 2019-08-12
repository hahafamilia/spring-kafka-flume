package com.example.springkafkaflume.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix="spring.kafka")
public class KafkaProperty {
    private String bootstrapServers;
    private String username;
    private String password;
    private String topic;

    private String saslMechanism;
    private String saslJaasConfig;
    private String securityProtocol;

}

