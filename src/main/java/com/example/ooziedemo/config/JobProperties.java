package com.example.ooziedemo.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "job")
@Setter
@Getter
public class JobProperties {
    private String oozieUrl;
    private String nameNode;
    private String resourceManager;
    private String jdbcUrl;
    private String username;
    private String password;
    private String basePath;
    private String hiveSql;
    private String useSystemLibpath;
}
