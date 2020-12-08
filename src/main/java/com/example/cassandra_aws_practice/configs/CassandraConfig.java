package com.example.cassandra_aws_practice.configs;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.net.ssl.SSLContext;

@Slf4j
@Configuration
public class CassandraConfig {

    @Bean
    public CqlSession sessionAws() {
        CqlSessionBuilder builder = CqlSession.builder();

        try {
            builder = CqlSession.builder()
                    .withConfigLoader(DriverConfigLoader.fromClasspath("application.conf"))
                    .withSslContext(SSLContext.getDefault());

        } catch (Exception e) {
            log.info("Could not start CqlSession: {}", e.toString());
        }
        return builder.build();
    }
}
