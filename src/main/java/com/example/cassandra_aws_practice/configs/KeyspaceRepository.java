package com.example.cassandra_aws_practice.configs;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

@Slf4j
@Repository
public class KeyspaceRepository {
    private final CqlSession session;

    @Autowired
    public KeyspaceRepository(@Qualifier("sessionAws")CqlSession session) {
        this.session = session;
    }

    public void createKeyspace(String keyspaceName, int numberOfReplicas){
        CreateKeyspace createKeyspace = SchemaBuilder.createKeyspace(keyspaceName)
                .ifNotExists()
                .withSimpleStrategy(numberOfReplicas);
        log.info("x -> \n " + createKeyspace.build().getQuery());

        session.execute(createKeyspace.build());
    }

    public void dropKeyspace(String keyspaceName){
        dropKeyspace(keyspaceName);
    }

    public void useKeyspace(String keyspaceName){
        session.execute("USE " + CqlIdentifier.fromCql(keyspaceName));
    }
}
