package com.example.cassandra_aws_practice.videos;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.example.cassandra_aws_practice.configs.KeyspaceRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.springframework.data.cassandra.core.cql.keyspace.DropTableSpecification.dropTable;

@Repository
public class VideoRepository {

    private static final String KEYSPACE_NAME = "entertainment";
    private static final String TABLE_NAME = "video_collection";
    private static final int NUMBER_OF_REPLICAS = 1;

    private CqlSession session;
    private KeyspaceRepository keyspaceRepository;
    private StoredProcedure storedProcedure;

    @Autowired
    public VideoRepository(@Qualifier("sessionAws") CqlSession session,
                           KeyspaceRepository keyspaceRepository,
                           StoredProcedure storedProcedure) {
        this.session = session;
        this.keyspaceRepository = keyspaceRepository;
        this.storedProcedure = storedProcedure;
    }

    @PostConstruct
    public void postConstruct(){
        keyspaceRepository.createKeyspace(KEYSPACE_NAME, NUMBER_OF_REPLICAS);
        keyspaceRepository.useKeyspace(KEYSPACE_NAME);
        this.createTable(KEYSPACE_NAME);
    }

    public void createTable(){
        createTable(KEYSPACE_NAME);
    }

    public void createTable(String keyspaceName){
        CreateTable createTable = SchemaBuilder.createTable(TABLE_NAME)
                .ifNotExists()
                .withPartitionKey("id", DataTypes.UUID)
                .withColumn("title", DataTypes.TEXT)
                .withColumn("creationDate", DataTypes.TIMESTAMP);

        SimpleStatement statement = createTable.build();

        if(keyspaceName != null){ statement.setKeyspace(keyspaceName); }
        session.execute(statement);
    }

    public void dropVideoTable(){
        dropVideoTable(TABLE_NAME);
    }

    public void dropVideoTable(String tableName){
        dropTable(tableName);
    }

    public void dropKeyspace(){
        keyspaceRepository.dropKeyspace(KEYSPACE_NAME);
    }

    public UUID insertVideo(Video video){
        return insertVideo(video, null);
    }

    public UUID insertVideo(Video video, String keyspaceName){
        return storedProcedure.insertVideoProcedure(video, keyspaceName);
    }

    public List<Video> selectAll(){
        return selectAll(null);
    }

    public List<Video> selectAll(String keyspaceName){
        return storedProcedure.selectAllProcedure(keyspaceName);
    }

    public Video selectById(UUID videoId) {
        return storedProcedure.selectByIdProcedure(videoId);
    }

    public void deleteAll() {
        dropVideoTable();
        createTable();
    }

    public void deleteById(UUID videoId) {
        storedProcedure.deleteByIdProcedure(videoId);
    }


    @Slf4j
    @Component
    protected static class StoredProcedure {

        private CqlSession session;

        public StoredProcedure(@Qualifier("sessionAws") CqlSession session) {
            this.session = session;
        }

        public UUID insertVideoProcedure(Video video, String keyspaceName){
            UUID videoId = UUID.randomUUID();

            video.setId(videoId);

            RegularInsert insertInto = QueryBuilder.insertInto(TABLE_NAME)
                    .value("id", QueryBuilder.bindMarker())
                    .value("title", QueryBuilder.bindMarker())
                    .value("creationDate", QueryBuilder.bindMarker());

            SimpleStatement insertStatement = insertInto.build();

            if(keyspaceName != null){
                insertStatement.setKeyspace(keyspaceName);
            }

            PreparedStatement preparedStatement = session.prepare(insertStatement);

            BoundStatement statement = preparedStatement.bind()
                    .setUuid(0, video.getId())
                    .setString(1, video.getTitle())
                    .setInstant(2, video.getCreationDate());

            session.execute(statement);

            return videoId;
        }

        public List<Video> selectAllProcedure(String keyspaceName){
            Select select = QueryBuilder.selectFrom(TABLE_NAME).all();

            ResultSet resultSet = executeStatement(select.build(), keyspaceName);

            ArrayList<Video> videos = new ArrayList<>();

            resultSet.forEach(x -> videos.add(new Video(x.getUuid("id"), x.getString("title"), x.getInstant("creationDate"))));
            return videos;
        }

        public Video selectByIdProcedure(UUID videoId) {
            Select select = QueryBuilder.selectFrom(TABLE_NAME)
                    .all()
                    .whereColumn("id")
                    .isEqualTo(QueryBuilder.bindMarker());

            PreparedStatement preparedStatement = session.prepare(select.build());
            ResultSet resultSet = session.execute(preparedStatement.bind().setUuid(0, videoId));

            Row result = resultSet.one();

            return new Video(result.getUuid("id"), result.getString("title"), result.getInstant("creationDate"));
        }

        public void deleteByIdProcedure(UUID videoId) {
            Delete delete = QueryBuilder.deleteFrom(TABLE_NAME).whereColumn("id").isEqualTo(QueryBuilder.bindMarker());
            PreparedStatement preparedStatement = session.prepare(delete.build());
            BoundStatement statement = preparedStatement.bind().setUuid(0, videoId);

            session.execute(statement);
        }


        private ResultSet executeStatement(SimpleStatement statement, String keyspaceName) {
            if(keyspaceName != null){
                statement.setKeyspace(keyspaceName);
            }
            return session.execute(statement);
        }
    }
}
