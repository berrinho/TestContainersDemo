package com.testcontainers.demo;

import static com.datastax.driver.core.schemabuilder.SchemaBuilder.createKeyspace;
import static org.junit.Assert.assertTrue;

import java.sql.ResultSet;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;

import com.datastax.driver.core.schemabuilder.CreateKeyspace;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.metadata.token.ReplicationStrategy;

public class CassandraTest {

    static CassandraContainer<?> cassandra =
            new CassandraContainer<>("cassandra:4.1.2").withExposedPorts(9042);

    private static final Logger LOG = LoggerFactory.getLogger(CassandraTest.class);

    CqlSession session;
    static String TABLE_NAME = "external_app_transfers";
    static String KEYSPACE_NAME = "spreportingtest";

    @BeforeAll
    static void beforeAll() {
        cassandra.start();
    }

    @AfterAll
    static void afterAll() {
        cassandra.stop();
    }

    @BeforeEach
    void setUp() {
        session = CqlSession
                .builder()
                .addContactPoint(this.cassandra.getContactPoint())
                .withLocalDatacenter(this.cassandra.getLocalDatacenter())
                .build();

        StringBuilder sb =
                new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
                        .append(KEYSPACE_NAME).append(" WITH replication = {")
                        .append("'class':'").append("SimpleStrategy")
                        .append("','replication_factor':").append(1)
                        .append("};");

        String query = sb.toString();
        session.execute(query);

        createTable();
        insertData();
    }

    public void createTable() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(KEYSPACE_NAME).append(".")
                .append(TABLE_NAME).append("(")
                .append("id uuid PRIMARY KEY, ")
                .append("title text,")
                .append("subject text);");

        String query = sb.toString();
        session.execute(query);
    }

    public void insertData() {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(KEYSPACE_NAME).append(".")
                .append(TABLE_NAME).append("(")
                .append("id, ")
                .append("title,")
                .append("subject)")
                .append(" VALUES (")
                .append("6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47, 'KRUIKSWIJK','Steven');");

        String query = sb.toString();
        session.execute(query);
    }

    @Test
    void checkDockerImage() {
        LOG.debug("has docker image loaded?");
        assertTrue(cassandra.isRunning());

        StringBuilder select_query = new StringBuilder("SELECT * FROM ")
                .append(KEYSPACE_NAME).append(".")
                .append(TABLE_NAME)
                .append(";");

        var results = session.execute(select_query.toString());


        while (results.iterator().hasNext()) {
            //long id = results.iterator().next().getLong("id");
            var row = results.iterator().next();
            String title = row.getString("title");
            String subject = row.getString("subject");
            LOG.debug("id is , title is {}, subject is {}", title, subject);
        }
        LOG.debug(results.toString());
    }



}
