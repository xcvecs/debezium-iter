package top.byteinfo;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSource;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.engine.spi.OffsetCommitPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Demo {
    private static final Logger LOGGER = LoggerFactory.getLogger(Demo.class);
    public static void main(String[] args) {
        cdc();
    }

    public static void cdc() {
        Configuration config = Configuration.create()
                /* begin engine properties */
                .with("connector.class",
                        "io.debezium.connector.mysql.MySqlConnector")
                .with("offset.storage",
                        "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename",
                        "src/main/resources/static/offsets.dat")
                .with("offset.flush.interval.ms", 10)
                /* begin connector properties */
                .with("name", "my-sql-connector")
                .with("database.hostname", "localhost")
                .with("database.port", 3306)
                .with("database.user", "root")
                .with("database.password", "root")
                .with("database.server.id", 85744)
                .with("database.server.name", "my-app-connector")
                .with("database.history",
                        "io.debezium.relational.history.FileDatabaseHistory")
                .with("database.history.file.filename",
                        "src/main/resources/static/dbhistory.dat")
                .with("max.batch.size",128)
                .build();
        Properties properties = config.asProperties();


// Create the engine with this configuration ...
        try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(properties)
                .using(OffsetCommitPolicy.always())
                .notifying(new JsonChangeConsumer()).build()
        ) {
            // Run the engine asynchronously ...
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(engine);

            // Do something else or wait for a signal or an event
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
// Engine is stopped when the main code is finished

    }


    public   static class JsonChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>> {

        MySqlStreamingChangeEventSource.BinlogPosition binlogPosition;
        @Override
        public void handleBatch(List<ChangeEvent<String, String>> changeEventList,
                                DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> recordCommitter)
                throws InterruptedException {


            LOGGER.info("batch"+String.valueOf(changeEventList.size()));
            for (ChangeEvent<String, String> changeEvent : changeEventList) {
                String value = changeEvent.value();
                LOGGER.info(value);
                recordCommitter.markProcessed(changeEvent);
            }
            recordCommitter.markBatchFinished();
            throw new RuntimeException("debug");

        }

    }

}
