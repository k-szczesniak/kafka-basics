package pl.ks.kafka;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.ks.kafka.config.KafkaConsumerFactory;
import pl.ks.kafka.config.OpenSearchClientFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;

public class OpenSearchConsumer {

    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        // first create an OpenSearch Client
        RestHighLevelClient openSearchClient = OpenSearchClientFactory.createOpenSearchClient();
        KafkaConsumer<String, String> consumer = KafkaConsumerFactory.createKafkaConsumer();

        try (openSearchClient; consumer) {

            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            // we need to create the index on OpenSearch if it doesn't exist already
            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The Wikimedia Index has been created!");
            } else {
                log.info("The Wikimedia Index already exits");
            }

            // we subscribe the consumer
            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Received " + recordCount + " record(s)");

                for (ConsumerRecord<String, String> record : records) {


                    // strategy 1
                    // define an ID using Kafka Record coordinates
                    // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    try {
                        // strategy 2
                        // we extract the ID from the JSON value
                        String id = extractId(record.value());

                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);
                        // by adding id we guarantee idempotence, because we will process (which in this case is saving to index)
                        // messages only once, when there will be doc with the same id, then doc will be updated, not created a new one

                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                        log.info(response.getId());
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                }

            }

        }
    }

    private static String extractId(String json){
        // gson library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }
}