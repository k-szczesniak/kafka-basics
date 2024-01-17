package pl.ks.kafka.utils;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataProcessor {

    private static final Logger log = LoggerFactory.getLogger(DataProcessor.class.getSimpleName());

    public static BulkRequest process(ConsumerRecords<String, String> records) {
        log.info("Received " + records.count() + " record(s)");
        return prepareBulkRequest(records);
    }

    private static BulkRequest prepareBulkRequest(ConsumerRecords<String, String> records) {
        BulkRequest bulkRequest = new BulkRequest();
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

                //  IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                //  log.info(response.getId());

                bulkRequest.add(indexRequest);

            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }

        return bulkRequest;
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
