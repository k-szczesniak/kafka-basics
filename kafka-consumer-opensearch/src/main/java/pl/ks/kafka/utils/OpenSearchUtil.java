package pl.ks.kafka.utils;

import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.ks.kafka.config.OpenSearchClientFactory;

import java.io.IOException;

public class OpenSearchUtil {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchUtil.class.getSimpleName());
    private static final RestHighLevelClient openSearchClient = OpenSearchClientFactory.createOpenSearchClient();


    public static void createOpenSearchIndex() throws IOException {
        boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

        // we need to create the index on OpenSearch if it doesn't exist already
        if (!indexExists) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
            openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            log.info("The Wikimedia Index has been created!");
        } else {
            log.info("The Wikimedia Index already exits");
        }
    }

    public static boolean persistData(BulkRequest bulkRequest) throws IOException {
        if (bulkRequest.numberOfActions() > 0){
            BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            log.info("Inserted " + bulkResponse.getItems().length + " record(s).");

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("OpenSearchUtil#persistData", e);
            }
            return true;
        }
        return false;
    }

}
