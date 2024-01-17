package pl.ks.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.opensearch.action.bulk.BulkRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.ks.kafka.config.KafkaConsumerFactory;
import pl.ks.kafka.utils.DataProcessor;
import pl.ks.kafka.utils.OpenSearchUtil;

import java.time.Duration;
import java.util.Collections;

public class OpenSearchConsumer {

    public static void main(String[] args) {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
        KafkaConsumer<String, String> consumer = KafkaConsumerFactory.createKafkaConsumer();

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
            consumer.wakeup();

            // join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                log.error("shutdownHook exception", e);
            }
        }));


        try (consumer) {
            OpenSearchUtil.createOpenSearchIndex();

            // we subscribe the topic
            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                BulkRequest bulkRequest = DataProcessor.process(records);
                boolean isProcessedCorrectly = OpenSearchUtil.persistData(bulkRequest);
                if (isProcessedCorrectly){
                    consumer.commitSync();
                    log.info("Offsets have been committed!");
                }
            }

        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); // close the consumer, this will also commit offsets
            log.info("The consumer is now gracefully shut down");
        }

    }

}