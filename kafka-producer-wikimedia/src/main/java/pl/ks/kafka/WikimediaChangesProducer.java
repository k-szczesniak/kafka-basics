package pl.ks.kafka;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import pl.ks.kafka.config.KafkaProducerFactory;

import java.net.URI;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {
        KafkaProducer<String, String> producer = KafkaProducerFactory.createProducer();

        String topic = "wikimedia.recentchange";

        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        BackgroundEventSource eventSource = new BackgroundEventSource.Builder(eventHandler,
                new EventSource.Builder(ConnectStrategy.http(URI.create(url))))
                .build();

        eventSource.start();

        // we produce for 10 minutes and block the program until then
        TimeUnit.MINUTES.sleep(10);
    }
}