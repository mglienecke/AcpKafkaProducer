package uk.ac.ed.inf.kafkasamples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class StockSymbolProducer {

    /**
     * The main method to start the stock symbol producer application.
     * It validates the input arguments, initializes a StockSymbolProducer instance,
     * and starts processing the configuration file to produce events to Kafka.
     *
     * @param args an array of command-line arguments where the first and only argument
     *             is expected to be the path to the configuration file.
     * @throws IOException if there is an error reading the configuration file.
     * @throws InterruptedException if the processing is interrupted during execution.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        new StockSymbolProducer().process();
    }

    public final String StockSymbolsConfig = "stock.symbols";
    public final String KafkaTopicConfig = "kafka.topic";
    public final String KafkaContinuousTopicConfig = "send.operation.continuous";

    /**
     * Processes the given configuration file to produce Kafka events based on stock symbols.
     * It reads configurations, initializes a Kafka producer, and sends messages to a Kafka topic.
     * Depending on the configuration, it can operate in continuous or single-record mode.
     *
     * @throws InterruptedException if the processing thread is interrupted
     */
    private void process() throws InterruptedException {
        Properties kafkaPros = getProperties();
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaPros);

        String[] symbols = ((String) kafkaPros.get(StockSymbolsConfig)).split(",");

        final String topic = kafkaPros.getProperty(KafkaTopicConfig);
        final boolean continuousMode = Boolean.parseBoolean(kafkaPros.getProperty(KafkaContinuousTopicConfig, "false"));
        System.out.println("continuous mode : " + continuousMode);

        try {
            int valueCounter = 1;

            while (true) {
                final String key = symbols[new Random().nextInt(symbols.length)];
                final String value = String.valueOf(valueCounter++);
                producer.send(new ProducerRecord<>(topic, key, value), (recordMetadata, ex) -> {
                    if (ex != null)
                        ex.printStackTrace();
                    else
                        System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topic, key, value);
                }).get(5000, TimeUnit.MILLISECONDS);

                if (! continuousMode) {
                    break;
                }
                Thread.sleep(100);
            }

            System.out.println("1 record sent to Kafka");
        } catch (ExecutionException e) {
            System.err.println("execution exc: " + e);
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            System.err.println("timeout exc: " + e);
        }

        producer.flush();
        producer.close();
    }

    private Properties getProperties() {
        Properties kafkaPros = new Properties();
        kafkaPros.put("bootstrap.servers", "kafka:9093");
        // kafkaPros.put("bootstrap.servers", "127.0.0.1:9092");

        kafkaPros.put("group.id", "ACP_CW2_Group");
        kafkaPros.put("acks", "all");
        kafkaPros.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaPros.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaPros.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaPros.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaPros.put("kafka.topic", "stockvalues");
        kafkaPros.put("stock.symbols", "AAPL,MSFT,GOOG,AMZN,TSLA,JPMC,CATP,UNIL,LLOY");
        kafkaPros.put("send.operation.continuous", "true");
        return kafkaPros;
    }
}

