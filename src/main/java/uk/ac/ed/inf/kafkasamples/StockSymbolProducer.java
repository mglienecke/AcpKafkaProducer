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
        if (args.length != 1) {
            System.out.println("Please provide the configuration file path as a command line argument");
            System.exit(1);
        }

        var producer = new ...
        producer....
    }

    /**
     * Loads configuration properties from the specified file.
     * This method reads a properties file and returns the configuration as a {@link Properties} object.
     * If the file does not exist, an {@link IOException} is thrown.
     *
     * @param configFile the path to the configuration file to be loaded
     * @return a {@link Properties} object containing the configurations read from the file
     * @throws IOException if the configuration file cannot be found or read
     */
    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }

    public final String StockSymbolsConfig = "stock.symbols";
    public final String KafkaTopicConfig = "kafka.topic";
    public final String KafkaContinuousTopicConfig = "send.operation.continuous";

    /**
     * Processes the given configuration file to produce Kafka events based on stock symbols.
     * It reads configurations, initializes a Kafka producer, and sends messages to a Kafka topic.
     * Depending on the configuration, it can operate in continuous or single-record mode.
     *
     * @param configFileName the path to the configuration file containing Kafka and application settings
     * @throws IOException if an error occurs while loading the configuration file
     * @throws InterruptedException if the processing thread is interrupted
     */
    private void process(String configFileName) throws IOException, InterruptedException {
        Properties kafkaPros = StockSymbolProducer.loadConfig(configFileName);

        //TODO: Create a new producer

        String[] symbols = ((String) kafkaPros.get(StockSymbolsConfig)).split(",");

        final String topic = kafkaPros.getProperty(KafkaTopicConfig);
        final boolean continuousMode = Boolean.parseBoolean(kafkaPros.getProperty(KafkaContinuousTopicConfig));

        try {
            int valueCounter = 1;

            while (true) {
                final String key = symbols[new Random().nextInt(symbols.length)];
                final String value = String.valueOf(valueCounter++);

                // TODO: Send a new record to the topic and wait for the ACK (get)
                producer.XXX(new YYY...<>(topic, key, value), (recordMetadata, ex) -> {
                    if (ex != null)
                        ex.printStackTrace();
                    else
                        System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topic, key, value);
                }).get(5000, TimeUnit.MILLISECONDS);

                if (! continuousMode) {
                    break;
                }

                // TODO: Sleep 100 msec in current thread
                ...
            }

            System.out.println("1 record sent to Kafka");
        } catch (ExecutionException e) {
            System.err.println("execution exc: " + e);
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            System.err.println("timeout exc: " + e);
        }

        // TODO: Flush and close
    }
}

