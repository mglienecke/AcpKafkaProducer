package uk.ac.ed.inf.kafkasamples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class StockSymbolProducer {

    private static RuntimeEnvironmentSettings settings = checkEnvironment();

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

        try (JedisPool pool = new JedisPool(settings.getRedisHost(), settings.getRedisPort());
                Jedis jedis = pool.getResource()) {
            final Logger logger = LoggerFactory.getLogger(StockSymbolProducer.class);

            logger.info("Redis connection established");

            // Store & Retrieve a simple string
            jedis.set("foo", "bar");
            System.out.println(jedis.get("foo")); // prints bar

            // Store & Retrieve a HashMap
            Map<String, String> hash = new HashMap<>();
            ;
            hash.put("name", "Michel");
            hash.put("surname", "Glienecke");
            hash.put("company", "On my own");
            hash.put("age", "xx");
            jedis.hset("user-session:123", hash);
            System.out.println(jedis.hgetAll("user-session:123"));


            jedis.set("myKey", "{ \"a\": 1 }");
            System.out.println(jedis.get("myKey"));

            // Prints: {name=John, surname=Smith, company=Redis, age=29}
        }

        new StockSymbolProducer().process();
    }

    private static RuntimeEnvironmentSettings checkEnvironment() {
        RuntimeEnvironmentSettings settings = new RuntimeEnvironmentSettings();

        if (System.getenv("KAFKA_BOOTSTRAP_SERVERS") == null) {
            throw new RuntimeException("KAFKA_BOOTSTRAP_SERVERS environment variable not set");
        }
        settings.setKafkaBootstrapServers(System.getenv("KAFKA_BOOTSTRAP_SERVERS"));

        if (System.getenv("REDIS_HOST") == null) {
            throw new RuntimeException("REDIS_HOST environment variable not set");
        }
        settings.setRedisHost(System.getenv("REDIS_HOST"));

        if (System.getenv("REDIS_PORT") == null) {
            throw new RuntimeException("REDIS_PORT environment variable not set");
        }
        settings.setRedisPort(Integer.parseInt(System.getenv("REDIS_PORT")));

        if (System.getenv("RABBITMQ_HOST") == null) {
            throw new RuntimeException("RABBITMQ_HOST environment variable not set");
        }
        settings.setRabbitmqHost(System.getenv("RABBITMQ_HOST"));

        if (System.getenv("RABBITMQ_PORT") == null) {
            throw new RuntimeException("RABBITMQ_PORT environment variable not set");
        }
        settings.setRabbitmqPort(Integer.parseInt(System.getenv("RABBITMQ_PORT")));

        return settings;
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
        // kafkaPros.put("bootstrap.servers", "acp-kafka-1:9093");
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

