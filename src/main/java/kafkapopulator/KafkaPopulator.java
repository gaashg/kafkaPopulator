package kafkapopulator;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaPopulator {
    private static Logger LOGGER = LoggerFactory.getLogger(KafkaPopulator.class);

    public static void main(String[] args) {
        Map<String, String> params = argsToMap(args);
        LOGGER.info("Received parameters: {}", params);

        if (Boolean.parseBoolean(params.get("createTopic")))
            createTopic(params);

        if (Boolean.parseBoolean(params.get("sendMessages")))
            sendMessages(params);

        LOGGER.info("Bye bye...");
    }

    private static void sendMessages(Map<String, String> params) {
        LOGGER.info("Going to send messages");

        String topicName = params.get("topicName");
        validateProperty("topicName", topicName);
        String filePath = params.get("filePath");
        validateProperty("filePath", filePath);
        int sleepTime = !StringUtils.isBlank(params.get("sendSleepIntervalMillis")) ? Integer.parseInt(params.get("sendSleepIntervalMillis")) : 0;

        Producer kafkaProducer = createKafkaProducer(params);
        //Read file location.
        String line = null;

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            while ((line = br.readLine()) != null) {
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, 0, System.currentTimeMillis(),
                        String.valueOf(line.hashCode()), line);
                LOGGER.info("Sending record {}", record);
                kafkaProducer.send(record);
                if (sleepTime > 0) {
                    try {
                        LOGGER.info("Going to sleep for {} seconds", sleepTime);
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e) {
                        LOGGER.info("Interrupted...");
                    }
                }
            }
            LOGGER.info("Sent all messages");
        } catch (IOException ioe){
            LOGGER.error("Failed to read file {}", filePath, ioe);
        } finally {
            if (kafkaProducer != null)
                kafkaProducer.close();
        }

    }

    private static KafkaProducer createKafkaProducer(Map<String, String> params) {
        // create instance for properties to access producer configs
        Properties props = new Properties();

        String hostname = params.get("kafkaHostname");
        validateProperty("kafkaHostname", params.get("kafkaHostname"));

        //Assign localhost id
        props.put("bootstrap.servers", hostname + ":9092");

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer <String, String>(props);
    }

    private static void createTopic(Map<String, String> params) {
        Properties props = new Properties();
        String hostname = params.get("kafkaHostname");
        validateProperty("kafkaHostname", params.get("kafkaHostname"));
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, hostname + ":9092");
        AdminClient adminClient = AdminClient.create(props);

        String topicName = params.get("topicName");
        validateProperty("topicName", topicName);
        String partitionsNum = params.get("partitionsNum");
        validateProperty("partitionsNum", partitionsNum);

        LOGGER.info("Creating new topic; topicName={}, partitionsNum={}", topicName, partitionsNum);
        NewTopic topic = new NewTopic(topicName, Integer.parseInt(partitionsNum), (short)1);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(topic));

        try {
            createTopicsResult.all().get();
            LOGGER.info("Topic created successfully; topicName={}", topicName);
            // real failure cause is wrapped inside the raised ExecutionException
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                LOGGER.error("Topic already exists !! Continuing execution", e);
            } else if (e.getCause() instanceof TimeoutException) {
                LOGGER.error("Timeout !!", e);
                throw new RuntimeException(e);
            }
        } catch (InterruptedException e) {
            LOGGER.error("Failure", e);
            throw new RuntimeException(e);
        } finally {
            adminClient.close();
        }
    }

    private static void validateProperty(String key, String value){
        if (StringUtils.isBlank(value))
            throw new RuntimeException("No value exists for property " + key);
    }

    private static Map<String, String> argsToMap(String[] args) {
        String[] splitParam = null;
        Map<String, String> params = new HashMap<String, String>();
        for (String param : args) {
            splitParam = param.split("=");
            params.put(splitParam[0], splitParam[1]);
        }

        return params;
    }
}
