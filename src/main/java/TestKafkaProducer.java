import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TestKafkaProducer {

    static Logger logger = LoggerFactory.getLogger(TestKafkaProducer.class);

    private Properties kafkaProps = new Properties();

    private KafkaProducer<String, String> producer;

    public TestKafkaProducer() {
        kafkaProps.put("bootstrap.servers",
                "localhost:9092,localhost:9093,localhost:9094");
        kafkaProps.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(kafkaProps);
    }

    // fire and forget
    public void send() {
        ProducerRecord<String, String> record
                = new ProducerRecord<>("test", "myKey", "myValue");
        try {
            logger.info("Sending message...");
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // synchronous send
    public void syncSend() {
        ProducerRecord<String, String> record
                = new ProducerRecord<>("test", "myKey", "myValue");
        try {
            logger.info("Sending message...");
            RecordMetadata metadata = producer.send(record).get();
            logger.info("Sent message to topic {}, partition {}, offset {}",
                    metadata.topic(), metadata.partition(), metadata.offset());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // asynchronous send
    public void asyncSend() {
        ProducerRecord<String, String> record
                = new ProducerRecord<>("test", "myKey", "myValue");
        logger.info("Sending message...");
        producer.send(record, new TestProducerCallback());
    }

    public void close() {
        producer.close();
    }

}
