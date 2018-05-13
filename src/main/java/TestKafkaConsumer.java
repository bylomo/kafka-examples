import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TestKafkaConsumer {

    static Logger logger = LoggerFactory.getLogger(TestKafkaConsumer.class);

    private Properties kafkaProps = new Properties();

    private KafkaConsumer<String, String> consumer;

    public TestKafkaConsumer() {
        kafkaProps.put("bootstrap.servers",
                "localhost:9092,localhost:9093,localhost:9094");
        kafkaProps.put("group.id", "test");
        kafkaProps.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(kafkaProps);
    }

    // normal consume
    public void consume() {
        consumer.subscribe(Collections.singletonList("test"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

    // consume with commit sync latest offset of this batch
    public void consumeWithCommitSync() {
        consumer.subscribe(Collections.singletonList("test"));
        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
                try {
                    logger.info("commit sync");
                    consumer.commitSync();
                } catch (CommitFailedException e) {
                    logger.error("commit failed", e);
                }
            }
        } finally {
            consumer.close();
        }
    }

    // consume with commit async latest offset of this batch
    public void consumeWithCommitAysnc() {
        consumer.subscribe(Collections.singletonList("test"));
        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                        if (e != null) {
                            logger.error("Commit failed for offsets {}", offsets, e);
                        }
                    }
                });
            }
        } finally {
            consumer.close();
        }
    }

    public void consumeWithCommitAsyncAndSync() {
        consumer.subscribe(Collections.singletonList("test"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
                // commit async
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                        if (e != null) {
                            logger.error("Commit failed for offsets {}", offsets, e);
                        }
                    }
                });
            }
        } catch (Exception e) {
            logger.error("Unexpected error", e);
        } finally {
            try {
                // commit sync
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public void consumeWithCommitSpecifiedPartitionOffset() {
        consumer.subscribe(Collections.singletonList("test"));
        int count = 0;
        try {

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                    if (count % 1000 == 0) {
                        consumer.commitAsync(currentOffsets, null);
                        count++;
                    }
                }
            }
        } finally {
            try {
                consumer.commitSync(currentOffsets);
            } finally {
                consumer.close();
            }
        }
    }

    private class HandleRebalance implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            logger.warn("Lost partitions in rebalance. Committing current offsets: " + currentOffsets);
            consumer.commitSync(currentOffsets);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            // TODO
        }
    }

    // consume with rebalance listener
    public void consumeWithRebalanceListener() {
        consumer.subscribe(Collections.singletonList("test"), new HandleRebalance());
        int count = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                    if (count % 1000 == 0) {
                        consumer.commitAsync(currentOffsets, null);
                        count++;
                    }
                }
            }
        } catch (WakeupException e) {
            // ignore
        } catch (Exception e) {
            logger.error("Unexpected error", e);
        } finally {
            try {
                consumer.commitSync(currentOffsets);
            } finally {
                consumer.close();
            }
        }
    }

    private class SaveOffsetsOnRebalance implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
             commitDBTransaction();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                consumer.seek(partition, getOffsetFromDB(partition));
            }
        }
    }

    private void commitDBTransaction () {
        // TODO
    }

    private long getOffsetFromDB(TopicPartition partition) {
        // TODO
        return 0;
    }

    private void storeRecordInDB(ConsumerRecord record) {
        // TODO
    }

    private void storeOffsetsInDB(ConsumerRecord record) {
        // TODO
    }

    private void processRecord(ConsumerRecord record) {
        logger.info("topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
    }

    // consume with save offsets rebalance listener
    public void consumeWithSaveOffsetsRebalanceListener() {
        consumer.subscribe(Collections.singletonList("test"), new SaveOffsetsOnRebalance());

        consumer.poll(0);
        for (TopicPartition partition : consumer.assignment()) {
            consumer.seek(partition, getOffsetFromDB(partition));
        }

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                    storeRecordInDB(record);
                    storeOffsetsInDB(record);
                }
                commitDBTransaction();
            }
        } catch (Exception e) {
            logger.error("Unexpected error", e);
        } finally {
            consumer.close();
        }
    }

}
