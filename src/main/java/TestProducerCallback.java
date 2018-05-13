import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestProducerCallback implements Callback {

    static Logger logger = LoggerFactory.getLogger(TestProducerCallback.class);

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            e.printStackTrace();
        } else {
            logger.info("Sent message to topic {}, partition {}, offset {}",
                    recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
        }
    }

}
