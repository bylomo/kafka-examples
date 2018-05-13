public class TestConsumerMain {

    public static void main(String[] args) {
        TestKafkaConsumer consumer = new TestKafkaConsumer();
//        consumer.consume();
//        consumer.consumeWithCommitSync();
//        consumer.consumeWithCommitAysnc();
        consumer.consumeWithCommitAsyncAndSync();
    }

}
