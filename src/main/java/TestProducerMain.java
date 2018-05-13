public class TestProducerMain {

    public static void main(String[] args) {
        TestKafkaProducer producer = new TestKafkaProducer();
        producer.send();
//        producer.syncSend();
//        producer.asyncSend();
        producer.close();
    }

}
