package wang.ray.sample.old.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 旧版kafka java client producer
 *
 * @author ray
 */
public class kafkaProducer extends Thread {

    private String topic;

    public kafkaProducer(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public void run() {
        Producer producer = createProducer();
        int i = 0;
        while (true) {
            String message = "message-" + i++;
            producer.send(new KeyedMessage<Integer, String>(topic, message));
            System.out.println("producer message: " + message);
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private Producer createProducer() {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9092,localhost:9093,localhost:9094");
        properties.put("serializer.class", StringEncoder.class.getName());
        return new Producer<Integer, String>(new ProducerConfig(properties));
    }

    public static void main(String[] args) {
        new kafkaProducer("test").start();
    }
}
