package wang.ray.sample.old.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class kafkaConsumer extends Thread {

    private String topic;
    private int threadCount;

    public kafkaConsumer(String topic, int threadCount) {
        super();
        this.topic = topic;
        this.threadCount = threadCount;
    }

    @Override
    public void run() {
        ConsumerConnector consumer = createConsumer();
        Map<String, Integer> topicCountMap = new HashMap<>();
        // 消费线程数，小于或等于分区数
        topicCountMap.put(topic, threadCount);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = messageStreams.get(topic);

        int threadNumber = 0;
        // 为每个stream启动一个线程消费消息
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerWork(stream, threadNumber++));
        }
    }

    class ConsumerWork implements Runnable {
        private final KafkaStream<byte[], byte[]> stream;
        private int threadNumber;

        public ConsumerWork(KafkaStream stream, int threadNumber) {
            this.stream = stream;
            this.threadNumber = threadNumber;
        }

        @Override
        public void run() {
            ConsumerIterator<byte[], byte[]> iter = stream.iterator();
            while (iter.hasNext()) {
                System.out.println(String.format("Thread %d consumed message: %s", threadNumber, new String(iter.next().message())));
            }
        }
    }


    private ConsumerConnector createConsumer() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "localhost:2181");
        properties.put("group.id", "group1");
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    public static void main(String[] args) {
        new kafkaConsumer("test", 1).start();// 使用kafka集群中创建好的主题 test
    }
}
