package producer_package;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {


    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
        String groupid = "my-fourth-application";
        String topic = "first_topics";
        // create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupid );
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest" );
        // create the producer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        // subscribe consumer to our topic
        //consumer.subscribe(Arrays.asList(topic));


        //read from a specific partition and offset
        TopicPartition partitionToreadfrom = new TopicPartition(topic, 0);
        long offsetToreadfrom = 15;
        consumer.assign(Arrays.asList(partitionToreadfrom));
        //seek
        consumer.seek(partitionToreadfrom, offsetToreadfrom);
        // poll for new data
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records){
                logger.info("Key: " + record.key() + ", Value" + record.value());
                logger.info("partition: " + record.partition() + ", offset" + record.offset());
            }
        }
    }
}
