package producer_package;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer_demo {


    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Producer_demo.class);

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        // create a producer record
        for (int i = 0; i < 5; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topics", "hello world1"+ Integer.toString(i));
            // send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        // the record was successfully sent
                        logger.info("Receive new data.\n" +
                                "The topic is : " + recordMetadata.topic() + "\n" +
                                "The partition is : " + recordMetadata.partition() + "\n" +
                                "The offset is : " + recordMetadata.offset());
                    } else {
                        logger.error("Error while producing " + e);
                    }
                }
            });


        }
        producer.flush();
        producer.close();
    }
}