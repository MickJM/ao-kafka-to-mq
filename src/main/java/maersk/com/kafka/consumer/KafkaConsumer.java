package maersk.com.kafka.consumer;

import java.io.IOException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.tomcat.jni.Thread;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.ibm.mq.MQException;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;

import maersk.com.kafka.AoKafkaToMqApplication;
import maersk.com.kafka.mq.MQConnection;
import maersk.com.kafka.mq.MQProducer;

@Component
public class KafkaConsumer {

	
	private Logger log = Logger.getLogger(this.getClass());

	@Autowired
    private ApplicationContext context;
		
	@Autowired
	private MQProducer mqproducer;
	
	@Autowired
	private MQConnection conn;
	
	
	@KafkaListener(topics = "${kafka.src.topic}" ) 
//			partitionOffsets = @PartitionOffset(initialOffset = "0", partition = "0")) )
    public void listen(ConsumerRecord<?,?> consumerRecord, Acknowledgment ack) throws InterruptedException, MQDataException {

		log.info("Attempting to write message ...");

		if (this.mqproducer != null) {
			log.info("Kafka Listener .... MQProducer exists");
			try {
				String msg = (String) consumerRecord.value();
				this.mqproducer.buildMessage(consumerRecord);
				if (ack != null) {
					ack.acknowledge();
				}
				
			} catch (IOException e) {
				log.error("Error writting message : " + e.getMessage());
			
			} catch (MQException e) {
				log.error("Error occurred writing messages to MQ : " + e.reasonCode);
				log.error("See the logs for additional details : " + e.getMessage());
				System.exit(1);

			}
		}
		
        //System.out.println("received message on " 
        //					+ consumerRecord.topic() 
        //					+ "- key:" 
        //					+ consumerRecord.key()
        //					+ " value: " + consumerRecord.value());

    }
	
}
