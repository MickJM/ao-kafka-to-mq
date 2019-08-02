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
    public void listen(ConsumerRecord<?,?> consumerRecord, Acknowledgment ack) {

		log.info("Attempting to write message ...");
		log.info("Thread name ... " + java.lang.Thread.currentThread().getName());

		
		if (this.mqproducer != null) {
			log.info("Kafka Listener .... MQProducer exists");
			try {
				String msg = (String) consumerRecord.value();
				this.mqproducer.WriteMessage(consumerRecord);
				if (ack != null) {
					ack.acknowledge();
				}
				
			} catch (IOException e) {
				log.error("Error writting message : " + e.getMessage());
			
			} catch (MQException e) {
				log.error("Error writting message : " + e.reasonCode);
				if (e.reasonCode == MQConstants.MQRC_Q_FULL) {
					log.error("Queue is full ...");
					System.exit(1);
				}
				if (e.reasonCode == MQConstants.MQRC_CONNECTION_BROKEN) {
					log.error("Queue manager was disconnected ...");
					this.conn.setNeedToReconnect(true);
				}

			}
		}
		
        //System.out.println("received message on " 
        //					+ consumerRecord.topic() 
        //					+ "- key:" 
        //					+ consumerRecord.key()
        //					+ " value: " + consumerRecord.value());

    }
	
}
