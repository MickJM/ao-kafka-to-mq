package maersk.com.kafka.consumer;

import java.io.IOException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.ibm.mq.MQException;
import com.ibm.mq.constants.MQConstants;

import maersk.com.kafka.mq.MQProducer;

@Component
public class KafkaConsumer {

	private Logger log = Logger.getLogger(this.getClass());

	
	@Autowired
	private MQProducer mqproducer;
	
	@KafkaListener(topics = "${kafka.src.topic}" ) 
//			partitionOffsets = @PartitionOffset(initialOffset = "0", partition = "0")) )
    public void listen(ConsumerRecord<?,?> consumerRecord, Acknowledgment ack) {
		
		if (this.mqproducer != null) {
			log.info("Kafka Listener .... MQProducer exists");
			try {
				String msg = (String) consumerRecord.value();
				this.mqproducer.WriteMessage(consumerRecord);
		//		ack.acknowledge();
				
				
			} catch (IOException e) {
				log.error("Error writting message : " + e.getMessage());
			
			} catch (MQException e) {
				log.error("Error writting message : " + e.reasonCode);
				if (e.reasonCode == MQConstants.MQRC_Q_FULL) {
					log.error("Queue is full ...");
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
