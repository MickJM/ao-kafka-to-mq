package maersk.com.kafka.consumer;

/*
 * Kafka Consumer
 * 
 *  Copyright Maersk 2019
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.tomcat.jni.Thread;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import maersk.com.kafka.AoKafkaToMqApplication;
import maersk.com.kafka.constants.MQKafkaConstants;
//import maersk.com.kafka.constants.MQKafkaConstants.MQKafka;
import maersk.com.kafka.mq.MQConnection;
import maersk.com.kafka.mq.MQProducer;

@Component
public class KafkaConsumer {

	private Logger log = Logger.getLogger(this.getClass());
	
	@Value("${application.debug:false}")
    private boolean _debug;

	@Autowired
    private ApplicationContext context;
		
	@Autowired
	private MQProducer mqproducer;
	
	@Autowired
	private MQConnection conn;
		
	/*
	 * Create a Kafka listener and pass the consumerRecord to MQ producer object 
	 */
	@KafkaListener(topics = "${kafka.src.topic}" ) 
    public void listen(ConsumerRecord<?,?> consumerRecord, Acknowledgment ack) throws 
    		InterruptedException, 
    		MQDataException {
		
		if (this._debug) { log.info("Attempting to write message ..."); }
		if (this.mqproducer != null) {
			if (this._debug) { log.info("Kafka Listener .... MQProducer exists"); }
			try {
				String msg = (String) consumerRecord.value();
				this.mqproducer.buildMessage(consumerRecord);
				if (ack != null) {
					ack.acknowledge();
				}
				
			} catch (IOException e) {
				log.error("Error writting message : " + e.getMessage());
				System.exit(MQKafkaConstants.EXIT);
				
			} catch (MQException e) {
				log.error("Error occurred writing messages to MQ : " + e.reasonCode);
				log.error("See the logs for additional details : " + e.getMessage());
				System.exit(MQKafkaConstants.EXIT);
			}
		}		
    }
}
