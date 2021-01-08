package maersk.com.kafka.mq;

/*
 * Write messages to a queue manager 
 * 
 *  Copyright Maersk 2019
 */

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.MQRFH2;

import maersk.com.kafka.constants.MQKafkaConstants;

@Component
@DependsOn("queuemanager")
public class MQProducer implements ApplicationListener<ContextRefreshedEvent> { 

	protected Logger log = Logger.getLogger(this.getClass());
	@Value("${application.debug:false}")
	private boolean _debug;		
	
	@Autowired
	private MQConnection conn;

	@Autowired 
	private MQQueueManager queueManager;
	
	private MQQueue queue;

	public MQProducer() {
	};
		

	/*
	 * Not needed, but will keep in ...
	 */
	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
	}

	/*
	 * Send the message to the connected queue manager
	 */
	public void buildMessage(ConsumerRecord<?,?> consumerRecord) throws IOException, MQException, InterruptedException, MQDataException {
		this.conn.sendMessage(consumerRecord);
			
	}
	
	
}
