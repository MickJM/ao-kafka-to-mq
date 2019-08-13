package maersk.com.kafka.mq;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.MQRFH2;

@Component
@DependsOn("queuemanager")
public class MQProducer implements ApplicationListener<ContextRefreshedEvent> { 

	private Logger log = Logger.getLogger(this.getClass());

	@Autowired
	private MQConnection conn;

	@Autowired 
	private MQQueueManager queueManager;
	
	//@Autowired
	private MQQueue queue;
	
	private MQPutMessageOptions pmo;
	
	@Value("${application.debug:false}")
	private boolean _debug;		
    //@Value("${ibm.mq.mqmd.rfh2.include:false}")
	//private boolean includeRFH2;  
    
    /*
     * 1 - REQUEST
     * 8 - DATAGRAM
     */
    //@Value("${ibm.mq.mqmd.messageType.reqType:8}")
	//private int reqType;	
    //@Value("${ibm.mq.mqmd.messageType.replyToQM:}")
	//private String replyToQM;	
    //@Value("${ibm.mq.mqmd.messageType.replyToQueue:}")
	//private String replyToQueue;	
    
    /*
     * Message Expiry - in 10ths of seconds
     * -1 - Unlimited Expiry
     */    
    //@Value("${ibm.mq.mqmd.expiry:-1}")
	//private int msgExpiry;	
    
    //@Value("${ibm.mq.retries.maxAttempts:3}")
	//private int maxAttempts;	
    
	public MQProducer() {
	};
		

	/*
	 * Not needed, but will keep in ...
	 */
	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
	}
	

	/*
	 * Build a message and write to MQ
	 */
	public void buildMessage(ConsumerRecord<?,?> consumerRecord) throws IOException, MQException, InterruptedException, MQDataException {

		this.conn.SendMessage(consumerRecord);
		
	}
	
	
}
