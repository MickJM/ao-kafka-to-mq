package maersk.com.kafka.mq;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
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
public class MQProducer {

	private Logger log = Logger.getLogger(this.getClass());

	@Autowired
	private MQConnection conn;

	@Autowired 
	private MQQueueManager queueManager;
	
	@Autowired
	private MQQueue queue;
	
	private MQPutMessageOptions pmo;
	
	@Value("${application.debug:false}")
	private boolean _debug;		
    @Value("${ibm.mq.mqmd.rfh2.include:false}")
	private boolean includeRFH2;  
    
    /*
     * 1 - REQUEST
     * 8 - DATAGRAM
     */
    @Value("${ibm.mq.mqmd.messageType.reqType:8}")
	private int reqType;	
    @Value("${ibm.mq.mqmd.messageType.replyToQM:}")
	private String replyToQM;	
    @Value("${ibm.mq.mqmd.messageType.replyToQueue:}")
	private String replyToQueue;	
    
    /*
     * Message Expiry - in 10ths of seconds
     * -1 - Unlimited Expiry
     */    
    @Value("${ibm.mq.mqmd.expiry:-1}")
	private int msgExpiry;	
    
	public MQProducer() {
		
		log.info("Creating MQProducer object ");
		//MQQueue queue = Manager.accessQueue("KAFKA.IN",MQConstants.MQOO_OUTPUT);
		
	};
	
	
	//@PostConstruct
	//private void OpenQueue() {
	//	log.info("******** Opening queue *********** - this.queue: " + this.queue);

    //		this.queue = this.conn.OpenQueueForWriting();

	//}
	
	/*
	 * If less than unlimited (-1) or zero - then set as unlimted
	 */
	@PostConstruct
	private void ValidateExpiry() {
	
		if ((this.msgExpiry < MQConstants.MQEI_UNLIMITED) || (this.msgExpiry == 0)) {
			log.info("Message expiry is invalid, resetting to UNLIMITED ");
			this.msgExpiry = MQConstants.MQEI_UNLIMITED;
		}
		
		if (this.msgExpiry != MQConstants.MQEI_UNLIMITED) {
			if (this.msgExpiry < 30) {
				log.warn("Message expiry is in 10ths ; low ( < 30 ) : "  + this.msgExpiry);
				
			}
			if ((this.msgExpiry >= 30) && (this.msgExpiry < 100)) {
				log.warn("Message expiry is in 10ths ; medium ( >= 30 < 100 ) : "  + this.msgExpiry);
				
			}
		}
		
	}
	
	@PostConstruct
	private void ValidateReplyToQueueManager() {

		this.pmo = new MQPutMessageOptions();
		
		if (this._debug) { log.info("Request message type : " + reqType); } 
		if (!(this.reqType != MQConstants.MQMT_DATAGRAM) 
				|| (!(this.reqType != MQConstants.MQMT_REQUEST))) {
			log.info("Request message type is valid");
		
		} else {
			log.error("Request message type mismatch; 0 - NONE, 1 - REQUEST, 8 - DATAGRAM");
			System.exit(1);
		}
		
		// Only check for reply to queue being empty, as the reply-to-queuemanager can be empty
		if (this.reqType == MQConstants.MQMT_REQUEST) {
			if (this.replyToQueue.isEmpty()) {
				log.error("ReplyToQueue is missing, for request type REQUEST");
				System.exit(1);
				
			}
		} 
		
	}

	
	//public void WriteMessage(String message) throws IOException, MQException {
	public void WriteMessage(ConsumerRecord<?,?> consumerRecord) throws IOException, MQException {
		
		/*
		if (this.conn.needToReconnect()) {		
			log.info("Creating queue object ... " + Thread.currentThread().getName());

			try {
				this.conn.reconnect();

			} catch (MQDataException e) {
				log.error("Unable to reconnect to queue manager ...");
				System.exit(1);
			}
			this.queue = this.conn.OpenQueueForWriting();

		}		
		*/
		
		
		MQMessage newmsg = new MQMessage();		

		if (this.includeRFH2) {
		    MQRFH2 rfh2 = new MQRFH2();
		    rfh2.setEncoding(MQConstants.MQENC_NATIVE);
		    rfh2.setCodedCharSetId(MQConstants.MQCCSI_INHERIT);
		    rfh2.setFormat(MQConstants.MQFMT_STRING);
		    rfh2.setNameValueCCSID(1208);
		    rfh2.setFlags(0);
	
		    rfh2.setFieldValue("mcd", "Msd", "");
		    rfh2.setFieldValue("jms", "Dst", "");
		    
			rfh2.setFieldValue("usr", "source-topic", consumerRecord.topic());
		    
			if (consumerRecord.key() != null) {
				rfh2.setFieldValue("usr", "key", consumerRecord.key());
			} else {
				rfh2.setFieldValue("usr", "key", "null");			
			}
		    rfh2.write(newmsg);
	
		    String message = (String) consumerRecord.value();
		    newmsg.write(message.getBytes());
		    
		    //newmsg.format  		= MQConstants.MQFMT_STRING;
			//newmsg.feedback         = MQConstants.MQFB_NONE;
			newmsg.messageId 		= MQConstants.MQMI_NONE;
			newmsg.correlationId 	= MQConstants.MQCI_NONE;
			newmsg.messageType      = MQConstants.MQMT_DATAGRAM;
			newmsg.format 			= MQConstants.MQFMT_RF_HEADER_2;
		} else {

		    String message = (String) consumerRecord.value();
		    newmsg.write(message.getBytes());

			newmsg.format  		= MQConstants.MQFMT_STRING;
			//newmsg.feedback         = MQConstants.MQFB_NONE;
			newmsg.messageId 		= MQConstants.MQMI_NONE;
			newmsg.correlationId 	= MQConstants.MQCI_NONE;
			newmsg.messageType      = MQConstants.MQMT_DATAGRAM;
			
		}
		
		if (this.reqType == MQConstants.MQMT_REQUEST) {
			newmsg.messageType      		= MQConstants.MQMT_REQUEST;
			newmsg.replyToQueueManagerName  = this.replyToQM;
			newmsg.replyToQueueName  		= this.replyToQueue;
			
		}
				
		newmsg.expiry = this.msgExpiry;
		
		this.pmo = new MQPutMessageOptions();	
		this.pmo.options = MQConstants.MQPMO_NEW_MSG_ID + MQConstants.MQPMO_FAIL_IF_QUIESCING;
		
		if (this._debug) {
			log.info("Queue manager objected is created ");
			//log.info("Connected to queue manager : " + this.queueManager.isConnected());
			log.info("Attempting to write to queue ...");				
		}

		this.queue.put(newmsg, pmo); 

		if (this._debug) {
			log.info("Message written to queue successully ...");
		}
		
		
	}
}
