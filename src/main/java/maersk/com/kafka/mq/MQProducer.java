package maersk.com.kafka.mq;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

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
import com.ibm.mq.headers.MQRFH2;

@Component
@DependsOn("queuemanager")
public class MQProducer {

	private Logger log = Logger.getLogger(this.getClass());

	@Autowired
	private MQQueueManager queueManager;
	
	@Autowired
	private MQQueue queue;
	
	private MQPutMessageOptions pmo;
	
	@Value("${application.debug:false}")
	private boolean _debug;	
	
    @Value("${ibm.mq.include.rfh2:false}")
	private boolean includeRFH2;	
    
    
	public MQProducer() {
		
		log.info("Creating MQProducer object ");
		this.pmo = new MQPutMessageOptions();
		
	};
	
	//public void WriteMessage(String message) throws IOException, MQException {
	
	public void WriteMessage(ConsumerRecord<?,?> consumerRecord) throws IOException, MQException {
			
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
		
		this.pmo = new MQPutMessageOptions();	
		
		if (this._debug) {
			log.info("Queue manager objected is created ");
			log.info("Connected to queue manager : " + this.queueManager.isConnected());
			log.info("Attempting to write to queue ...");				
		}

		queue.put(newmsg, pmo); 

		if (this._debug) {
			log.info("Message written to queue successully ...");
		}
		
		
	}
}
