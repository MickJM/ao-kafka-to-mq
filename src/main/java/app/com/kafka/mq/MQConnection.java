package app.com.kafka.mq;

/*
 * Connect to a queue manager
 * 
 *  Copyright Maersk 2019
 */

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDLH;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.MQHeaderList;
import com.ibm.mq.headers.MQRFH2;
import com.ibm.mq.headers.pcf.PCFMessageAgent;

import app.com.kafka.constants.MQKafkaConstants;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;

@Component
public class MQConnection {

	protected Logger log = Logger.getLogger(this.getClass());
	
    private Map<String,AtomicInteger>errors = new HashMap<String,AtomicInteger>();
	
	@Value("${application.debug:false}")
    private boolean _debug;

	@Value("${ibm.mq.queuemanager}")
	private String queueManager;
	public String GetQueueManagerName() { return this.queueManager; }
	public void SetQueueManagerName(String val) { this.queueManager = val; }
	
	// taken from connName
	private String hostName;
	public String GetHostName() { return this.hostName; }
	public void SetHostName(String val) { this.hostName = val; }

	// hostname(port)
	@Value("${ibm.mq.connName}")
	private String connName;	
	public String GetConnName() { return this.connName; }
	public void SetConnName(String val) { this.connName = val; }
	
	@Value("${ibm.mq.channel}")
	private String channel;
	public String GetChannel() { return this.channel; }
	public void SetChannel(String val) { this.channel = val; }
	
	@Value("${ibm.mq.queue}")
	private String destQueue;
	
	private int port;
	public int GetPort() { return this.port; }
	public void SetPort(int val) { this.port = val; }
	
	@Value("${ibm.mq.useCCDT:false}")
	private boolean useCCDT;
	public boolean GetCCDT() { return this.useCCDT; }
	public void SetCCDT(boolean val) { this.useCCDT = val; }
	
	@Value("${ibm.mq.ccdtFile:missing}")
	private String ccdtFile;

	@Value("${ibm.mq.user}")
	private String userId;
	public String GetUserId() { return this.userId; }
	public void SetUserId(String val) { this.userId = val; }
	
	@Value("${ibm.mq.password}")
	private String password;
	public String GetPassword() { return this.password; }
	public void SetPassword(String val) { this.password = val; }
	
	@Value("${ibm.mq.sslCipherSpec}")
	private String cipher;

	//
	@Value("${ibm.mq.useSSL}")
	private boolean useSSL;
		
	@Value("${ibm.mq.security.truststore}")
	private String truststore;
	@Value("${ibm.mq.security.truststore-password}")
	private String truststorepass;
	@Value("${ibm.mq.security.keystore}")
	private String keystore;
	@Value("${ibm.mq.security.keystore-password}")
	private String keystorepass;
	
	/*
	 * if we want the source kafka topic and kafka key on the MQ message
	 * ... use the MQRFH2 header to store the additional meta-data
	 */
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

    @Value("${ibm.mq.mqmd.persistent:1}")
	private int msgPersistent;	
    
    @Value("${ibm.mq.retries.maxAttempts:3}")
	private int maxAttempts;	

	private MQQueueManager queManager;
	public MQQueueManager getQueueManager() {
		return this.queManager;
	}
	
	private MQQueue queue;
	private String dlqName;
	
	public MQConnection() {
	}
	
	/*
	 * Ensure that the MQ message expires is set correctly ...
	 * ... if someone has passed in the messageExpiry value to be less than -1 or equal to zero (0)
	 * ... the set as unlimited (-1)
	 */
	@PostConstruct
	private void validateExpiry() {
	
		if ((this.msgExpiry < MQConstants.MQEI_UNLIMITED) || (this.msgExpiry == MQKafkaConstants.MQMD_EXPIRY_UNLIMITED)) {
			log.warn("Message expiry is invalid, resetting to UNLIMITED ");
			this.msgExpiry = MQConstants.MQEI_UNLIMITED;
		}
		
		if (this.msgExpiry != MQConstants.MQEI_UNLIMITED) {
			if (this.msgExpiry < MQKafkaConstants.MQMD_EXPIRY_LOW) {
				log.warn("Message expiry is in 10ths of seconds ; low message expiry ( < 30 ) : "  + this.msgExpiry);
				
			}
			if ((this.msgExpiry >= MQKafkaConstants.MQMD_EXPIRY_LOW) && (this.msgExpiry < MQKafkaConstants.MQMD_EXPIRY_MEDIUM)) {
				log.warn("Message expiry is in 10ths of seconds ; medium message expiry ( >= 30 < 100 ) : "  + this.msgExpiry);
				
			}
		}
		
	}
	
	/*
	 * Ensure we are processing a REQUEST or DATAGRAM message
	 */
	@PostConstruct
	private void validateReplyToQueueManager() {

		if (this._debug) { log.info("Request message type : " + reqType); } 
		if (!(this.reqType != MQConstants.MQMT_DATAGRAM) 
				|| (!(this.reqType != MQConstants.MQMT_REQUEST))) {
			if (this._debug) {  log.info("Request message type is valid"); }
		
		} else {
			log.error("Request message type mismatch; 1 - REQUEST, 8 - DATAGRAM");
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
	

	/*
	 * Connect to the queue manager ...
	 * ... using either a connection name string (connName)
	 * ... or an MQ CCDT (Client Channel Defintion Table) 
	 */
	@Bean("queuemanager") 
	public MQQueueManager createQueueManagerConnection() throws MQException, MQDataException, Exception {
		
		validateHostAndPort();
		validateUser();
		
		Hashtable<String, Comparable> env = new Hashtable<String, Comparable>();
		/*
		 * if we are not using a CCDT file (Client Channel Definition Table), set the MQ attributes from
		 * ... the config file
		 */
		if (!this.useCCDT) {
			env.put(MQConstants.HOST_NAME_PROPERTY, this.hostName);
			env.put(MQConstants.CHANNEL_PROPERTY, this.channel);
			env.put(MQConstants.PORT_PROPERTY, this.port);
		}		
		env.put(MQConstants.CONNECT_OPTIONS_PROPERTY, MQConstants.MQCNO_RECONNECT);
		
		/*
		 * 
		 * If a username and password is provided, then use it
		 * ... if CHCKCLNT is set to OPTIONAL or RECDADM
		 * ... RECDADM will use the username and password if provided ... if a password is not provided
		 * ...... then the connection is used like OPTIONAL
		 */
		
		if (this.userId != null) {
			env.put(MQConstants.USER_ID_PROPERTY, this.userId);  }
		if (this.password != null) {
			env.put(MQConstants.PASSWORD_PROPERTY, this.password); }
		env.put(MQConstants.TRANSPORT_PROPERTY,MQConstants.TRANSPORT_MQSERIES);

		if (this._debug) {
			log.info("Host 		: " + this.hostName);
			log.info("Channel 	: " + this.channel);
			log.info("Port 		: " + this.port);
			log.info("Queue Man : " + this.queueManager);
			log.info("User 		: " + this.userId);
			log.info("Password  : **********");
			if (this.useSSL) {
				log.info("SSL is enabled ...."); }
		}
		
		/*
		 * if SSL is enabled on the MQ channel, show some details
		 */
		if (this.useSSL) {
			System.setProperty("javax.net.ssl.trustStore", this.truststore);
	        System.setProperty("javax.net.ssl.trustStorePassword", this.truststorepass);
	        System.setProperty("javax.net.ssl.trustStoreType","JKS");
	        System.setProperty("javax.net.ssl.keyStore", this.keystore);
	        System.setProperty("javax.net.ssl.keyStorePassword", this.keystorepass);
	        System.setProperty("javax.net.ssl.keyStoreType","JKS");
	        System.setProperty("com.ibm.mq.cfg.useIBMCipherMappings","false");
	        env.put(MQConstants.SSL_CIPHER_SUITE_PROPERTY, this.cipher); 
		
		} else {
			if (this._debug) {
				log.info("SSL is NOT enabled ...."); }
		}
		
        /*
         * if there are any SSL issues, use -Djavax.net.debug=all on the JVM command
         */
		if (this._debug) {
			log.info("TrustStore       : " + this.truststore);
			log.info("TrustStore Pass  : ********");
			log.info("KeyStore         : " + this.keystore);
			log.info("KeyStore Pass    : ********");
			log.info("Cipher Suite     : " + this.cipher);
		}
		
		/*
		 * Connect to a queue manager, either using the host/port or CCDT file
		 */
		if (!this.useCCDT) {
			log.info("Attempting to connect to queue manager " + this.queueManager);
			this.queManager = new MQQueueManager(this.queueManager, env);
			log.info("Connection to queue manager established ");
			
		} else {
			URL ccdtFileName = new URL("file:///" + this.ccdtFile);
			log.info("Attempting to connect to queue manager " + this.queueManager + " using CCDT file");
			this.queManager = new MQQueueManager(this.queueManager, env, ccdtFileName);
			log.info("Connection to queue manager established ");			
		}

		
		//log.info("Getting DLQ and Opeing queue for reading");

		/*
		 * Get the queue managers dead-letter-queue and open the destination queue for output
		 */
		//this.dlqName = this.queManager.getAttributeString(MQConstants.MQCA_DEAD_LETTER_Q_NAME, 48).trim();
		//this.queue = openQueueForWriting(this.destQueue);
		
		setQMMetrics(MQConstants.MQQMSTA_RUNNING);
		setMessageSuccessMetrics();
		setMessageFailedMetrics();		
		
		return this.queManager;
	}

	
	@Bean("deadletterandopenqueue")
	@DependsOn("queuemanager")
	public MQQueue GetDeadLetterQueueAndOpenQueueForReading() throws MQException {

		log.info("Getting DLQ and Opening queue for reading");

		/*
		 * Get the queue managers dead-letter-queue and open the destination queue for output
		 */
		this.dlqName = this.queManager.getAttributeString(MQConstants.MQCA_DEAD_LETTER_Q_NAME, 48).trim();
		setMessageOnDLQMetrics();
		
		this.queue = openQueueForWriting(this.destQueue);
		return this.queue;
		
	}
	
	
	
	/*
	 * Open the queue for output
	 */
	public MQQueue openQueueForWriting(String qName) throws MQException {
		
		if (this._debug) { log.info("Opening queue " + qName + " for writing"); }
		
		MQQueue outQueue = null;
		int openOptions = MQConstants.MQOO_FAIL_IF_QUIESCING 
					+ MQConstants.MQOO_OUTPUT ;
	
		outQueue = this.queManager.accessQueue(qName, openOptions);
		if (this._debug) { log.info("Queue opened successfully"); }
			
		return outQueue;
		
	}

	/*
	 * Send the message
	 */
	public void sendMessage(ConsumerRecord<?,?> consumerRecord) throws IOException, InterruptedException, MQDataException, MQException {

		MQMessage newmsg = new MQMessage();
		
		/*
		 * If we are wanting to create an MRFH2 header, then do so ...
		 */
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
			}
		    rfh2.write(newmsg);	
		    String message = (String) consumerRecord.value();
		    newmsg.write(message.getBytes());
			newmsg.format 			= MQConstants.MQFMT_RF_HEADER_2;
		    newmsg.messageId 		= MQConstants.MQMI_NONE;
			newmsg.correlationId 	= MQConstants.MQCI_NONE;
			newmsg.messageType      = MQConstants.MQMT_DATAGRAM;
			
		} else {
		    String message = (String) consumerRecord.value();
		    newmsg.write(message.getBytes());
			newmsg.format  			= MQConstants.MQFMT_STRING;
			newmsg.messageId 		= MQConstants.MQMI_NONE;
			newmsg.correlationId 	= MQConstants.MQCI_NONE;
			newmsg.messageType      = MQConstants.MQMT_DATAGRAM;
			
		}
		
		/*
		 * If the message type is a request, set the details on the MQMD header
		 */
		if (this.reqType == MQConstants.MQMT_REQUEST) {
			newmsg.messageType      		= MQConstants.MQMT_REQUEST;
			newmsg.replyToQueueManagerName  = this.replyToQM;
			newmsg.replyToQueueName  		= this.replyToQueue;
			
		}
		newmsg.expiry = this.msgExpiry;
		newmsg.persistence = this.msgPersistent;
		
		/*
		 * Create a PutMessageOptions object and write the message
		 */
		MQPutMessageOptions pmo = new MQPutMessageOptions();	
		pmo.options = MQConstants.MQPMO_NEW_MSG_ID 
				+ MQConstants.MQPMO_FAIL_IF_QUIESCING;
		if (this._debug) {
			log.info("Attempting to write to queue ..."); }
		putMessageToMQ(newmsg, pmo);

		if (this._debug) {
			log.info("Message written to queue successully ..."); }
	}

	/*
	 * 'put' the message to the connected queue manager 
	 * 
	 * If there is an error, try to re-connect to the queue manager and put again ...
	 * ... if we get an error that isn't a CONNECTION_BROKER or QUIESCING, and we are connected to the queue manager
	 * ... write the messages to the DLQ
	 */
	private void putMessageToMQ(MQMessage message, MQPutMessageOptions pmo) throws InterruptedException, MQDataException, IOException {
		
		int attempts = MQKafkaConstants.REPROCESS_MSG_INIT;		
		while (attempts <= this.maxAttempts) {
		
			try {
				this.queue.put(message, pmo);
				setMessageSuccessMetrics();
				break;
 
			} catch (MQException e) {
				if (e.completionCode == MQKafkaConstants.REPROCESS_MSG_INIT && 
						((e.reasonCode == MQConstants.MQRC_CONNECTION_BROKEN)
						|| (e.reasonCode == MQConstants.MQRC_CONNECTION_QUIESCING))) {
					
					setQMMetrics(0);
					
					try {
						attempts++;
						Thread.sleep(5000);
						this.queManager = createQueueManagerConnection();
						this.queue = openQueueForWriting(this.destQueue);
					
					} catch (MQException mqe) {
						log.warn("MQ error exception, trying to put messages : " + mqe.getMessage());
						log.warn("Trying to reconnect : " + mqe.getMessage());
						Thread.sleep(5000);
						
					} catch (Exception e1) {
						log.warn("Error trying to put messages : " + e1.getMessage());
						log.warn("Trying to reconnect : " + e1.getMessage());
						Thread.sleep(5000);

					}
			 	} else {
					log.error("Unhandled MQException : reasonCode " + e.reasonCode );
					log.error("Exception : " + e.getMessage() );
					writeMessageToDLQ(message);
					//setSentToQLQMetrics();
					break;
				}
			}
		}
		if (attempts > this.maxAttempts) {
			log.error("Unable to reconnect to queue manager " );
			System.exit(1);			
		}
		
	}
	
	/*
	 * if we get an error writing to a queue, try writing it to the DLQ
	 * ... ensure we have an unlimited expiry and persistent message
	 */
	private void writeMessageToDLQ(MQMessage message) throws MQDataException, IOException {

		MQPutMessageOptions pmo = new MQPutMessageOptions();	
		pmo.options = MQConstants.MQPMO_NEW_MSG_ID + MQConstants.MQPMO_FAIL_IF_QUIESCING;
		message.expiry = MQKafkaConstants.UNLIMITED_EXPIRY;
		message.persistence = this.msgPersistent;
		
		MQQueue dlqQueue = null;
		try {
			dlqQueue = openQueueForWriting(this.dlqName);	
			dlqQueue.put(message,pmo);
			log.warn("Message written to DLQ");
			setMessageOnDLQMetrics();
			
		} catch (MQException e) {
			log.error("Error writting to DLQ " + this.dlqName);
			log.error("Reason : " + e.reasonCode + " Description : " + e.getMessage());			
						
		} catch (Exception e) {
			log.error("Error writting to DLQ " + this.dlqName);
			log.error("Description : " + e.getMessage());

		} finally {
			try {
				if (dlqQueue != null) {
					dlqQueue.close();
				}	
				
			} catch (MQException e) {
				log.warn("Error closing DLQ " + this.dlqName);
			}
		}
		
	}
	
	/*
	 * Extract connection server and port
	 */
	public void validateHostAndPort() {
		validateHostAndPort(this.useCCDT, this.connName);
	}
	
	public void validateHostAndPort(boolean useCCDT, String conn) {
		
		/*
		 * ALL parameter are passed in the application.yaml file ...
		 *    These values can be overrrided using an application-???.yaml file per environment
		 *    ... or passed in on the command line
		 */
		if (useCCDT && (!conn.equals(""))) {
			log.error("The use of MQ CCDT filename and connName are mutually exclusive");
			System.exit(MQKafkaConstants.EXIT);
		}
		if (useCCDT) {
			return;
		}

		// Split the host and port number from the connName ... host(port)
		if (!conn.equals("")) {
			Pattern pattern = Pattern.compile("^([^()]*)\\(([^()]*)\\)(.*)$");
			Matcher matcher = pattern.matcher(conn);	
			if (matcher.matches()) {
				this.hostName = matcher.group(1).trim();
				this.port = Integer.parseInt(matcher.group(2).trim());
			} else {
				log.error("While attempting to connect to a queue manager, the connName is invalid ");
				System.exit(MQKafkaConstants.EXIT);				
			}
		} else {
			log.error("While attempting to connect to a queue manager, the connName is missing ");
			System.exit(MQKafkaConstants.EXIT);
			
		}
	}

	/*
	 * Check the user, if its passed in 
	 */
	private void validateUser() {

		// if no use, for get it ...
		if (this.userId == null) {
			return;
		}
		
		if (!this.userId.equals("")) {
			if ((this.userId.equals("mqm") || (this.userId.equals("MQM")))) {
				log.error("The MQ channel USERID must not be running as 'mqm' ");
				System.exit(MQKafkaConstants.EXIT);
			}
		} else {
			this.userId = null;
			this.password = null;
		}
		
		
	}

	/*
	 * Messages on DLQ
	 */
	private void setMessageOnDLQMetrics() {
		
		AtomicInteger err = errors.get("MessagesOnDLQ");
		if (err == null) {    			
			errors.put("MessagesOnDLQ"
					,Metrics.gauge(new StringBuilder()
					.append("mq:")
					.append("MessagesOnDLQ").toString(), 
					Tags.of("name", this.queueManager, "DLQ",this.dlqName)
					, new AtomicInteger(0)));
			
		} else {
			err.incrementAndGet();
		}
	}

	/*
	 * Messages failed
	 */
	private void setMessageFailedMetrics() {
		
		AtomicInteger err = errors.get("MessagesFailed");
		if (err == null) {    			
			errors.put("MessagesFailed"
					,Metrics.gauge(new StringBuilder()
					.append("mq:")
					.append("MessagesFailed").toString(), 
					Tags.of("name", this.queueManager, "queue",this.destQueue)
					, new AtomicInteger(0)));
			
		} else {
			err.incrementAndGet();
		}
	}

	/*
	 * Messages successfully commited to the queue manager
	 */
	private void setMessageSuccessMetrics() {
		
		AtomicInteger err = errors.get("MessagesCommitted");
		if (err == null) {    			
			errors.put("MessagesCommitted"
					,Metrics.gauge(new StringBuilder()
					.append("mq:")
					.append("MessagesCommitted").toString(), 
					Tags.of("name", this.queueManager, "queue",this.destQueue)
					, new AtomicInteger(0)));
			
		} else {
			err.incrementAndGet();
		}
	}

	/*
	 * Are we connected to a queue manager ?
	 */
	private void setQMMetrics(int val) {
		
		AtomicInteger err = errors.get("ConnectedToQueueManager");
		if (err == null) {    			
			errors.put("ConnectedToQueueManager"
					,Metrics.gauge(new StringBuilder()
					.append("mq:")
					.append("ConnectedToQueueManager").toString(), 
					Tags.of("name", this.queueManager)
					, new AtomicInteger(val)));
			
		} else {
			err.set(val);
		}
	}
	
	/*
	 * Commit or rollback
	 * *** Not currently usew ***
	 */
	public void commit() throws MQException {
		this.queManager.commit();
	}

	public void rollBack() throws MQException {
		this.queManager.backout();
	}
	
	/*
	 * Clean up ... disconnect from the queue manager
	 * if we are closing
	 */
    @PreDestroy
    public void closeQMConnection() {
    	
    	try {
	    	if (this.queManager != null) {	
	    		if (this._debug) {
	    			log.info("Closing queue manager connection");
	    		}
	    		this.queManager.close();
	    	}
	    	
    	} catch (Exception e) {
    		// do nothing
    	}
    }

}
