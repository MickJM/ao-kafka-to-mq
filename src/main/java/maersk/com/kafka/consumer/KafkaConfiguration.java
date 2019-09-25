package maersk.com.kafka.consumer;

/*
 * Kafka Configuration
 * 
 *  Copyright Maersk 2019
 */

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class KafkaConfiguration {

	private Logger log = Logger.getLogger(this.getClass());

    @Value("${kafka.debug:false}")
    private boolean _debug;
    
    //consumer property
    @Value("${kafka.src.bootstrap.servers}")
    private String srcBootstrapServers;
    @Value("${kafka.src.username}")
    private String srcUsername;
    @Value("${kafka.src.password}")
    private String srcPassword;
    @Value("${kafka.src.login.module:org.apache.kafka.common.security.plain.PlainLoginModule}")
    private String srcLoginModule;
    @Value("${kafka.src.sasl.mechanism:PLAIN}")
    private String srcSaslMechanism;
    @Value("${kafka.src.truststore.location:}")
    private String srcTruststoreLocation;
    @Value("${kafka.src.truststore.password:}")
    private String srcTruststorePassword;
    @Value("${kafka.src.consumer.group:kafka-to-mq}")
    private String srcConsumerGroup;
    @Value("${kafka.src.offset.auto.reset:earliest}")
    private String srcOffsetAutoReset;
    @Value("${kafka.src.max.poll.records:100}")
    private String srcMaxPollRecords;
    @Value("${kafka.src.topic}")
    private String sourceTopic;
    @Value("${kafka.src.sasl.protocol:SASL_SSL}")
    private String srcSecurityProtocol;
    @Value("${kafka.src.concurrency:1}")
    private int srcConcurrency;
    @Value("${kafka.src.retry.max.attempts:3}")
    private int maxRetryAttempts;
    @Value("${kafka.src.retry.initial.interval-secs:1}")
    private int retryInitialIntervalSeconds;
    @Value("${kafka.src.consumer.retry.max.interval.secs:10}")
    private int retryMaxIntervalSeconds;

	@Value("${application.name:kafka-consumer}")
	private String clientId;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {

		if (this._debug) { 
			log.info("eyc-catcher #################"); 
			log.info("ConsumerFactory being created"); 
		}

        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.srcBootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, this.srcConsumerGroup);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.srcOffsetAutoReset);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, this.srcMaxPollRecords);
    
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, this.clientId);
        try {
			properties.put(ConsumerConfig.CLIENT_ID_CONFIG, InetAddress.getLocalHost().getHostName());

        } catch (UnknownHostException e) {
			// do nothing ....
		}
        
		if (this._debug) { log.info("ConsumerFactory: setting SASL"); }
        addSaslProperties(properties, srcSaslMechanism, srcSecurityProtocol, srcLoginModule, srcUsername, srcPassword);

		if (this._debug) { log.info("ConsumerFactory: setting truststore"); }    		
        addTruststoreProperties(properties, this.srcTruststoreLocation, this.srcTruststorePassword);

        return new DefaultKafkaConsumerFactory<>(properties);
    }

	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> 
							kafkaListenerContainerFactory(ConsumerFactory<String, String> consumerFactory) {

		if (this._debug) { log.info("KafkaListenerContainerFactory: Start"); }

		if (this._debug) { log.info("KafkaListenerContainerFactory: srcConcurrency: " + this.srcConcurrency); }
		
		ConcurrentKafkaListenerContainerFactory<String, String> factory 
        			= new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);        
        factory.setConcurrency(this.srcConcurrency);
        factory.getContainerProperties().setPollTimeout(3000);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        factory.setRetryTemplate(retryTemplate());
        factory.setStatefulRetry(true);
        
		if (this._debug) { 
			log.info("KafkaListenerContainerFactory: setPollTimeOut : 3000"); 
			log.info("KafkaListenerContainerFactory: return");
		}        
        return factory;
    }
    
	/*
	 * Create a RetryTemplate
	 */
    private RetryTemplate retryTemplate() {
        RetryTemplate template = new RetryTemplate();
        template.setRetryPolicy(retryPolicy());
        template.setBackOffPolicy(backOffPolicy());
        return template;
    
    }

    /*
     * Create a Retry policy
     */
    private RetryPolicy retryPolicy() {
        SimpleRetryPolicy policy = new SimpleRetryPolicy();
        policy.setMaxAttempts(maxRetryAttempts);
        return policy;
    
    }

    /*
     * Create a BackOff policy object
     */
    private BackOffPolicy backOffPolicy() {
        ExponentialBackOffPolicy policy = new ExponentialBackOffPolicy();
        policy.setInitialInterval(retryInitialIntervalSeconds*1000);
        policy.setMaxInterval(retryMaxIntervalSeconds*1000);
        return policy;
    
    }
    
    /*
     * Add SASL properties
     */
    private void addSaslProperties(Map<String, Object> properties, String saslMechanism, String securityProtocol, String loginModule, String username, String password) {

		if (this._debug) { log.info("addSaslProperties: started"); }    		

    	if (!StringUtils.isEmpty(username)) {
            properties.put("security.protocol", securityProtocol);
            properties.put("sasl.mechanism", saslMechanism);
            String saslJaaSConfig = String.format("%s required username=\"%s\" password=\"%s\" ;", loginModule, username, password);
            properties.put("sasl.jaas.config", saslJaaSConfig);
            
    		if (this._debug) { 
    			log.info("addSaslProperties: security set"); 
    			log.info("saslJaasConfig : " + saslJaaSConfig );
    		}    		
        }
		if (this._debug) { log.info("addSaslProperties: exit"); }    		

    }

    /*
     * Add the truststore
     */
    private void addTruststoreProperties(Map<String, Object> properties, String location, String password) {
		
    	if (this._debug) { log.info("addTruststoreProperties: start"); }    		
    	if (!StringUtils.isEmpty(location)) {
        	if (this._debug) { log.info("addTruststoreProperties: truststore set"); }    		
            properties.put("ssl.truststore.location", location);
            properties.put("ssl.truststore.password", password);
        }
    	if (this._debug) { log.info("addTruststoreProperties: exit"); }    		
    	
    }
    
}
