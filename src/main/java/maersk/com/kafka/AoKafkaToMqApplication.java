package maersk.com.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class AoKafkaToMqApplication {

	private static ConfigurableApplicationContext ctx;
	
	
	public static void main(String[] args) {

		SpringApplication.run(AoKafkaToMqApplication.class, args);
	}

	
}
