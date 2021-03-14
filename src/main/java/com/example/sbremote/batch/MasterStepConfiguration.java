package com.example.sbremote.batch;

import com.example.sbremote.batch.partitioner.RemotePartitioner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.MessageChannelPartitionHandler;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.PollableChannel;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableBatchProcessing
@EnableBatchIntegration
@Slf4j
public class MasterStepConfiguration {

	@Autowired
	private RemotePartitioningManagerStepBuilderFactory remoteStepBuilderFactory;

	@Autowired
	@Qualifier("remoteWorkerStep")
	private Step remoteWorkerStep;

	@Value("${batch.remote.partition: #{2}}")
	private int partition;

	@Bean
	public ThreadPoolTaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(20);
		executor.setQueueCapacity(20);
		executor.setMaxPoolSize(20);
		executor.setThreadNamePrefix("sb-master-");
		executor.initialize();

		return executor;
	}

	@Bean
	public PartitionHandler partitionHandler() {
		MessageChannelPartitionHandler partitionHandler = new MessageChannelPartitionHandler();
		partitionHandler.setStepName("remoteWorkerStep");
		partitionHandler.setGridSize(partition);
		partitionHandler.setReplyChannel(replies());
		MessagingTemplate template = new MessagingTemplate();
		template.setDefaultChannel(requests());
		template.setReceiveTimeout(100000);
		partitionHandler.setMessagingOperations(template);
		return partitionHandler;
	}

	@Bean
	@Qualifier("masterStep")
	public Step masterStep() throws Exception {
		return remoteStepBuilderFactory.get("masterStep")
				.partitioner("remoteWorkerStep", new RemotePartitioner())
				.gridSize(partition)
				.outputChannel(requests())
//				.partitionHandler(partitionHandler())
				.build();
	}

	/*
	 * Configure outbound flow (requests going to workers)
	 */
	@Bean
	public DirectChannel requests() {
		return new DirectChannel();
	}

	@Bean
	public IntegrationFlow outboundFlow(KafkaProperties kafkaProperties, KafkaTemplate kafkaTemplate) {
		final KafkaProducerMessageHandler kafkaMessageHandler = new KafkaProducerMessageHandler(kafkaTemplate);
		kafkaMessageHandler.setTopicExpression(new LiteralExpression("migration-topics"));
		return IntegrationFlows
				.from(requests())
//				.handle(Kafka.outboundAdapter(connectionFactory).destination("requests"))
//				.handle(Kafka.outboundChannelAdapter(kafkaTemplate))
				.handle(kafkaMessageHandler)
				.get();
	}

	@Bean
	public PollableChannel replies() {
		return new QueueChannel();
	}
}
