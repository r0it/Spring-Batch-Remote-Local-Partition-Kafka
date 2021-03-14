package com.example.sbremote.batch;

import com.example.sbremote.batch.partitioner.LocalPartitioner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.H2PagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@EnableBatchProcessing
@EnableBatchIntegration
public class WorkerStepConfiguation {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    public JobExplorer jobExplorer;

    @Autowired
    DataSource dataSource;

    @Autowired
    ApplicationContext applicationContext;

    @Autowired
    private ItemReader<Customer> crItemReader;

    @Autowired
    private ItemWriter<Customer> crItemWriter;

    @Autowired
    private LocalPartitioner localPartitioner;

    @Autowired
    private RemotePartitioningWorkerStepBuilderFactory remotePartitioningWorkerStepBuilderFactory;

    @Bean("crItemReader")
    @StepScope
    public ItemReader<Customer> reader(@Value("#{stepExecutionContext['locStartIdx']}") Long startIdx,
                                       @Value("#{stepExecutionContext['locEndIdx']}") Long endIdx){
        log.info("Local Reader- locStartIdx: {}, locEndIdx: {}", startIdx, endIdx);
        Map<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("id", Order.ASCENDING);
        H2PagingQueryProvider queryProvider = new H2PagingQueryProvider();
        queryProvider.setSelectClause("SELECT id, name");
        queryProvider.setFromClause("FROM customers");
        queryProvider.setWhereClause("id >= " + startIdx + " and id <= " + endIdx);
        queryProvider.setSortKeys(sortKeys);
        return new JdbcPagingItemReaderBuilder<Customer>()
                .name("pagingItemReader")
                .dataSource(dataSource)
                .pageSize(10)
                .queryProvider(queryProvider)
                .rowMapper(new BeanPropertyRowMapper<>(Customer.class))
                .build();
    }

    @Bean("crItemWriter")
    @StepScope
    public JdbcBatchItemWriter<Customer> itemWriter() {
        return new JdbcBatchItemWriterBuilder<Customer>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Customer>())
                .sql("UPDATE customers set name=:name where id=:id")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public ThreadPoolTaskExecutor workerTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(20);
        executor.setQueueCapacity(20);
        executor.setMaxPoolSize(20);
        executor.setThreadNamePrefix("sb-worker-");
        executor.initialize();

        return executor;
    }

    /*
     * Configure inbound flow (requests coming from the master)
     */
    @Bean
    public DirectChannel workerRequests() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow inboundFlow(KafkaProperties kafkaProperties) throws Exception {
        return IntegrationFlows
                .from(Kafka.messageDrivenChannelAdapter(new DefaultKafkaConsumerFactory(kafkaProperties.buildConsumerProperties()) , "migration-topics"))
//                .from(kafkaMessageDrivenChannelAdapter(kafkaListenerContainer))
                .channel(workerRequests())
                .get();
    }

//    public KafkaMessageDrivenChannelAdapter kafkaMessageDrivenChannelAdapter(ConcurrentMessageListenerContainer kafkaListenerContainer) throws Exception {
//        KafkaMessageDrivenChannelAdapter<String, StepExecution> kafkaMessageDrivenChannelAdapter =
//                new KafkaMessageDrivenChannelAdapter<String, StepExecution>(kafkaListenerContainer);
//        kafkaMessageDrivenChannelAdapter.setOutputChannel(workerRequests());
//        return kafkaMessageDrivenChannelAdapter;
//    }
////
//    @Bean
//    public ConcurrentMessageListenerContainer kafkaListenerContainer(KafkaProperties kafkaProperties) throws Exception {
//        ContainerProperties containerProps = new ContainerProperties("migration-topics");
//        ConcurrentMessageListenerContainer<String, StepExecution> container = new ConcurrentMessageListenerContainer<>(
//                new DefaultKafkaConsumerFactory<String, StepExecution>(kafkaProperties.buildConsumerProperties()), containerProps);
//        container.setConcurrency(1);
//        return (ConcurrentMessageListenerContainer<String, StepExecution>) container;
//    }

    @Bean
    public Step remoteWorkerStep() throws Exception {
        return remotePartitioningWorkerStepBuilderFactory.get("remoteWorkerStep"
        )
                .inputChannel(workerRequests())
                .allowStartIfComplete(true)
                .listener(localPartitioner)
                .partitioner("localWorkerStep", localPartitioner)
                .step(localWorkerStep())
                .gridSize(4)
                .taskExecutor(workerTaskExecutor())
                .build();
    }

    @Bean
    public Step localWorkerStep() {
        return stepBuilderFactory.get("localWorkerStep"
                +System.currentTimeMillis() //temporary fix for JobExecutionException: Cannot restart step from STARTED status
        )
                .allowStartIfComplete(true)
                .<Customer, Customer>chunk(10)
                .reader(crItemReader)
                .processor(new ItemProcessor<Customer, Customer>() {
                    @Override
                    public Customer process(Customer customer) throws Exception {
                        log.info("id: {}", customer.getId());
                        return customer;
                    }
                })
                .writer(crItemWriter)
                .build();
    }
}
