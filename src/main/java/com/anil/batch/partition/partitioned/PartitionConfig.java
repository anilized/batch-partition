package com.anil.batch.partition.partitioned;

import com.anil.batch.partition.model.TicketEntity;
import com.anil.batch.partition.repository.TicketRepository;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.*;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableBatchProcessing
public class PartitionConfig {

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private TicketRepository ticketRepository;

    @Bean
    public Job updateTicketStatusJob() {
        return new JobBuilder("updateTicketStatusJob", jobRepository)
                .start(fetchTicketCountStep())
                .next(partitionStep())
                .listener(jobExecutionListener())
                .build();
    }

    @Bean
    public Step fetchTicketCountStep() {
        return new StepBuilder("fetchTicketCountStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    long count = ticketRepository.count();
                    chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("ticketCount", count);
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Step partitionStep() {
        return new StepBuilder("partitionStep", jobRepository)
                .partitioner(workerStep().getName(), partitioner())
                .step(workerStep())
                .gridSize(Runtime.getRuntime().availableProcessors())
                .taskExecutor(new SimpleAsyncTaskExecutor())
                .build();
    }

    @Bean
    public Partitioner partitioner() {
        return gridSize -> {
            long totalCount = ticketRepository.count();
            long range = totalCount / Runtime.getRuntime().availableProcessors(); //6250
            long fromId = 1;
            long toId = range;
            Map<String, ExecutionContext> partitions = new HashMap<>();
            for (int i = 1; i <= gridSize; i++) {
                System.out.println("\nStarting: Thread " + i);
                System.out.println("fromId " + fromId);
                System.out.println("toId " + toId);
                ExecutionContext context = new ExecutionContext();
                context.putLong("fromID", fromId);
                context.putLong("toId", toId);

                context.putString("name", "Thread" + i);

                partitions.put("partition" + i, context);

                fromId = toId + 1;
                toId += range;
            }
            return partitions;
        };
    }

    @Bean
    public Step workerStep() {
        return new StepBuilder("workerStep", jobRepository)
                .<TicketEntity, TicketEntity>chunk(1000, transactionManager)
                .reader(ticketReader(null, null)) // Placeholder values
                .processor(ticketProcessor())
                .writer(ticketWriter())
                .listener(stepExecutionListener())
                .build();
    }

    @Bean
    @StepScope
    public ItemReader<TicketEntity> ticketReader(@Value("#{stepExecutionContext[fromID]}") Long fromId, @Value("#{stepExecutionContext[toId]}") Long toId) {
        RepositoryItemReader<TicketEntity> reader = new RepositoryItemReader<>();
        reader.setRepository(ticketRepository);
        reader.setMethodName("findBetweenId");
        reader.setArguments(List.of(fromId, toId));
        reader.setPageSize(1000);
        reader.setSort(Map.of("id", Sort.Direction.ASC));
        return reader;
    }

    @Bean
    public ItemProcessor<TicketEntity, TicketEntity> ticketProcessor() {
        return ticket -> {
            ticket.setState("UPDATED");
            return ticket;
        };
    }

    @Bean
    public ItemWriter<TicketEntity> ticketWriter() {
        return tickets -> {
            for (TicketEntity ticket : tickets) {
                ticketRepository.updateStatusById(ticket.getId(), ticket.getState());
            }
        };
    }

    @Bean
    public JobExecutionListener jobExecutionListener() {
        return new JobExecutionListener() {
            private long startTime;
            private long endTime;

            @Override
            public void beforeJob(JobExecution jobExecution) {
                startTime = System.currentTimeMillis();
                System.out.println(" === JOB START TIME ===" + startTime);
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                endTime = System.currentTimeMillis();
                System.out.println(" === JOB END TIME ===" + endTime);
                System.out.println("Total time taken for job: " + (endTime - startTime) + "ms");
            }
        };
    }

    @Bean
    public StepExecutionListener stepExecutionListener() {
        return new StepExecutionListener() {
            private long startTime;
            private long endTime;

            @Override
            public void beforeStep(StepExecution stepExecution) {
                startTime = System.currentTimeMillis();
            }

            @Override
            public ExitStatus afterStep(StepExecution stepExecution) {
                endTime = System.currentTimeMillis();
                System.out.println("Total time taken for step " + stepExecution.getStepName() + ": " + (endTime - startTime) + "ms");
                return ExitStatus.COMPLETED;
            }
        };
    }
}
