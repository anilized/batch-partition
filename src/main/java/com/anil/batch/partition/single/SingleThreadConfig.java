package com.anil.batch.partition.single;

import com.anil.batch.partition.model.TicketEntity;
import com.anil.batch.partition.repository.TicketRepository;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Map;


public class SingleThreadConfig {
    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private TicketRepository ticketRepository;

    @Bean
    public Job updateTicketStatusJob() {
        return new JobBuilder("updateTicketStatusJob", jobRepository)
                .start(updateTicketStatusStep())
                .listener(jobExecutionListener())
                .build();
    }

    @Bean
    public Step updateTicketStatusStep() {
        return new StepBuilder("updateTicketStatusStep", jobRepository)
                .<TicketEntity, TicketEntity>chunk(1000, transactionManager)
                .reader(ticketReader())
                .processor(ticketProcessor())
                .writer(ticketWriter())
                .listener(stepExecutionListener())
                .build();
    }

    @Bean
    @StepScope
    public ItemReader<TicketEntity> ticketReader() {
        RepositoryItemReader<TicketEntity> reader = new RepositoryItemReader<>();
        reader.setRepository(ticketRepository);
        reader.setMethodName("findAll");
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
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                endTime = System.currentTimeMillis();
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
