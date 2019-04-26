package com.humio.test.randi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.humio.test.randi.model.HecEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.*;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@SpringBootApplication
@EnableBatchProcessing
@Slf4j
public class RandiApplication {
    @Value("${backfill.days}")
    private int backfillDays;
    @Value("${backfill.daily-amount}")
    private int backfillAmount;

    @Value("${humio-url}")
    private String humioBaseUrl;

    @Value("${ingest-token}")
    private String ingestToken;

    public static ZonedDateTime END_TIME = ZonedDateTime.now(ZoneId.of("UTC"));

    @Bean
    public ItemReader<Long> generateEventsReader(Supplier<Long> averageEventSize) {
        long firstEventTime = END_TIME.minusDays(backfillDays).toInstant().toEpochMilli();
        long totalBytes = backfillDays * backfillAmount * 1_000_000_000L;
        long avgMessageLength = averageEventSize.get();

        long totalMessages = totalBytes / avgMessageLength;
        log.info("Will attempt to send {} bytes in {} events of {} bytes", totalBytes, totalMessages, avgMessageLength);

        return new AbstractItemCountingItemStreamItemReader<>() {
            @Override
            protected Long doRead() throws Exception {
                final int count = getCurrentItemCount();
                if (count > totalMessages) {
                    log.info("Total messages exceeded");
                    return null;
                }
                return firstEventTime + ((Duration.ofDays(backfillDays).toMillis() / totalMessages) * count);
            }

            @Override
            protected void doOpen() throws Exception {
                setName("EventsGenerator");
            }

            @Override
            protected void doClose() throws Exception {

            }
        };
    }

    @Bean
    public HttpClient humioHttpClient() {
        return HttpClient.newBuilder()
                .build();
    }


    @Bean
    public ItemWriter<HecEvent> humioWriter(HttpClient humioHttpClient) {
        final ObjectMapper objectMapper = new ObjectMapper();
        return items -> {
            final byte[] payload = items.stream()
                    .map(event -> {
                        try {
                            return objectMapper.writeValueAsString(event);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException("Failed to marshall event: " + event, e);
                        }
                    })
                    .collect(Collectors.joining(System.lineSeparator())).getBytes();
            final HttpRequest request = HttpRequest.newBuilder(new URI(humioBaseUrl + "/api/v1/ingest/hec"))
                    .header("Content-Type", "text/plain; charset=utf-8")
                    .header("Authorization", "Bearer " + ingestToken)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                    .build();
            log.info("Shipping {} events, {} bytes", items.size(), payload.length);
            final HttpResponse<String> response = humioHttpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new RuntimeException("Failed to ingest: " + response.body());
            }
        };
    }

    @Bean
    public Step fillEvents(StepBuilderFactory stepBuilderFactory, ItemReader<Long> generateEventsReader, ItemProcessor<Long, HecEvent> logItemProcessor, ItemWriter<HecEvent> humioWriter) {
        return stepBuilderFactory.get("FillEvents")
                .<Long, HecEvent>chunk(10000)
                .reader(generateEventsReader)
                .processor(logItemProcessor)
                .writer(humioWriter)
                .build();
    }

    @Bean
    public Job backfillJob(JobBuilderFactory jobBuilderFactory, Step generateEvents) {
        return jobBuilderFactory.get("BackfillJob")
                .start(generateEvents)
                .build();
    }

    public static void main(String[] args) {
        SpringApplication.run(RandiApplication.class, args);
    }

}
