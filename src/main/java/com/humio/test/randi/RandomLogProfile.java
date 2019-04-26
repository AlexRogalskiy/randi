package com.humio.test.randi;

import com.humio.test.randi.model.HecEvent;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Configuration
@Profile("random")
public class RandomLogProfile {
    @Value("${backfill.random.event-length}")
    public int eventLength;
    public static int MESSAGE_COUNT = 50;

    @Bean
    public Supplier<Long> averageEventSize() {
        return () -> (long) eventLength;
    }

    @Bean
    public Supplier<String> randommessage() {
        final List<String> randoms = IntStream.range(0, MESSAGE_COUNT)
                .mapToObj(operand -> RandomStringUtils.randomAlphanumeric(eventLength))
                .collect(Collectors.toList());
        return () -> randoms.get(RandomUtils.nextInt(0, MESSAGE_COUNT));
    }

    @Bean
    public ItemProcessor<Long, HecEvent> logItemProcessor() {
        return item -> HecEvent.builder()
                .time(item/1000.0)
                .event(randommessage().get())
                .build();
    }

}
