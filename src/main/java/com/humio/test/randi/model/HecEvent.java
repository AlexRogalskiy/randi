package com.humio.test.randi.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class HecEvent {
    private final double time;
    private final String event;
}
