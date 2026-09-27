package com.example.kafka.duplicate;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Thin Jackson wrapper.  ObjectMapper is thread-safe after construction, so
 * a single shared instance is fine for the entire JVM lifetime.
 */
public class JsonUtil {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonUtil() {}

    public static String toJson(OrderEvent event) {
        try {
            return MAPPER.writeValueAsString(event);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize OrderEvent to JSON", e);
        }
    }

    public static OrderEvent fromJson(String json) {
        try {
            return MAPPER.readValue(json, OrderEvent.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize JSON to OrderEvent: " + json, e);
        }
    }
}
