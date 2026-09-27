package com.example.kafka.duplicate;

/**
 * Simple POJO representing an order event flowing through Kafka.
 *
 * Jackson requires a no-arg constructor plus getters/setters for automatic
 * JSON serialization/deserialization via ObjectMapper.
 */
public class OrderEvent {

    // Unique per individual send() call — used by the consumer to detect
    // when the exact same event object has been delivered more than once.
    private String eventId;

    // Logical business identifier — multiple events can share the same orderId
    // (e.g. order created, order paid, order shipped are all the same order).
    // In IDEMPOTENT_MODE the producer uses orderId as the Kafka message key so
    // that all events for one order always land on the same partition.
    private String orderId;

    private double amount;    // Order amount in dollars
    private long   timestamp; // Epoch millis when the event was created on the producer side

    // Jackson no-arg constructor
    public OrderEvent() {}

    public OrderEvent(String eventId, String orderId, double amount, long timestamp) {
        this.eventId   = eventId;
        this.orderId   = orderId;
        this.amount    = amount;
        this.timestamp = timestamp;
    }

    public String getEventId()   { return eventId; }
    public String getOrderId()   { return orderId; }
    public double getAmount()    { return amount; }
    public long   getTimestamp() { return timestamp; }

    public void setEventId(String eventId)   { this.eventId   = eventId; }
    public void setOrderId(String orderId)   { this.orderId   = orderId; }
    public void setAmount(double amount)     { this.amount    = amount; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "OrderEvent{eventId='" + eventId + "', orderId='" + orderId +
               "', amount=" + amount + ", timestamp=" + timestamp + '}';
    }
}
