package com.nitro.clients.kafka;

import kafka.utils.Time;

/**
 * A Time instance that our embedded Kafka instance uses.
 */
class SystemTime implements Time {

    public long milliseconds() {
        return System.currentTimeMillis();
    }

    public long nanoseconds() {
        return System.nanoTime();
    }

    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // Ignore
        }
    }
}