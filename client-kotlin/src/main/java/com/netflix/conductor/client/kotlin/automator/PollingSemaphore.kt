package com.netflix.conductor.client.kotlin.automator

import org.slf4j.LoggerFactory
import java.util.concurrent.Semaphore

/**
 * A class wrapping a semaphore which holds the number of permits available for polling and
 * executing tasks.
 */
class PollingSemaphore(numSlots: Int) {
    private val semaphore: Semaphore

    init {
        LOGGER.debug("Polling semaphore initialized with {} permits", numSlots)
        semaphore = Semaphore(numSlots)
    }

    /** Signals that processing is complete and the specified number of permits can be released.  */
    fun complete(numSlots: Int) {
        LOGGER.debug("Completed execution; releasing permit")
        semaphore.release(numSlots)
    }

    /**
     * Gets the number of threads available for processing.
     *
     * @return number of available permits
     */
    fun availableSlots(): Int {
        val available = semaphore.availablePermits()
        LOGGER.debug("Number of available permits: {}", available)
        return available
    }

    /**
     * Signals if processing is allowed based on whether specified number of permits can be
     * acquired.
     *
     * @param numSlots the number of permits to acquire
     * @return `true` - if permit is acquired `false` - if permit could not be acquired
     */
    fun acquireSlots(numSlots: Int): Boolean {
        val acquired = semaphore.tryAcquire(numSlots)
        LOGGER.debug("Trying to acquire {} permit: {}", numSlots, acquired)
        return acquired
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(PollingSemaphore::class.java)
    }
}
