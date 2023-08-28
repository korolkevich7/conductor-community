package com.netflix.conductor.client.kotlin.automator

import kotlinx.coroutines.sync.Semaphore
import org.slf4j.LoggerFactory

/**
 * A class wrapping a semaphore which holds the number of permits available for polling and
 * executing tasks.
 */
class CoroutinePollingSemaphore(numSlots: Int) {
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
        val available = semaphore.availablePermits
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
        private val LOGGER = LoggerFactory.getLogger(CoroutinePollingSemaphore::class.java)
    }
}

fun Semaphore.release(numSlots: Int) {
    repeat(numSlots) { this.release() }
}

fun Semaphore.tryAcquire(numSlots: Int): Boolean {
    if (this.availablePermits < numSlots) return false
    var acquireCount = 0
    repeat(numSlots) {
        if (this.tryAcquire()) acquireCount++ else {
            release(acquireCount)
            return false
        }
    }
    return true
}