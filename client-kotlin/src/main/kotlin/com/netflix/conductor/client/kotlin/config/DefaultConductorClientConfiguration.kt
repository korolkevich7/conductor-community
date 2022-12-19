package com.netflix.conductor.client.kotlin.config

/**
 * A default implementation of [ConductorClientConfiguration] where external payload storage
 * is disabled.
 */
class DefaultConductorClientConfiguration : ConductorClientConfiguration {

    override fun getWorkflowInputPayloadThresholdKB(): Int {
        return 5120
    }

    override fun getWorkflowInputMaxPayloadThresholdKB(): Int {
        return 10240
    }

    override fun getTaskOutputPayloadThresholdKB(): Int {
        return 3072
    }

    override fun getTaskOutputMaxPayloadThresholdKB(): Int {
        return 10240
    }

    override fun isExternalPayloadStorageEnabled(): Boolean {
        return false
    }
}
