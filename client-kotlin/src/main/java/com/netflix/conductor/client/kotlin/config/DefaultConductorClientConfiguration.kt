package com.netflix.conductor.client.kotlin.config

/**
 * A default implementation of [ConductorClientConfiguration] where external payload storage
 * is disabled.
 */
object DefaultConductorClientConfiguration : ConductorClientConfiguration {

    override fun getWorkflowInputPayloadThresholdKB(): Int = 5120

    override fun getWorkflowInputMaxPayloadThresholdKB(): Int = 10240

    override fun getTaskOutputPayloadThresholdKB(): Int = 3072

    override fun getTaskOutputMaxPayloadThresholdKB(): Int = 10240

    override fun isExternalPayloadStorageEnabled(): Boolean = false
}
