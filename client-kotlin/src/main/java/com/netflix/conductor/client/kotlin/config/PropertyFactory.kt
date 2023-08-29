/*
 * Copyright 2020 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.client.kotlin.config

import com.netflix.config.DynamicProperty
import java.util.concurrent.ConcurrentHashMap

/** Used to configure the Conductor workers using properties.  */
class PropertyFactory private constructor(prefix: String, propName: String, workerName: String) {
    private val global: DynamicProperty
    private val local: DynamicProperty

    init {
        global = DynamicProperty.getInstance("$prefix.$propName")
        local = DynamicProperty.getInstance("$prefix.$workerName.$propName")
    }

    /**
     * @param defaultValue Default Value
     * @return Returns the value as integer. If not value is set (either global or worker specific),
     * then returns the default value.
     */
    fun getInteger(): Int? =
        local.integer ?: global.integer

    /**
     * @param defaultValue Default Value
     * @return Returns the value as String. If not value is set (either global or worker specific),
     * then returns the default value.
     */
    fun getString(): String? =
        local.string ?: global.string

    /**
     * @param defaultValue Default Value
     * @return Returns the value as Boolean. If not value is set (either global or worker specific),
     * then returns the default value.
     */
    fun getBoolean(): Boolean? =
        local.boolean ?: global.boolean

    companion object {
        private const val PROPERTY_PREFIX = "conductor.worker"
        private val PROPERTY_FACTORY_MAP = ConcurrentHashMap<String, PropertyFactory>()
        @JvmStatic
        fun getInteger(workerName: String, property: String, defaultValue: Int? = null): Int? {
            return getPropertyFactory(workerName, property).getInteger() ?: defaultValue
        }

        fun getInteger(workerName: String, property: String, defaultValue: Int): Int {
            return getPropertyFactory(workerName, property).getInteger() ?: defaultValue
        }

        @JvmStatic
        fun getBoolean(workerName: String, property: String, defaultValue: Boolean? = null): Boolean? {
            return getPropertyFactory(workerName, property).getBoolean() ?: defaultValue
        }

        fun getBoolean(workerName: String, property: String, defaultValue: Boolean): Boolean {
            return getPropertyFactory(workerName, property).getBoolean() ?: defaultValue
        }

        @JvmStatic
        fun getString(workerName: String, property: String, defaultValue: String? = null): String? {
            return getPropertyFactory(workerName, property).getString() ?: defaultValue
        }

        fun getString(workerName: String, property: String, defaultValue: String): String {
            return getPropertyFactory(workerName, property).getString() ?: defaultValue
        }

        private fun getPropertyFactory(workerName: String, property: String): PropertyFactory {
            val key = "$property.$workerName"
            return PROPERTY_FACTORY_MAP.computeIfAbsent(
                key
            ) { PropertyFactory(PROPERTY_PREFIX, property, workerName) }
        }
    }
}
