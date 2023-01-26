package com.netflix.conductor.client.kotlin.http.jersey;

import com.netflix.conductor.common.metadata.events.EventHandler;
import com.sun.jersey.api.client.GenericType;

import java.util.List;

public class Constants {
    public static final GenericType<List<EventHandler>> eventHandlerList = new GenericType<>() {};
}
