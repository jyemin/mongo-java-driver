/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.connection;

import com.mongodb.MongoInternalException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ThreadFactory;

/**
 * Utility class for thread creation that will create virtual threads if supported by the platform.  Virtual threads
 * became generally available in Java 21.
 *
 * <p>
 * Because Java 21 is not required by the driver yet, the JDK's virtual thread API is called reflectively in order to avoid link errors
 * when running on pre-Java 21 runtimes.
 * </p>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ThreadUtil {
    private static final boolean VIRTUAL_THREADS_AVAILABLE;
    private static final Method OF_VIRTUAL_FACTORY_METHOD;
    private static final Method OF_VIRTUAL_BUILDER_NAME_METHOD;
    private static final Method OF_VIRTUAL_BUILDER_FACTORY_METHOD;

    static {
        boolean virtualThreadAvailable = false;
        Method ofVirtualFactoryMethod = null;
        Method ofVirtualBuilderNameMethod = null;
        Method ofVirtualBuilderFactoryMethod = null;
        try {
            ofVirtualFactoryMethod = Thread.class.getMethod("ofVirtual");
            Class<?> ofVirtualBuilderClass = Class.forName("java.lang.Thread$Builder$OfVirtual");
            ofVirtualBuilderNameMethod = ofVirtualBuilderClass.getMethod("name", String.class);
            ofVirtualBuilderFactoryMethod = ofVirtualBuilderClass.getMethod("factory");
            virtualThreadAvailable = true;
        } catch (NoSuchMethodException | ClassNotFoundException ignored) {
        }

        VIRTUAL_THREADS_AVAILABLE = virtualThreadAvailable;
        OF_VIRTUAL_FACTORY_METHOD = ofVirtualFactoryMethod;
        OF_VIRTUAL_BUILDER_NAME_METHOD = ofVirtualBuilderNameMethod;
        OF_VIRTUAL_BUILDER_FACTORY_METHOD = ofVirtualBuilderFactoryMethod;
    }


    public static ThreadFactory createThreadFactory(final String name) {
        if (VIRTUAL_THREADS_AVAILABLE) {
            try {
                Object ofVirtualBuilder = OF_VIRTUAL_FACTORY_METHOD.invoke(null);
                OF_VIRTUAL_BUILDER_NAME_METHOD.invoke(ofVirtualBuilder, name);
                return (ThreadFactory) OF_VIRTUAL_BUILDER_FACTORY_METHOD.invoke(ofVirtualBuilder);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new MongoInternalException("Unexpected exception", e);
            }
        } else {
            return runnable -> {
                Thread thread = new Thread(runnable, name);
                thread.setDaemon(true);
                return thread;
            };
        }
    }
}
