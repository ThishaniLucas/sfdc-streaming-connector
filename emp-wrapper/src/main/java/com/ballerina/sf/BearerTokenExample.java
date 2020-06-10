// Copyright (c) 2020 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.ballerina.sf;

import org.cometd.bayeux.Channel;
import org.eclipse.jetty.util.ajax.JSON;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.ballerinalang.jvm.BRuntime;
import org.ballerinalang.jvm.BallerinaValues;
import org.ballerinalang.jvm.types.AttachedFunction;
import org.ballerinalang.jvm.types.BPackage;
import org.ballerinalang.jvm.values.HandleValue;
import org.ballerinalang.jvm.values.MapValue;
import org.ballerinalang.jvm.values.ObjectValue;

/**
 * An example of using the EMP connector using bearer tokens
 *
 * @author hal.hildebrand
 * @since API v37.0
 */
public class BearerTokenExample {

    public static void makeConnect(String apiUrl, String bearerToken, String topic, ObjectValue serviceObject) throws Exception {
        long replayFrom = EmpConnector.REPLAY_FROM_EARLIEST;
        BRuntime runtime = BRuntime.getCurrentRuntime();
        BayeuxParameters params = new BayeuxParameters() {
            @Override
            public String bearerToken() {
                return bearerToken;
            }
            @Override
            public URL host() {
                try {
                    return new URL(apiUrl);
                } catch (MalformedURLException e) {
                    throw new IllegalArgumentException(String.format("Unable to create url: %s", apiUrl), e);
                }
            }
        };
        Consumer<Map<String, Object>> consumer = event -> new EventReceiver(serviceObject,runtime).injectEvent(event);
        EmpConnector connector = new EmpConnector(params);
        LoggingListener loggingListener = new LoggingListener(true, true);
        connector.addListener(Channel.META_CONNECT, loggingListener)
        .addListener(Channel.META_DISCONNECT, loggingListener)
        .addListener(Channel.META_HANDSHAKE, loggingListener);
        connector.start().get(5, TimeUnit.SECONDS);
        TopicSubscription subscription = connector.subscribe(topic, replayFrom, consumer).get(5, TimeUnit.SECONDS);
        System.out.println(String.format("Subscribed: %s", subscription));
    }

    private static class EventReceiver {
        private final ObjectValue serviceObject;
        private final BRuntime runtime;

        EventReceiver(ObjectValue serviceObject, BRuntime runtime) {
            this.serviceObject = serviceObject;
            this.runtime = runtime;
        }

        public void injectEvent(Map<String, Object> event) {
            System.out.println(String.format("Received:\n%s", JSON.toString(event)));
            runtime.invokeMethodAsync(serviceObject, "onEvent", JSON.toString(event), true);
        }
    }
}
