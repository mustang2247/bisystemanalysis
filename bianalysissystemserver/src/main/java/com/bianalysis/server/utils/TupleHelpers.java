/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bianalysis.server.utils;

import org.apache.storm.tuple.Tuple;

public final class TupleHelpers {

    private TupleHelpers() {
    }

    /**
     * 是否为设置的tick机制
     * 一种定时机制
     * @param tuple
     * @return
     */
    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(org.apache.storm.Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(
                org.apache.storm.Constants.SYSTEM_TICK_STREAM_ID);
    }

    /**
     * 是否为设置的tick机制
     * 一种定时机制
     * @param tuple
     * @return
     */
    public static boolean isTickTuple(com.twitter.heron.api.tuple.Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(
                Constants.SYSTEM_TICK_STREAM_ID);
    }

}
