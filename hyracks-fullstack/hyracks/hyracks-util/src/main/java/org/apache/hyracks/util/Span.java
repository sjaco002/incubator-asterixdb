/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.util;

import java.util.concurrent.TimeUnit;

public class Span {
    private final long startNanos;
    private final long spanNanos;

    private Span(long span, TimeUnit unit) {
        startNanos = System.nanoTime();
        spanNanos = unit.toNanos(span);
    }

    public static Span start(long span, TimeUnit unit) {
        return new Span(span, unit);
    }

    public boolean elapsed() {
        return elapsed(TimeUnit.NANOSECONDS) > spanNanos;
    }

    public long elapsed(TimeUnit unit) {
        return unit.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }

    public void sleep(long sleep, TimeUnit unit) throws InterruptedException {
        TimeUnit.NANOSECONDS.sleep(Math.min(elapsed(TimeUnit.NANOSECONDS), unit.toNanos(sleep)));
    }

    public long remaining(TimeUnit unit) {
        return unit.convert(Long.max(spanNanos - elapsed(TimeUnit.NANOSECONDS), 0L), TimeUnit.NANOSECONDS);
    }

    public void loopUntilExhausted(ThrowingAction action) throws Exception {
        loopUntilExhausted(action, 0, TimeUnit.NANOSECONDS);
    }

    public void loopUntilExhausted(ThrowingAction action, long delay, TimeUnit delayUnit) throws Exception {
        while (!elapsed()) {
            action.run();
            if (elapsed(delayUnit) < delay) {
                break;
            }
            delayUnit.sleep(delay);
        }
    }
}
