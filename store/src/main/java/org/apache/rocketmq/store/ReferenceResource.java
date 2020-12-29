/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {
    protected final AtomicLong refCount = new AtomicLong(1);
    protected volatile boolean available = true;
    protected volatile boolean cleanupOver = false;
    private volatile long firstShutdownTimestamp = 0;

    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    /**
     * MappedFile文件销毁的实现方法
     * @param intervalForcibly：表示拒绝被销毁的最大存活时间
     */
    public void shutdown(final long intervalForcibly) {
        //初次调用时this.available为true ，设置available为false ，并
        //设置初次关闭的时间戳（ firstShutdownTimestamp ）为当前时间戳， 然后调用release （）方法
        //尝试释放资源
        if (this.available) {
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        } else if (this.getRefCount() > 0) {
            //对比当前时间与firstShutdownTimestamp ，如果已经超过了其最大拒绝存活期，每执行
            //一次，将引用数减少1000 ，直到引用数小于0 时通过执行realse 方法释放资源。
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }


    public void release() {
        //将引用次数减l
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;
       //如果引用次数小于等于0 ，则执行cleanup 方法
        synchronized (this) {
            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    /**
     * ，对比当前时间与firstShutdownTimestamp ，如果已经超过了其最大拒绝存活期，每执行
     * 一次，将引用数减少1000 ，直到引用数小于0 时通过执行realse 方法释放资源。
     * @return
     */
    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
