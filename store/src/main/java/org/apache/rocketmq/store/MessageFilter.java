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

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * RocketMQ 支持表达式过滤与类过滤两种模式。
 * 表达式又分为TAG 和SQL92，类过滤模式允许提交－个过滤类到FilterServer ，消息消费者从FilterServer 拉取消息，消息经
 * 过FilterServer 时会执行过滤逻辑。
 *
 * SQL92 表达式以消息属性过滤上下文。
 * TAG 模式就是简单为消息定义标签，根据消息属性tag 进行匹配。
 *
 *
 *1.消息发送者在消息发送时如果设置了消息的tags 属性，存储在消息属性中，先存储在
 * CommitLog文件中，然后转发到消息消费队列，消息消费队列会用8 个字节存储消息tag的hashcode。
 *
 * 2.在Broker端拉取消息时，遍历ConsumeQueue ，只对比消息tag的hashcode ， 如果匹配则返回，否则忽略该消息
 *
 * 3.Consume在收到消息后，同样需要先对消息进行过滤，只是此时比较的是消息tag的值而不再是hashcode。
 *
 *
 *
 */
public interface MessageFilter {
    /**
     * match by tags code or filter bit map which is calculated when message received
     * and stored in consume queue ext.
     *根据ConsumeQueue 判断消息是否匹配。
     * @param tagsCode tagsCode  消息tag 的hashcode。
     * @param cqExtUnit extend unit of consume queue  consumequeue条目扩展属性。
     */
    boolean isMatchedByConsumeQueue(final Long tagsCode,
        final ConsumeQueueExt.CqExtUnit cqExtUnit);

    /**
     * match by message content which are stored in commit log.
     * <br>{@code msgBuffer} and {@code properties} are not all null.If invoked in store,
     * {@code properties} is null;If invoked in {@code PullRequestHoldService}, {@code msgBuffer} is null.
     *根据存储在commitlog 文件中的内容判断消息是否匹配。
     * @param msgBuffer message buffer in commit log, may be null if not invoked in store. 消息内容
     * @param properties message properties, should decode from buffer if null by yourself. 消息属性，主要用于SQL92过滤模式
     */
    boolean isMatchedByCommitLog(final ByteBuffer msgBuffer,
        final Map<String, String> properties);
}
