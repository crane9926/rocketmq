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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 消息失败策略，延迟实现的门面类
 */
public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;

    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * 根据topic信息，选择一个消息队列
     * @param tpInfo
     * @param lastBrokerName
     * @return
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        if (this.sendLatencyFaultEnable) {
            try {
                //获取threadLocal变量
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                //首先循环所有队列，判断是否存在brokerName=lastBrokerName并且可用的消息队列，如果存在则直接返回该mq
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    //使用index与队列个数取模，获取一个位置pos
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    //获取pos位置的MessageQueue
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    //判断队列是否可用
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }

                //如果不存在上述消息队列，则尝试从规避的broker中选择一个相对较好的broker
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {

                    //？？？？从原来的队列 列表中选择一个queue,修改其所在的broker为notBestBroker（猜测：broker发送心跳是会把新的queue信息更新到nameServer）

                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    //移除不可用broker
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            return tpInfo.selectOneMessageQueue();
        }
        //否则选择brokerName！=lastBrokerName的队列（规避掉上次发送的broker）
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     *
     * @param brokerName
     * @param currentLatency:本次消息发送延迟时间
     * @param isolation:是否隔离(如果为true则使用30s来计算broker故障规避时长，如果为false则使用currentLatency来计算broker故障规避时长)
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            //计算broker故障规避时长
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    /**
     * 从latencyMax尾部开始寻找，找到第一个比currentLatency小的下标，然后从notAvailableDuration数组中获取需要规避的时长。
     * @param currentLatency
     * @return
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
