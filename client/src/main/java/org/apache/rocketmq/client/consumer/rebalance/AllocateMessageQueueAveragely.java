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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 * 平均负载策略
 */
public class AllocateMessageQueueAveragely extends AbstractAllocateMessageQueueStrategy {
	@Override
	public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {

		List<MessageQueue> result = new ArrayList<>();
		if (!check(consumerGroup, currentCID, mqAll, cidAll)) { return result; }
        //获取当前客户端id在所有消费者里面的位置
		int index = cidAll.indexOf(currentCID);
        //看是否能够均分
		int mod = mqAll.size() % cidAll.size();
        //如果消息队列的数量小于等于消费者的数量。一个消费者最多只能分到一个消息消息队列
        //否则，如果不能均分且index小于mode，均分以后再加上一个
		int averageSize = mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size() + 1 : mqAll.size() / cidAll.size());
		//开始的位置
		int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
		//被分配的消息队列的范围上限
		int range = Math.min(averageSize, mqAll.size() - startIndex);
		for (int i = 0; i < range; i++) {
			result.add(mqAll.get((startIndex + i) % mqAll.size()));
		}
		return result;
	}

	@Override
	public String getName() {
		return "AVG";
	}
}
