/*
 * Copyright 2018-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.listener;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.TopicPartition;

import org.springframework.context.ApplicationListener;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.event.ListenerContainerPartitionIdleEvent;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;

/**
 *
 * A manager that backs off consumption for a given topic if the timestamp provided is not due.
 * Use with {@link SeekToCurrentErrorHandler} to guarantee that the message is read again after
 * partition consumption is resumed (or seek it manually by other means).
 *
 * @author Tomaz Fernandes
 * @since 2.7
 * @see SeekToCurrentErrorHandler
 */
public class KafkaConsumerBackoffManager implements ApplicationListener<ListenerContainerPartitionIdleEvent> {

	private static final LogAccessor logger = new LogAccessor(LogFactory.getLog(KafkaConsumerBackoffManager.class));

	private final KafkaListenerEndpointRegistry registry;

	private final Map<TopicPartition, Context> backOffTimes;

	private final Clock clock;

	public KafkaConsumerBackoffManager(KafkaListenerEndpointRegistry registry,
									Clock clock) {
		this.registry = registry;
		this.clock = clock;
		this.backOffTimes = new HashMap<>();
	}

	public void maybeBackoff(Context context) {
		long backoffTime = ChronoUnit.MILLIS.between(LocalDateTime.now(this.clock), context.dueTimestamp);
		if (backoffTime > 0) {
			pauseConsumptionAndThrow(context, backoffTime);
		}
	}

	private void pauseConsumptionAndThrow(Context context, Long timeToSleep) throws KafkaBackoffException {
		TopicPartition topicPartition = context.topicPartition;
		getListenerContainerFromContext(context).pausePartition(topicPartition);
		addBackoff(context, topicPartition);
		throw new KafkaBackoffException(String.format("Partition %s from topic %s is not ready for consumption, " +
				"backing off for approx. %s millis.", context.topicPartition.partition(), context.topicPartition.topic(), timeToSleep),
				topicPartition, context.listenerId, context.dueTimestamp.format(
						RetryTopicHeaders.DEFAULT_BACKOFF_TIMESTAMP_HEADER_FORMATTER));
	}

	private MessageListenerContainer getListenerContainerFromContext(Context context) {
		return this.registry.getListenerContainer(context.listenerId);
	}

	@Override
	public void onApplicationEvent(ListenerContainerPartitionIdleEvent partitionIdleEvent) {
		Context context = getBackoff(partitionIdleEvent.getTopicPartition());
		if (context == null || LocalDateTime.now(this.clock).isBefore(context.dueTimestamp)) {
			return;
		}
		MessageListenerContainer container = getListenerContainerFromContext(context);
		container.resumePartition(context.topicPartition);
		removeBackoff(context.topicPartition);
	}

	protected void addBackoff(Context context, TopicPartition topicPartition) {
		synchronized (this.backOffTimes) {
			this.backOffTimes.put(topicPartition, context);
		}
	}

	protected Context getBackoff(TopicPartition topicPartition) {
		synchronized (this.backOffTimes) {
			return this.backOffTimes.get(topicPartition);
		}
	}

	protected void removeBackoff(TopicPartition topicPartition) {
		synchronized (this.backOffTimes) {
			this.backOffTimes.remove(topicPartition);
		}
	}

	public Context createContext(LocalDateTime dueTimestamp, String listenerId, TopicPartition topicPartition) {
		return new Context(dueTimestamp, listenerId, topicPartition);
	}

	public static class Context {

		/**
		 * The time after which the message should be processed.
		 */
		final LocalDateTime dueTimestamp;

		/**
		 * The id for the listener that should be paused.
		 */
		final String listenerId;

		/**
		 * The topic that contains the partition to be paused.
		 */
		final TopicPartition topicPartition;

		Context(LocalDateTime dueTimestamp, String listenerId, TopicPartition topicPartition) {
			this.dueTimestamp = dueTimestamp;
			this.listenerId = listenerId;
			this.topicPartition = topicPartition;
		}
	}
}
