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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.kafka.event.ListenerContainerPartitionIdleEvent;

/**
 *
 * A manager that backs off consumption for a given topic if the timestamp provided is not
 * due. Use with {@link SeekToCurrentErrorHandler} to guarantee that the message is read
 * again after partition consumption is resumed (or seek it manually by other means).
 * It's also necessary to set a {@link ContainerProperties#setIdlePartitionEventInterval(Long)}
 * so the Manager can resume the partition consumption.
 *
 * Note that when a record backs off the partition consumption gets paused for
 * approximately that amount of time, so you must have a fixed backoff value per partition
 * in order to make sure no record waits more than it should.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 * @see SeekToCurrentErrorHandler
 */
public class KafkaConsumerBackoffManager implements ApplicationListener<ListenerContainerPartitionIdleEvent> {

	/**
	 * Internal Back Off Clock Bean Name.
	 */
	public static final String INTERNAL_BACKOFF_CLOCK_BEAN_NAME = "internalBackOffClock";

	private final ListenerContainerRegistry registry;

	private final Map<TopicPartition, Context> backOffTimes;

	private final Clock clock;

	public KafkaConsumerBackoffManager(ListenerContainerRegistry registry,
									@Qualifier(INTERNAL_BACKOFF_CLOCK_BEAN_NAME) Clock clock) {

		this.registry = registry;
		this.clock = clock;
		this.backOffTimes = new HashMap<>();
	}

	/**
	 * Backs off if the current time is before the dueTimestamp provided
	 * in the {@link Context} object.
	 * @param context the state that will be used for backing off.
	 */
	public void maybeBackoff(Context context) {
		long backoffTime = ChronoUnit.MILLIS.between(Instant.now(this.clock),
				Instant.ofEpochMilli(context.dueTimestamp));
		if (backoffTime > 0) {
			pauseConsumptionAndThrow(context, backoffTime);
		}
	}

	private void pauseConsumptionAndThrow(Context context, Long timeToSleep) throws KafkaBackoffException {
		TopicPartition topicPartition = context.topicPartition;
		getListenerContainerFromContext(context).pausePartition(topicPartition);
		addBackoff(context, topicPartition);
		throw new KafkaBackoffException(String.format("Partition %s from topic %s is not ready for consumption, " +
				"backing off for approx. %s millis.", context.topicPartition.partition(),
				context.topicPartition.topic(), timeToSleep),
				topicPartition, context.listenerId, context.dueTimestamp);
	}

	@Override
	public void onApplicationEvent(ListenerContainerPartitionIdleEvent partitionIdleEvent) {
		Context context = getBackoff(partitionIdleEvent.getTopicPartition());
		if (context == null || isNotDue(context.dueTimestamp)) {
			return;
		}
		MessageListenerContainer container = getListenerContainerFromContext(context);
		container.resumePartition(context.topicPartition);
		removeBackoff(context.topicPartition);
	}

	private boolean isNotDue(long dueTimestamp) {
		return Instant.now(this.clock).isBefore(Instant.ofEpochMilli(dueTimestamp));
	}

	private MessageListenerContainer getListenerContainerFromContext(Context context) {
		return this.registry.getListenerContainer(context.listenerId);
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

	public Context createContext(long dueTimestamp, String listenerId, TopicPartition topicPartition) {
		return new Context(dueTimestamp, listenerId, topicPartition);
	}

	/**
	 * Provides the state that will be used for backing off.
	 * @since 2.7
	 */
	public static class Context {

		/**
		 * The time after which the message should be processed,
		 * in milliseconds since epoch.
		 */
		final long dueTimestamp; // NOSONAR

		/**
		 * The id for the listener that should be paused.
		 */
		final String listenerId; // NOSONAR

		/**
		 * The topic that contains the partition to be paused.
		 */
		final TopicPartition topicPartition; // NOSONAR

		Context(long dueTimestamp, String listenerId, TopicPartition topicPartition) {
			this.dueTimestamp = dueTimestamp;
			this.listenerId = listenerId;
			this.topicPartition = topicPartition;
		}
	}
}
