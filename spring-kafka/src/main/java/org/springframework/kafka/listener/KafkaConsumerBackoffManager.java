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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.core.log.LogAccessor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.event.ListenerContainerPartitionIdleEvent;
import org.springframework.lang.Nullable;
import org.springframework.retry.backoff.Sleeper;

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

	private static final LogAccessor logger = new LogAccessor(LogFactory.getLog(KafkaConsumerBackoffManager.class));
	/**
	 * Internal Back Off Clock Bean Name.
	 */
	public static final String INTERNAL_BACKOFF_CLOCK_BEAN_NAME = "internalBackOffClock";

	private static final int TIMING_CORRECTION_THRESHOLD = 100;

	private static final int POLL_TIMEOUTS_FOR_CORRECTION_WINDOW = 2;

	private final ListenerContainerRegistry registry;

	private final Map<TopicPartition, Context> backOffContexts;

	private final Clock clock;

	private final TaskExecutor taskExecutor;

	private final Sleeper sleeper;

	public KafkaConsumerBackoffManager(ListenerContainerRegistry registry,
									@Qualifier(INTERNAL_BACKOFF_CLOCK_BEAN_NAME) Clock clock,
									TaskExecutor taskExecutor,
									Sleeper sleeper) {

		this.registry = registry;
		this.clock = clock;
		this.taskExecutor = taskExecutor;
		this.sleeper = sleeper;
		this.backOffContexts = new HashMap<>();
	}

	/**
	 * Backs off if the current time is before the dueTimestamp provided
	 * in the {@link Context} object.
	 * @param context the back off context for this execution.
	 */
	public void maybeBackoff(Context context) {
		long backoffTime = context.dueTimestamp - getCurrentMillisFromClock();
		if (backoffTime > 0) {
			pauseConsumptionAndThrow(context, backoffTime);
		}
	}

	private void pauseConsumptionAndThrow(Context context, Long backOffTime) throws KafkaBackoffException {
		TopicPartition topicPartition = context.topicPartition;
		getListenerContainerFromContext(context).pausePartition(topicPartition);
		addBackoff(context, topicPartition);
		throw new KafkaBackoffException(String.format("Partition %s from topic %s is not ready for consumption, " +
				"backing off for approx. %s millis.", context.topicPartition.partition(),
				context.topicPartition.topic(), backOffTime),
				topicPartition, context.listenerId, context.dueTimestamp);
	}

	@Override
	public void onApplicationEvent(ListenerContainerPartitionIdleEvent partitionIdleEvent) {
		logger.debug(() -> String.format("partitionIdleEvent received at %s. Partition: %s",
				getCurrentMillisFromClock(), partitionIdleEvent.getTopicPartition()));

		Context backOffContext = getBackOffContext(partitionIdleEvent.getTopicPartition());

		if (backOffContext == null) {
			return;
		}
		maybeResumeConsumption(backOffContext);
	}

	private long getCurrentMillisFromClock() {
		return Instant.now(this.clock).toEpochMilli();
	}

	private void maybeResumeConsumption(Context context) {
		long now = getCurrentMillisFromClock();
		long timeUntilDue = context.dueTimestamp - now;
		long pollTimeout = getListenerContainerFromContext(context)
				.getContainerProperties()
				.getPollTimeout();
		boolean isDue = timeUntilDue <= pollTimeout;

		if (maybeApplyTimingCorrection(context, pollTimeout, timeUntilDue) || isDue) {
			resumePartition(context);
		}
		else {
			logger.debug(() -> String.format("TopicPartition %s not due. DueTimestamp: %s Now: %s ",
					context.topicPartition, context.dueTimestamp, now));
		}
	}

	private void resumePartition(Context context) {
		MessageListenerContainer container = getListenerContainerFromContext(context);
		logger.debug(() -> "Resuming partition at " + getCurrentMillisFromClock());
		container.resumePartition(context.topicPartition);
		removeBackoff(context.topicPartition);
	}

	private boolean maybeApplyTimingCorrection(Context context, long pollTimeout, long timeUntilDue) {
		// Correction can only be applied to ConsumerAwareMessageListener
		// listener instances.
		if (context.consumerForTimingCorrection == null) {
			return false;
		}

		boolean isInCorrectionWindow = timeUntilDue > pollTimeout && timeUntilDue <=
				pollTimeout * POLL_TIMEOUTS_FOR_CORRECTION_WINDOW;

		long correctionAmount = timeUntilDue % pollTimeout;
		if (isInCorrectionWindow && correctionAmount > TIMING_CORRECTION_THRESHOLD) {
			this.taskExecutor.execute(() -> doApplyTimingCorrection(context, correctionAmount));
			return true;
		}
		return false;
	}

	private void doApplyTimingCorrection(Context context, long correctionAmount) {
		try {
			logger.debug(() -> String.format("Applying correction of %s millis at %s for TopicPartition %s",
					correctionAmount, getCurrentMillisFromClock(), context.topicPartition));
			this.sleeper.sleep(correctionAmount);
			logger.debug(() -> "Waking up consumer for partition topic: " + context.topicPartition);
			context.consumerForTimingCorrection.wakeup();
		}
		catch (InterruptedException e) {
			Thread.interrupted();
			throw new IllegalStateException("Interrupted waking up consumer while applying correction " +
					"for TopicPartition " + context.topicPartition, e);
		}
		catch (Throwable e) {
			logger.error(e, () -> "Error waking up consumer while applying correction " +
					"for TopicPartition " + context.topicPartition);
		}
	}

	private MessageListenerContainer getListenerContainerFromContext(Context context) {
		return this.registry.getListenerContainer(context.listenerId);
	}

	protected void addBackoff(Context context, TopicPartition topicPartition) {
		synchronized (this.backOffContexts) {
			this.backOffContexts.put(topicPartition, context);
		}
	}

	protected Context getBackOffContext(TopicPartition topicPartition) {
		synchronized (this.backOffContexts) {
			return this.backOffContexts.get(topicPartition);
		}
	}

	protected void removeBackoff(TopicPartition topicPartition) {
		synchronized (this.backOffContexts) {
			this.backOffContexts.remove(topicPartition);
		}
	}

	public Context createContext(long dueTimestamp, String listenerId, TopicPartition topicPartition,
								@Nullable Consumer<?, ?> consumerForTimingCorrection) {
		return new Context(dueTimestamp, topicPartition, listenerId, consumerForTimingCorrection);
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
		private final long dueTimestamp; // NOSONAR

		/**
		 * The id for the listener that should be paused.
		 */
		private final String listenerId; // NOSONAR

		/**
		 * The topic that contains the partition to be paused.
		 */
		private final TopicPartition topicPartition; // NOSONAR

		/**
		 * The consumer of the message, if present.
		 */
		private final Consumer<?, ?> consumerForTimingCorrection; // NOSONAR

		Context(long dueTimestamp, TopicPartition topicPartition, String listenerId,
						@Nullable Consumer<?, ?> consumerForTimingCorrection) {
			this.dueTimestamp = dueTimestamp;
			this.listenerId = listenerId;
			this.topicPartition = topicPartition;
			this.consumerForTimingCorrection = consumerForTimingCorrection;
		}
	}
}
