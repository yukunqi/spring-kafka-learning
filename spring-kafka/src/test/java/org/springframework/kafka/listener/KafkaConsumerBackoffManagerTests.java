/*
 * Copyright 2019-2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import java.math.BigInteger;
import java.time.Clock;
import java.time.Instant;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.event.ListenerContainerPartitionIdleEvent;
import org.springframework.kafka.retrytopic.TestClockUtils;
import org.springframework.retry.backoff.Sleeper;

/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
class KafkaConsumerBackoffManagerTests {

	@Mock
	private KafkaListenerEndpointRegistry registry;

	@Mock
	private MessageListenerContainer listenerContainer;

	@Mock
	private ListenerContainerPartitionIdleEvent partitionIdleEvent;

	@Mock
	private TaskExecutor taskExecutor;

	@Mock
	private Consumer<?, ?> consumer;

	@Mock
	private ContainerProperties containerProperties;

	@Mock
	private Sleeper sleeper;

	@Captor
	private ArgumentCaptor<Runnable> correctionRunnableCaptor;

	private static final String testListenerId = "testListenerId";

	private static final Clock clock = TestClockUtils.CLOCK;

	private static final String testTopic = "testTopic";

	private static final int testPartition = 0;

	private static final TopicPartition topicPartition = new TopicPartition(testTopic, testPartition);

	private static final long originalTimestamp = Instant.now(clock).minusMillis(2500L).toEpochMilli();

	private static final byte[] originalTimestampBytes = BigInteger.valueOf(originalTimestamp).toByteArray();


	@Test
	void shouldBackoffgivenDueTimestampIsLater() {

		// setup
		given(this.registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		given(registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		KafkaConsumerBackoffManager backoffManager = new KafkaConsumerBackoffManager(registry, clock, taskExecutor, sleeper);

		long dueTimestamp = originalTimestamp + 5000;
		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(dueTimestamp, testListenerId, topicPartition, consumer);

		// given
		KafkaBackoffException backoffException = catchThrowableOfType(() -> backoffManager.maybeBackoff(context),
				KafkaBackoffException.class);

		// then
		assertThat(backoffException.getDueTimestamp()).isEqualTo(dueTimestamp);
		assertThat(backoffException.getListenerId()).isEqualTo(testListenerId);
		assertThat(backoffException.getTopicPartition()).isEqualTo(topicPartition);
		assertThat(backoffManager.getBackOffContext(topicPartition)).isEqualTo(context);
		then(listenerContainer).should(times(1)).pausePartition(topicPartition);
	}

	@Test
	void shouldNotBackoffGivenDueTimestampIsPast() {

		// setup
		KafkaConsumerBackoffManager backoffManager = new KafkaConsumerBackoffManager(registry, clock, taskExecutor, sleeper);
		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(originalTimestamp - 5000, testListenerId, topicPartition, consumer);

		// given
		backoffManager.maybeBackoff(context);

		// then
		assertThat(backoffManager.getBackOffContext(topicPartition)).isNull();
		then(listenerContainer).should(times(0)).pausePartition(topicPartition);
	}

	@Test
	void shouldDoNothingIfIdleBeforeDueTimestamp() {

		// setup
		given(this.partitionIdleEvent.getTopicPartition()).willReturn(topicPartition);
		given(registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		given(listenerContainer.getContainerProperties()).willReturn(containerProperties);
		given(containerProperties.getPollTimeout()).willReturn(500L);

		KafkaConsumerBackoffManager backoffManager = new KafkaConsumerBackoffManager(registry, clock, taskExecutor, sleeper);

		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(originalTimestamp + 5000, testListenerId, topicPartition, consumer);
		backoffManager.addBackoff(context, topicPartition);

		// given
		backoffManager.onApplicationEvent(partitionIdleEvent);

		// then
		assertThat(backoffManager.getBackOffContext(topicPartition)).isEqualTo(context);
		then(listenerContainer).should(times(0)).resumePartition(topicPartition);
	}

	@Test
	void shouldResumePartitionIfIdleAfterDueTimestamp() {

		// setup
		given(registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		given(listenerContainer.getContainerProperties()).willReturn(containerProperties);
		given(containerProperties.getPollTimeout()).willReturn(500L);

		given(this.registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		given(this.partitionIdleEvent.getTopicPartition()).willReturn(topicPartition);
		KafkaConsumerBackoffManager backoffManager = new KafkaConsumerBackoffManager(registry, clock, taskExecutor, sleeper);
		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(originalTimestamp - 5000, testListenerId, topicPartition, consumer);
		backoffManager.addBackoff(context, topicPartition);

		// given
		backoffManager.onApplicationEvent(partitionIdleEvent);

		// then
		assertThat(backoffManager.getBackOffContext(topicPartition)).isNull();
		then(listenerContainer).should(times(1)).resumePartition(topicPartition);
	}

	@Test
	void shouldApplyCorrectionIfInCorrectionWindow() throws InterruptedException {

		// setup
		given(registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		given(listenerContainer.getContainerProperties()).willReturn(containerProperties);
		long pollTimout = 500L;
		given(containerProperties.getPollTimeout()).willReturn(pollTimout);

		given(this.registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		given(this.partitionIdleEvent.getTopicPartition()).willReturn(topicPartition);
		KafkaConsumerBackoffManager backoffManager = new KafkaConsumerBackoffManager(registry, clock, taskExecutor, sleeper);
		long dueBackOffTime = 750L;
		long dueTimestamp = Instant.now(clock).plusMillis(dueBackOffTime).toEpochMilli();
		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(dueTimestamp,
						testListenerId, topicPartition, consumer);
		backoffManager.addBackoff(context, topicPartition);

		// given
		backoffManager.onApplicationEvent(partitionIdleEvent);

		// then
		then(this.taskExecutor).should(times(1)).execute(correctionRunnableCaptor.capture());
		Runnable correctionRunnable = correctionRunnableCaptor.getValue();
		correctionRunnable.run();

		then(sleeper).should(times(1)).sleep(dueBackOffTime - pollTimout);
		then(consumer).should(times(1)).wakeup();

		assertThat(backoffManager.getBackOffContext(topicPartition)).isNull();
		then(listenerContainer).should(times(1)).resumePartition(topicPartition);
	}

	@Test
	void shouldNotApplyCorrectionIfTooLateForCorrectionWindow() throws InterruptedException {

		// setup
		given(registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		given(listenerContainer.getContainerProperties()).willReturn(containerProperties);
		long pollTimout = 500L;
		given(containerProperties.getPollTimeout()).willReturn(pollTimout);

		given(this.registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		given(this.partitionIdleEvent.getTopicPartition()).willReturn(topicPartition);
		KafkaConsumerBackoffManager backoffManager = new KafkaConsumerBackoffManager(registry, clock, taskExecutor, sleeper);
		long dueBackOffTime = 250L;
		long dueTimestamp = Instant.now(clock).plusMillis(dueBackOffTime).toEpochMilli();
		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(dueTimestamp,
						testListenerId, topicPartition, consumer);
		backoffManager.addBackoff(context, topicPartition);

		// given
		backoffManager.onApplicationEvent(partitionIdleEvent);

		// then
		then(this.taskExecutor).should(never()).execute(any(Runnable.class));
	}

	@Test
	void shouldNotApplyCorrectionIfTooSoonForCorrectionWindow() throws InterruptedException {

		// setup
		given(registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		given(listenerContainer.getContainerProperties()).willReturn(containerProperties);
		long pollTimout = 500L;
		given(containerProperties.getPollTimeout()).willReturn(pollTimout);

		given(this.registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		given(this.partitionIdleEvent.getTopicPartition()).willReturn(topicPartition);
		KafkaConsumerBackoffManager backoffManager = new KafkaConsumerBackoffManager(registry, clock, taskExecutor, sleeper);
		long dueBackOffTime = 1250L;
		long dueTimestamp = Instant.now(clock).plusMillis(dueBackOffTime).toEpochMilli();
		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(dueTimestamp,
						testListenerId, topicPartition, consumer);
		backoffManager.addBackoff(context, topicPartition);

		// given
		backoffManager.onApplicationEvent(partitionIdleEvent);

		// then
		then(this.taskExecutor).should(never()).execute(any(Runnable.class));
	}
}
