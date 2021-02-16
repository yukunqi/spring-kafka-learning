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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.event.ListenerContainerPartitionIdleEvent;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;

/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
class KafkaConsumerBackoffManagerTest {

	private final String testListenerId = "testListenerId";

	@Mock
	KafkaListenerEndpointRegistry registry;

	@Mock
	MessageListenerContainer listenerContainer;

	Clock clock = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault());

	String testTopic = "testTopic";

	int testPartition = 0;

	private TopicPartition topicPartition = new TopicPartition(testTopic, testPartition);

	@Mock
	private ListenerContainerPartitionIdleEvent partitionIdleEvent;

	@Test
	void shouldBackoffgivenDueTimestampIsLater() {

		// setup
		given(this.registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		KafkaConsumerBackoffManager backoffManager = new KafkaConsumerBackoffManager(registry, clock);
		LocalDateTime dueTimestamp = LocalDateTime.now(clock).plusMinutes(5);
		String expectedTimestamp = dueTimestamp.format(
				RetryTopicHeaders.DEFAULT_BACKOFF_TIMESTAMP_HEADER_FORMATTER);
		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(dueTimestamp, testListenerId, topicPartition);

		// given
		KafkaBackoffException backoffException = assertThrows(KafkaBackoffException.class,
				() -> backoffManager.maybeBackoff(context));

		// then
		assertEquals(expectedTimestamp, backoffException.getDueTimestamp());
		assertEquals(testListenerId, backoffException.getListenerId());
		assertEquals(topicPartition, backoffException.getTopicPartition());
		assertEquals(context, backoffManager.getBackoff(topicPartition));
		then(listenerContainer).should(times(1)).pausePartition(topicPartition);
	}

	@Test
	void shouldNotBackoffgivenDueTimestampIsPast() {

		// setup
		KafkaConsumerBackoffManager backoffManager = new KafkaConsumerBackoffManager(registry, clock);
		LocalDateTime dueTimestamp = LocalDateTime.now(clock).minusMinutes(5);
		String expectedTimestamp = dueTimestamp.format(
				RetryTopicHeaders.DEFAULT_BACKOFF_TIMESTAMP_HEADER_FORMATTER);
		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(dueTimestamp, testListenerId, topicPartition);

		// given
		backoffManager.maybeBackoff(context);

		// then
		assertNull(backoffManager.getBackoff(topicPartition));
		then(listenerContainer).should(times(0)).pausePartition(topicPartition);
	}

	@Test
	void shouldDoNothingIfIdleBeforeDueTimestamp() {

		// setup
		given(this.partitionIdleEvent.getTopicPartition()).willReturn(topicPartition);
		KafkaConsumerBackoffManager backoffManager = new KafkaConsumerBackoffManager(registry, clock);
		LocalDateTime dueTimestamp = LocalDateTime.now(clock).plusMinutes(5);
		String expectedTimestamp = dueTimestamp.format(
				RetryTopicHeaders.DEFAULT_BACKOFF_TIMESTAMP_HEADER_FORMATTER);
		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(dueTimestamp, testListenerId, topicPartition);
		backoffManager.addBackoff(context, topicPartition);

		// given
		backoffManager.onApplicationEvent(partitionIdleEvent);

		// then
		assertEquals(context, backoffManager.getBackoff(topicPartition));
		then(listenerContainer).should(times(0)).resumePartition(topicPartition);
	}

	@Test
	void shouldResumePartitionIfIdleAfterDueTimestamp() {

		// setup
		given(this.registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		given(this.partitionIdleEvent.getTopicPartition()).willReturn(topicPartition);
		KafkaConsumerBackoffManager backoffManager = new KafkaConsumerBackoffManager(registry, clock);
		LocalDateTime dueTimestamp = LocalDateTime.now(clock).minusMinutes(5);
		String expectedTimestamp = dueTimestamp.format(
				RetryTopicHeaders.DEFAULT_BACKOFF_TIMESTAMP_HEADER_FORMATTER);
		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(dueTimestamp, testListenerId, topicPartition);
		backoffManager.addBackoff(context, topicPartition);

		// given
		backoffManager.onApplicationEvent(partitionIdleEvent);

		// then
		assertNull(backoffManager.getBackoff(topicPartition));
		then(listenerContainer).should(times(1)).resumePartition(topicPartition);
	}
}
