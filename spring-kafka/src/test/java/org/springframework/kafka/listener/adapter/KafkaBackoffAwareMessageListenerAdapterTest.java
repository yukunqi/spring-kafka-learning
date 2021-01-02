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

package org.springframework.kafka.listener.adapter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

import java.math.BigInteger;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.support.Acknowledgment;

/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
class KafkaBackoffAwareMessageListenerAdapterTest {

	private final String testTopic = "testTopic";
	private final int testPartition = 0;

	@Mock
	AcknowledgingConsumerAwareMessageListener<Object, Object> delegate;

	@Mock
	Acknowledgment ack;

	@Mock
	ConsumerRecord<Object, Object> data;

	@Mock
	Consumer<?, ?> consumer;

	@Mock
	Headers headers;

	@Mock
	Header header;

	@Captor
	ArgumentCaptor<Long> timestampCaptor;

	@Mock
	KafkaConsumerBackoffManager kafkaConsumerBackoffManager;

	Clock clock = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault());

	private long originalTimestamp = Instant.now(this.clock).toEpochMilli();

	private byte[] originalTimestampBytes = BigInteger.valueOf(originalTimestamp).toByteArray();

	@Mock
	KafkaConsumerBackoffManager.Context context;

	String listenerId = "testListenerId";

	@Test
	void shouldThrowIfMethodWithNoAckInvoked() {
		// setup
		KafkaBackoffAwareMessageListenerAdapter<Object, Object> backoffAwareMessageListenerAdapter =
				new KafkaBackoffAwareMessageListenerAdapter<>(delegate, kafkaConsumerBackoffManager, listenerId);

		// when - then
		assertThrows(UnsupportedOperationException.class, () ->  backoffAwareMessageListenerAdapter.onMessage(data));
	}

	@Test
	void shouldJustDelegateIfNoBackoffHeaderPresent() {

		// setup
		given(data.headers()).willReturn(headers);
		given(headers.lastHeader(RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP)).willReturn(null);

		KafkaBackoffAwareMessageListenerAdapter<Object, Object> backoffAwareMessageListenerAdapter =
				new KafkaBackoffAwareMessageListenerAdapter<>(delegate, kafkaConsumerBackoffManager, listenerId);

		backoffAwareMessageListenerAdapter.onMessage(data, ack, consumer);

		then(delegate)
				.should(times(1))
				.onMessage(data, ack, consumer);
		then(ack)
				.should(times(1))
				.acknowledge();
	}

	@Test
	void shouldCallBackoffManagerIfBackoffHeaderIsPresent() {

		// setup
		given(data.headers()).willReturn(headers);
		given(headers.lastHeader(RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP))
				.willReturn(header);

		given(header.value())
				.willReturn(originalTimestampBytes);
		given(data.topic()).willReturn(testTopic);
		given(data.partition()).willReturn(testPartition);
		TopicPartition topicPartition = new TopicPartition(testTopic, testPartition);

		given(kafkaConsumerBackoffManager.createContext(originalTimestamp, listenerId, topicPartition))
					.willReturn(context);

		KafkaBackoffAwareMessageListenerAdapter<Object, Object> backoffAwareMessageListenerAdapter =
				new KafkaBackoffAwareMessageListenerAdapter<>(delegate, kafkaConsumerBackoffManager, listenerId);

		// given
		backoffAwareMessageListenerAdapter.onMessage(data, ack, consumer);

		// then
		then(kafkaConsumerBackoffManager).should(times(1))
				.createContext(timestampCaptor.capture(), eq(listenerId), eq(topicPartition));
		assertEquals(originalTimestamp, timestampCaptor.getValue());
		then(kafkaConsumerBackoffManager).should(times(1))
				.maybeBackoff(context);

		then(delegate).should(times(1)).onMessage(data, ack, consumer);
		then(ack).should(times(1)).acknowledge();
	}
}
