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

package org.springframework.kafka.retrytopic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

import java.math.BigInteger;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.listener.adapter.AbstractDelegatingMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.KafkaBackoffAwareMessageListenerAdapter;
import org.springframework.kafka.support.Acknowledgment;

/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
@SuppressWarnings({"unchecked", "rawtypes"})
class ListenerContainerFactoryConfigurerTests {

	@Mock
	private KafkaConsumerBackoffManager kafkaConsumerBackoffManager;

	@Mock
	private DeadLetterPublishingRecovererFactory deadLetterPublishingRecovererFactory;

	@Mock
	private DeadLetterPublishingRecovererFactory.Configuration recovererConfiguration;

	@Mock
	private DeadLetterPublishingRecoverer recoverer;

	@Mock
	private ContainerProperties containerProperties;

	@Captor
	private ArgumentCaptor<ErrorHandler> errorHandlerCaptor;

	private ConsumerRecord<?, ?> record = new ConsumerRecord<>("test-topic", 1, 1234L, new Object(), new Object());

	private List<ConsumerRecord<?, ?>> records = Collections.singletonList(record);

	@Mock
	private Consumer<?, ?> consumer;

	@Mock
	private ConcurrentMessageListenerContainer<?, ?> container;

	@Mock
	private OffsetCommitCallback offsetCommitCallback;

	@Mock
	private java.util.function.Consumer<ErrorHandler> errorHandlerCustomizer;

	@SuppressWarnings("rawtypes")
	@Captor
	private ArgumentCaptor<ContainerCustomizer> containerCustomizerCaptor;

	@Mock
	private ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory;

	@Mock
	private AcknowledgingConsumerAwareMessageListener<?, ?> listener;

	@Mock
	private MessageListener<?, ?> wrongListener;

	@Captor
	private ArgumentCaptor<AbstractDelegatingMessageListenerAdapter<?>> listenerAdapterCaptor;

	@SuppressWarnings("rawtypes")
	@Mock
	private ConsumerRecord data;

	@Mock
	private Acknowledgment ack;

	@Captor
	private ArgumentCaptor<LocalDateTime> timestampCaptor;

	@Captor
	private ArgumentCaptor<String> listenerIdCaptor;

	@Mock
	private java.util.function.Consumer<ConcurrentMessageListenerContainer<?, ?>> configurerContainerCustomizer;

	private final Clock clock = TestClockUtils.CLOCK;

	private long originalTimestamp = Instant.now(this.clock).toEpochMilli();

	private byte[] originalTimestampBytes = BigInteger.valueOf(originalTimestamp).toByteArray();

	@Test
	void shouldSetupErrorHandling() {

		// setup
		given(containerFactory.getContainerProperties()).willReturn(containerProperties);
		given(container.getContainerProperties()).willReturn(containerProperties);
		given(deadLetterPublishingRecovererFactory.create(recovererConfiguration)).willReturn(recoverer);
		given(containerProperties.getAckMode()).willReturn(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		given(containerProperties.getCommitCallback()).willReturn(offsetCommitCallback);

		// given
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager, deadLetterPublishingRecovererFactory);
		configurer.setErrorHandlerCustomizer(errorHandlerCustomizer);
		ConcurrentKafkaListenerContainerFactory<?, ?> factory = configurer.configure(containerFactory, recovererConfiguration);

		// then
		then(containerFactory).should(times(1)).setErrorHandler(errorHandlerCaptor.capture());
		ErrorHandler errorHandler = errorHandlerCaptor.getValue();
		assertTrue(SeekToCurrentErrorHandler.class.isAssignableFrom(errorHandler.getClass()));
		SeekToCurrentErrorHandler seekToCurrent = (SeekToCurrentErrorHandler) errorHandler;

		RuntimeException ex = new RuntimeException();
		seekToCurrent.handle(ex, records, consumer, container);

		then(recoverer).should(times(1)).accept(record, consumer, ex);
		then(containerProperties).should(times(1))
				.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		then(consumer).should(times(1)).commitAsync(any(Map.class), eq(offsetCommitCallback));
		then(errorHandlerCustomizer).should(times(1)).accept(errorHandler);

	}

	@Test
	void shouldNotOverrideIdlePartitionEventInterval() {

		// setup
		long idlePartitionInterval = 100L;
		given(containerFactory.getContainerProperties()).willReturn(containerProperties);
		given(container.getContainerProperties()).willReturn(containerProperties);
		given(deadLetterPublishingRecovererFactory.create(recovererConfiguration)).willReturn(recoverer);
		given(containerProperties.getIdlePartitionEventInterval()).willReturn(idlePartitionInterval);
		given(containerProperties.getMessageListener()).willReturn(listener);

		// given
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager, deadLetterPublishingRecovererFactory);
		ConcurrentKafkaListenerContainerFactory<?, ?> factory = configurer.configure(containerFactory, recovererConfiguration);

		// then
		then(containerFactory).should(times(1)).setContainerCustomizer(containerCustomizerCaptor.capture());
		ContainerCustomizer containerCustomizer = containerCustomizerCaptor.getValue();
		containerCustomizer.configure(container);

		then(containerProperties).should(times(0)).setIdlePartitionEventInterval(anyLong());
	}

	@Test
	void shouldSetIdlePartitionEventIntervalIfNull() {

		// setup
		long idlePartitionInterval = 1000L;
		given(containerFactory.getContainerProperties()).willReturn(containerProperties);
		given(container.getContainerProperties()).willReturn(containerProperties);
		given(deadLetterPublishingRecovererFactory.create(recovererConfiguration)).willReturn(recoverer);
		given(containerProperties.getIdlePartitionEventInterval()).willReturn(null);
		given(containerProperties.getMessageListener()).willReturn(listener);

		// given
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager, deadLetterPublishingRecovererFactory);
		ConcurrentKafkaListenerContainerFactory<?, ?> factory = configurer.configure(containerFactory, recovererConfiguration);

		// then
		then(containerFactory).should(times(1)).setContainerCustomizer(containerCustomizerCaptor.capture());
		ContainerCustomizer containerCustomizer = containerCustomizerCaptor.getValue();
		containerCustomizer.configure(container);
		then(containerProperties).should(times(1)).setIdlePartitionEventInterval(idlePartitionInterval);
	}

	@Test
	void shouldSetupMessageListenerAdapter() {

		// setup
		given(containerFactory.getContainerProperties()).willReturn(containerProperties);
		given(container.getContainerProperties()).willReturn(containerProperties);
		given(deadLetterPublishingRecovererFactory.create(recovererConfiguration)).willReturn(recoverer);
		given(containerProperties.getIdlePartitionEventInterval()).willReturn(null);
		given(containerProperties.getMessageListener()).willReturn(listener);
		RecordHeaders headers = new RecordHeaders();
		headers.add(RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP, originalTimestampBytes);
		given(data.headers()).willReturn(headers);
		String testListenerId = "testListenerId";
		given(container.getListenerId()).willReturn(testListenerId);

		// given
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager, deadLetterPublishingRecovererFactory);
		configurer.setContainerCustomizer(configurerContainerCustomizer);
		ConcurrentKafkaListenerContainerFactory<?, ?> factory = configurer.configure(containerFactory, recovererConfiguration);

		// then
		then(containerFactory)
				.should(times(1))
				.setContainerCustomizer(containerCustomizerCaptor.capture());
		ContainerCustomizer containerCustomizer = containerCustomizerCaptor.getValue();
		containerCustomizer.configure(container);

		then(container).should(times(1)).setupMessageListener(listenerAdapterCaptor.capture());
		KafkaBackoffAwareMessageListenerAdapter<?, ?> listenerAdapter = (KafkaBackoffAwareMessageListenerAdapter<?, ?>) listenerAdapterCaptor.getValue();
		listenerAdapter.onMessage(data, ack, consumer);

		then(this.kafkaConsumerBackoffManager).should(times(1))
				.createContext(anyLong(), listenerIdCaptor.capture(), any(TopicPartition.class));
		assertEquals(testListenerId, listenerIdCaptor.getValue());
		then(listener).should(times(1)).onMessage(data, ack, consumer);

		then(this.configurerContainerCustomizer).should(times(1)).accept(container);
	}
	@Test
	void shouldThrowIfNotAckowledgingMessageListenerAdapter() {

		// setup
		given(containerFactory.getContainerProperties()).willReturn(containerProperties);
		given(container.getContainerProperties()).willReturn(containerProperties);
		given(deadLetterPublishingRecovererFactory.create(recovererConfiguration)).willReturn(recoverer);
		given(containerProperties.getMessageListener()).willReturn(wrongListener);

		RecordHeaders headers = new RecordHeaders();
		headers.add(RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP, originalTimestampBytes);
		String testListenerId = "testListenerId";

		// given
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager, deadLetterPublishingRecovererFactory);
		configurer.setContainerCustomizer(configurerContainerCustomizer);
		ConcurrentKafkaListenerContainerFactory<?, ?> factory = configurer.configure(containerFactory, recovererConfiguration);

		// then
		then(containerFactory)
				.should(times(1))
				.setContainerCustomizer(containerCustomizerCaptor.capture());
		ContainerCustomizer containerCustomizer = containerCustomizerCaptor.getValue();

		assertThrows(IllegalArgumentException.class, () -> containerCustomizer.configure(container));

	}

	@Test
	void shouldCacheFactoryInstances() {

		// setup
		given(containerFactory.getContainerProperties()).willReturn(containerProperties);
		given(deadLetterPublishingRecovererFactory.create(recovererConfiguration)).willReturn(recoverer);

		// given
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager, deadLetterPublishingRecovererFactory);
		ConcurrentKafkaListenerContainerFactory<?, ?> factory = configurer.configure(containerFactory, recovererConfiguration);
		ConcurrentKafkaListenerContainerFactory<?, ?> secondFactory = configurer.configure(containerFactory, recovererConfiguration);

		// then
		assertEquals(factory, secondFactory);
		then(containerFactory).should(times(1)).setContainerCustomizer(any());
		then(containerFactory).should(times(1)).setErrorHandler(any());
		then(containerProperties).should(times(1)).setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
	}
}
