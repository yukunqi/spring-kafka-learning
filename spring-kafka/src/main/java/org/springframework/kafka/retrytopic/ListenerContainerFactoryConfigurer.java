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

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.listener.adapter.KafkaBackoffAwareMessageListenerAdapter;
import org.springframework.util.Assert;
import org.springframework.util.backoff.FixedBackOff;

/**
 *
 * Configures the provided {@link ConcurrentKafkaListenerContainerFactory} with a
 * {@link SeekToCurrentErrorHandler}, the {@link DeadLetterPublishingRecoverer} created by
 * the {@link DeadLetterPublishingRecovererFactory}.
 *
 * Mind that the same factory can be used by many different
 * {@link org.springframework.kafka.annotation.RetryableTopic}s but should not be shared
 * with non retryable topics as some of their configurations will be overriden.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public class ListenerContainerFactoryConfigurer {

	private static Set<ConcurrentKafkaListenerContainerFactory<?, ?>> configuredFactoriesCache;

	private final KafkaConsumerBackoffManager kafkaConsumerBackoffManager;

	static {
		configuredFactoriesCache = new HashSet<>();
	}

	private static final long DEFAULT_IDLE_PARTITION_EVENT_INTERVAL = 1000L;

	private final DeadLetterPublishingRecovererFactory deadLetterPublishingRecovererFactory;

	private Consumer<ConcurrentMessageListenerContainer<?, ?>> containerCustomizer = container -> {
	};

	private Consumer<ErrorHandler> errorHandlerCustomizer = errorHandler -> {
	};

	ListenerContainerFactoryConfigurer(KafkaConsumerBackoffManager kafkaConsumerBackoffManager,
			DeadLetterPublishingRecovererFactory deadLetterPublishingRecovererFactory) {

		this.kafkaConsumerBackoffManager = kafkaConsumerBackoffManager;
		this.deadLetterPublishingRecovererFactory = deadLetterPublishingRecovererFactory;
	}

	ConcurrentKafkaListenerContainerFactory<?, ?> configure(
			ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory,
			DeadLetterPublishingRecovererFactory.Configuration configuration) {
		if (existsInCache(containerFactory)) {
			return containerFactory;
		}
		containerFactory.setContainerCustomizer(container -> setupBackoffAwareMessageListenerAdapter(container));
		containerFactory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		containerFactory
				.setErrorHandler(createErrorHandler(this.deadLetterPublishingRecovererFactory.create(configuration)));
		addToFactoriesCache(containerFactory);
		return containerFactory;
	}

	private boolean existsInCache(ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory) {
		synchronized (configuredFactoriesCache) {
			return configuredFactoriesCache.contains(containerFactory);
		}
	}

	private void addToFactoriesCache(ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory) {
		synchronized (configuredFactoriesCache) {
			configuredFactoriesCache.add(containerFactory);
		}
	}

	public void setContainerCustomizer(Consumer<ConcurrentMessageListenerContainer<?, ?>> containerCustomizer) {
		Assert.notNull(containerCustomizer, "'containerCustomizer' cannot be null");
		this.containerCustomizer = containerCustomizer;
	}

	public void setErrorHandlerCustomizer(Consumer<ErrorHandler> errorHandlerCustomizer) {
		this.errorHandlerCustomizer = errorHandlerCustomizer;
	}

	private ErrorHandler createErrorHandler(DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {
		SeekToCurrentErrorHandler errorHandler = new SeekToCurrentErrorHandler(deadLetterPublishingRecoverer,
				new FixedBackOff(0, 0));
		errorHandler.setCommitRecovered(true);
		this.errorHandlerCustomizer.accept(errorHandler);
		return errorHandler;
	}

	private void setupBackoffAwareMessageListenerAdapter(ConcurrentMessageListenerContainer<?, ?> container) {
		AcknowledgingConsumerAwareMessageListener<?, ?> listener = checkAndCast(container.getContainerProperties()
				.getMessageListener(), AcknowledgingConsumerAwareMessageListener.class);
		Long idlePartitionEventInterval = container.getContainerProperties().getIdlePartitionEventInterval();
		if (idlePartitionEventInterval == null) {
			container.getContainerProperties().setIdlePartitionEventInterval(DEFAULT_IDLE_PARTITION_EVENT_INTERVAL);
		}
		container.setupMessageListener(new KafkaBackoffAwareMessageListenerAdapter<>(listener,
				this.kafkaConsumerBackoffManager, container.getListenerId()));
		this.containerCustomizer.accept(container);
	}

	@SuppressWarnings("unchecked")
	private <T> T checkAndCast(Object obj, Class<T> clazz) {
		Assert.isAssignable(clazz, obj.getClass(),
				() -> String.format("The provided class %s is not assignable from %s",
						obj.getClass().getSimpleName(), clazz.getSimpleName()));
		return (T) obj;
	}
}
