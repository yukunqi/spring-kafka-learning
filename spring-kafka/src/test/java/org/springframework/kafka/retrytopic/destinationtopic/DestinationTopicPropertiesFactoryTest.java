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

package org.springframework.kafka.retrytopic.destinationtopic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.classify.BinaryExceptionClassifierBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;


/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
class DestinationTopicPropertiesFactoryTest {

	private final String retryTopicSuffix = "test-retry-suffix";

	private final String dltSuffix = "test-dlt-suffix";

	private final int maxAttempts = 4;

	private final int numPartitions = 0;

	private final RetryTopicConfiguration.FixedDelayTopicStrategy fixedDelayTopicStrategy =
			RetryTopicConfiguration.FixedDelayTopicStrategy.SINGLE_TOPIC;

	private final RetryTopicConfiguration.DltProcessingFailureStrategy dltProcessingFailureStrategy =
			RetryTopicConfiguration.DltProcessingFailureStrategy.FAIL;

	private final BackOffPolicy backOffPolicy = new FixedBackOffPolicy();

	private final BinaryExceptionClassifier classifier = new BinaryExceptionClassifierBuilder()
			.retryOn(IllegalArgumentException.class).build();

	@Mock
	private KafkaOperations<?, ?> kafkaOperations;

	@BeforeEach
	void setup() {
		((FixedBackOffPolicy) backOffPolicy).setBackOffPeriod(1000);
	}

	@Test
	void shouldCreateMainAndDltProperties() {
		// when
		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, 1,
						backOffPolicy, classifier, numPartitions, kafkaOperations, fixedDelayTopicStrategy, dltProcessingFailureStrategy).createProperties();

		// then
		assertTrue(propertiesList.size() == 2);
		DestinationTopic.Properties mainTopicProperties = propertiesList.get(0);
		assertEquals("", mainTopicProperties.suffix());
		assertFalse(mainTopicProperties.isDltTopic());
		DestinationTopic mainTopic = new DestinationTopic("mainTopic", mainTopicProperties);
		assertEquals(0L, mainTopic.getDestinationDelay());
		assertTrue(mainTopic.shouldRetryOn(0, new IllegalArgumentException()));
		assertFalse(mainTopic.shouldRetryOn(maxAttempts, new IllegalArgumentException()));
		assertFalse(mainTopic.shouldRetryOn(0, new RuntimeException()));

		DestinationTopic.Properties dltProperties = propertiesList.get(1);
		assertDltTopic(dltProperties);
	}

	private void assertDltTopic(DestinationTopic.Properties dltProperties) {
		assertEquals(dltSuffix, dltProperties.suffix());
		assertTrue(dltProperties.isDltTopic());
		DestinationTopic dltTopic = new DestinationTopic("mainTopic", dltProperties);
		assertEquals(0, dltTopic.getDestinationDelay());
		assertFalse(dltTopic.shouldRetryOn(0, new IllegalArgumentException()));
		assertFalse(dltTopic.shouldRetryOn(maxAttempts, new IllegalArgumentException()));
		assertFalse(dltTopic.shouldRetryOn(0, new RuntimeException()));
	}

	@Test
	void shouldCreateTwoRetryPropertiesForMultipleBackoffValues() {
		// when
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(1000);
		backOffPolicy.setMultiplier(2);
		int maxAttempts = 3;

		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, maxAttempts,
						backOffPolicy, classifier, numPartitions, kafkaOperations, fixedDelayTopicStrategy,
						dltProcessingFailureStrategy).createProperties();

		List<DestinationTopic> destinationTopicList = propertiesList
				.stream()
				.map(properties -> new DestinationTopic("mainTopic" + properties.suffix(), properties))
				.collect(Collectors.toList());

		// then
		assertTrue(propertiesList.size() == 4);
		DestinationTopic.Properties firstRetryProperties = propertiesList.get(1);
		assertEquals(retryTopicSuffix + "-1000", firstRetryProperties.suffix());
		assertFalse(firstRetryProperties.isDltTopic());
		DestinationTopic firstRetryDestinationTopic = destinationTopicList.get(1);
		assertEquals(1000, firstRetryDestinationTopic.getDestinationDelay());
		assertEquals(numPartitions, firstRetryDestinationTopic.getDestinationPartitions());
		assertTrue(firstRetryDestinationTopic.shouldRetryOn(0, new IllegalArgumentException()));
		assertFalse(firstRetryDestinationTopic.shouldRetryOn(maxAttempts, new IllegalArgumentException()));
		assertFalse(firstRetryDestinationTopic.shouldRetryOn(0, new RuntimeException()));

		DestinationTopic.Properties secondRetryProperties = propertiesList.get(2);
		assertEquals(retryTopicSuffix + "-2000", secondRetryProperties.suffix());
		assertFalse(secondRetryProperties.isDltTopic());
		DestinationTopic secondRetryDestinationTopic = destinationTopicList.get(2);
		assertEquals(2000, secondRetryDestinationTopic.getDestinationDelay());
		assertEquals(numPartitions, secondRetryDestinationTopic.getDestinationPartitions());
		assertTrue(secondRetryDestinationTopic.shouldRetryOn(0, new IllegalArgumentException()));
		assertFalse(secondRetryDestinationTopic.shouldRetryOn(maxAttempts, new IllegalArgumentException()));
		assertFalse(secondRetryDestinationTopic.shouldRetryOn(0, new RuntimeException()));

		assertDltTopic(propertiesList.get(3));
	}

	@Test
	void shouldCreateOneRetryPropertyForFixedBackoffWithSingleTopicStrategy() {

		// when
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		int maxAttempts = 5;

		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, maxAttempts,
						backOffPolicy, classifier, numPartitions, kafkaOperations, RetryTopicConfiguration.FixedDelayTopicStrategy.SINGLE_TOPIC,
						dltProcessingFailureStrategy).createProperties();

		List<DestinationTopic> destinationTopicList = propertiesList
				.stream()
				.map(properties -> new DestinationTopic("mainTopic" + properties.suffix(), properties))
				.collect(Collectors.toList());

		// then
		assertTrue(propertiesList.size() == 3);

		DestinationTopic.Properties mainTopicProperties = propertiesList.get(0);
		DestinationTopic mainDestinationTopic = destinationTopicList.get(0);
		assertTrue(mainDestinationTopic.isMainTopic());

		DestinationTopic.Properties firstRetryProperties = propertiesList.get(1);
		assertEquals(retryTopicSuffix, firstRetryProperties.suffix());
		DestinationTopic retryDestinationTopic = destinationTopicList.get(1);
		assertTrue(retryDestinationTopic.isSingleTopicRetry());
		assertEquals(1000, retryDestinationTopic.getDestinationDelay());

		DestinationTopic.Properties dltProperties = propertiesList.get(2);
		assertEquals(dltSuffix, dltProperties.suffix());
		assertTrue(dltProperties.isDltTopic());
		DestinationTopic dltTopic = destinationTopicList.get(2);
		assertEquals(0, dltTopic.getDestinationDelay());
		assertEquals(numPartitions, dltTopic.getDestinationPartitions());
	}

	@Test
	void shouldCreateRetryPropertiesForFixedBackoffWithMultiTopicStrategy() {

		// when
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(5000);
		int maxAttempts = 3;

		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, maxAttempts,
						backOffPolicy, classifier, numPartitions, kafkaOperations,
						RetryTopicConfiguration.FixedDelayTopicStrategy.MULTIPLE_TOPICS,
						dltProcessingFailureStrategy).createProperties();

		List<DestinationTopic> destinationTopicList = propertiesList
				.stream()
				.map(properties -> new DestinationTopic("mainTopic" + properties.suffix(), properties))
				.collect(Collectors.toList());

		// then
		assertTrue(propertiesList.size() == 4);

		DestinationTopic.Properties mainTopicProperties = propertiesList.get(0);
		DestinationTopic mainDestinationTopic = destinationTopicList.get(0);
		assertTrue(mainDestinationTopic.isMainTopic());

		DestinationTopic.Properties firstRetryProperties = propertiesList.get(1);
		assertEquals(retryTopicSuffix + "-0", firstRetryProperties.suffix());
		DestinationTopic retryDestinationTopic = destinationTopicList.get(1);
		assertFalse(retryDestinationTopic.isSingleTopicRetry());
		assertEquals(5000, retryDestinationTopic.getDestinationDelay());

		DestinationTopic.Properties secondRetryProperties = propertiesList.get(2);
		assertEquals(retryTopicSuffix + "-1", secondRetryProperties.suffix());
		DestinationTopic secondRetryDestinationTopic = destinationTopicList.get(2);
		assertFalse(secondRetryDestinationTopic.isSingleTopicRetry());
		assertEquals(5000, secondRetryDestinationTopic.getDestinationDelay());

		DestinationTopic.Properties dltProperties = propertiesList.get(3);
		assertEquals(dltSuffix, dltProperties.suffix());
		assertTrue(dltProperties.isDltTopic());
		DestinationTopic dltTopic = destinationTopicList.get(3);
		assertEquals(0, dltTopic.getDestinationDelay());
		assertEquals(numPartitions, dltTopic.getDestinationPartitions());
	}
}
