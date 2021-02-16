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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.retrytopic.RetryTopicHeaders;

/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
class DestinationTopicContainerTest extends DestinationTopicTest {

	private Map<String, DestinationTopicResolver.DestinationsHolder> destinationTopicMap;

	private final Clock clock = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault());

	private DestinationTopicContainer destinationTopicContainer = new DestinationTopicContainer(clock);

	@BeforeEach
	public void setup() {
		destinationTopicMap = new HashMap<>();
		DestinationTopicResolver.DestinationsHolder mainDestinationHolder =
				DestinationTopicResolver.holderFor(mainDestinationTopic, firstRetryDestinationTopic);
		DestinationTopicResolver.DestinationsHolder firstRetryDestinationHolder =
				DestinationTopicResolver.holderFor(firstRetryDestinationTopic, secondRetryDestinationTopic);
		DestinationTopicResolver.DestinationsHolder secondRetryDestinationHolder =
				DestinationTopicResolver.holderFor(secondRetryDestinationTopic, dltDestinationTopic);
		DestinationTopicResolver.DestinationsHolder dltDestinationHolder =
				DestinationTopicResolver.holderFor(dltDestinationTopic, noOpsDestinationTopic);

		DestinationTopicResolver.DestinationsHolder mainDestinationHolder2 =
				DestinationTopicResolver.holderFor(mainDestinationTopic2, firstRetryDestinationTopic2);
		DestinationTopicResolver.DestinationsHolder firstRetryDestinationHolder2 =
				DestinationTopicResolver.holderFor(firstRetryDestinationTopic2, secondRetryDestinationTopic2);
		DestinationTopicResolver.DestinationsHolder secondRetryDestinationHolder2 =
				DestinationTopicResolver.holderFor(secondRetryDestinationTopic2, dltDestinationTopic2);
		DestinationTopicResolver.DestinationsHolder dltDestinationHolder2 =
				DestinationTopicResolver.holderFor(dltDestinationTopic2, noOpsDestinationTopic2);

		destinationTopicMap.put(mainDestinationTopic.getDestinationName(), mainDestinationHolder);
		destinationTopicMap.put(firstRetryDestinationTopic.getDestinationName(), firstRetryDestinationHolder);
		destinationTopicMap.put(secondRetryDestinationTopic.getDestinationName(), secondRetryDestinationHolder);
		destinationTopicMap.put(dltDestinationTopic.getDestinationName(), dltDestinationHolder);
		destinationTopicMap.put(mainDestinationTopic2.getDestinationName(), mainDestinationHolder2);
		destinationTopicMap.put(firstRetryDestinationTopic2.getDestinationName(), firstRetryDestinationHolder2);
		destinationTopicMap.put(secondRetryDestinationTopic2.getDestinationName(), secondRetryDestinationHolder2);
		destinationTopicMap.put(dltDestinationTopic2.getDestinationName(), dltDestinationHolder2);
		destinationTopicContainer.addDestinations(destinationTopicMap);
	}

	@Test
	void shouldResolveRetryDestination() {
		assertEquals(firstRetryDestinationTopic, destinationTopicContainer
				.resolveNextDestination(mainDestinationTopic.getDestinationName(), 1, new IllegalArgumentException()));
		assertEquals(secondRetryDestinationTopic, destinationTopicContainer
				.resolveNextDestination(firstRetryDestinationTopic.getDestinationName(), 1, new IllegalArgumentException()));
		assertEquals(dltDestinationTopic, destinationTopicContainer
				.resolveNextDestination(secondRetryDestinationTopic.getDestinationName(), 1, new IllegalArgumentException()));
		assertEquals(noOpsDestinationTopic, destinationTopicContainer
				.resolveNextDestination(dltDestinationTopic.getDestinationName(), 1, new IllegalArgumentException()));
		assertEquals(firstRetryDestinationTopic2, destinationTopicContainer
				.resolveNextDestination(mainDestinationTopic2.getDestinationName(), 1, new IllegalArgumentException()));
		assertEquals(secondRetryDestinationTopic2, destinationTopicContainer
				.resolveNextDestination(firstRetryDestinationTopic2.getDestinationName(), 1, new IllegalArgumentException()));
		assertEquals(dltDestinationTopic2, destinationTopicContainer
				.resolveNextDestination(secondRetryDestinationTopic2.getDestinationName(), 1, new IllegalArgumentException()));
		assertEquals(dltDestinationTopic2, destinationTopicContainer
				.resolveNextDestination(dltDestinationTopic2.getDestinationName(), 1, new IllegalArgumentException()));
	}

	@Test
	void shouldResolveDltDestinationForNonRetryableException() {
		assertEquals(dltDestinationTopic, destinationTopicContainer
				.resolveNextDestination(mainDestinationTopic.getDestinationName(), 1, new RuntimeException()));
	}

	@Test
	void shouldResolveNoOpsDestinationForDoNotRetryDltPolicy() {
		assertEquals(noOpsDestinationTopic, destinationTopicContainer
				.resolveNextDestination(dltDestinationTopic.getDestinationName(), 1, new IllegalArgumentException()));
	}

	@Test
	void shouldResolveDltDestinationForAlwaysRetryDltPolicy() {
		assertEquals(dltDestinationTopic2, destinationTopicContainer
				.resolveNextDestination(dltDestinationTopic2.getDestinationName(), 1, new IllegalArgumentException()));
	}

	@Test
	void shouldThrowIfNoDestinationFound() {
		assertThrows(NullPointerException.class,
				() -> destinationTopicContainer.resolveNextDestination("Non-existing-topic", 0, new IllegalArgumentException()));
	}

	@Test
	void shouldResolveDestinationNextExecutionTime() {
		RuntimeException e = new IllegalArgumentException();
		assertEquals(destinationTopicContainer.resolveDestinationNextExecutionTime(
				mainDestinationTopic.getDestinationName(), 0, e),
				getExpectedNextExecutionTime(firstRetryDestinationTopic));
		assertEquals(destinationTopicContainer.resolveDestinationNextExecutionTime(
				firstRetryDestinationTopic.getDestinationName(), 0, e),
				getExpectedNextExecutionTime(secondRetryDestinationTopic));
		assertEquals(destinationTopicContainer.resolveDestinationNextExecutionTime(
				secondRetryDestinationTopic.getDestinationName(), 0, e),
				getExpectedNextExecutionTime(dltDestinationTopic));
		assertEquals(destinationTopicContainer.resolveDestinationNextExecutionTime(
				dltDestinationTopic.getDestinationName(), 0, e),
				getExpectedNextExecutionTime(noOpsDestinationTopic));
	}

	private String getExpectedNextExecutionTime(DestinationTopic destinationTopic) {
		String expectedNextExecutionTime = LocalDateTime.now(clock).plus(destinationTopic.getDestinationDelay(),
				ChronoUnit.MILLIS).format(RetryTopicHeaders.DEFAULT_BACKOFF_TIMESTAMP_HEADER_FORMATTER);
		return expectedNextExecutionTime;
	}

	@Test
	void shouldThrowIfAddsDestinationsAfterClosed() {
		destinationTopicContainer.onApplicationEvent(null);
		assertThrows(IllegalStateException.class, () ->
				destinationTopicContainer.addDestinations(Collections.emptyMap()));
	}

	@Test
	void shouldGetKafkaTemplateForTopic() {
		assertEquals(kafkaOperations1, destinationTopicContainer.getKafkaOperationsFor(firstRetryDestinationTopic.getDestinationName()));
		assertEquals(kafkaOperations2, destinationTopicContainer.getKafkaOperationsFor(firstRetryDestinationTopic2.getDestinationName()));
	}
}
