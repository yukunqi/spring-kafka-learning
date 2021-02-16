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

import java.util.Objects;
import java.util.function.BiPredicate;

import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;

/**
 *
 * Representation a Destination Topic to which messages can be forwarded, such as retry topics and dlt.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public class DestinationTopic {

	private final String destinationName;

	private final Properties properties;

	public DestinationTopic(String destinationName, Properties properties) {
		this.destinationName = destinationName;
		this.properties = properties;
	}

	public DestinationTopic(String destinationName, DestinationTopic sourceDestinationtopic, String suffix, Type type) {
		this.destinationName = destinationName;
		this.properties = new Properties(sourceDestinationtopic.properties, suffix, type);
	}

	public Long getDestinationDelay() {
		return this.properties.delayMs;
	}

	public Integer getDestinationPartitions() {
		return this.properties.numPartitions;
	}

	public boolean isAlwaysRetryOnDltFailure() {
		return RetryTopicConfiguration.DltProcessingFailureStrategy.ALWAYS_RETRY
				.equals(this.properties.dltProcessingFailureStrategy);
	}

	public boolean isDltTopic() {
		return Type.DLT.equals(this.properties.type);
	}

	public boolean isNoOpsTopic() {
		return Type.NO_OPS.equals(this.properties.type);
	}

	public boolean isSingleTopicRetry() {
		return Type.SINGLE_TOPIC_RETRY.equals(this.properties.type);
	}

	public boolean isMainTopic() {
		return Type.MAIN.equals(this.properties.type);
	}

	public String getDestinationName() {
		return this.destinationName;
	}

	public KafkaOperations<?, ?> getKafkaOperations() {
		return this.properties.kafkaOperations;
	}

	public boolean shouldRetryOn(Integer attempt, Exception e) {
		return this.properties.shouldRetryOn.test(attempt, e);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		DestinationTopic that = (DestinationTopic) o;
		return this.destinationName.equals(that.destinationName) && this.properties.equals(that.properties);
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.destinationName, this.properties);
	}

	public static class Properties {
		private final long delayMs;
		private final String suffix;
		private final Type type;
		private final int maxAttempts;
		private final int numPartitions;
		private final RetryTopicConfiguration.DltProcessingFailureStrategy dltProcessingFailureStrategy;
		private final KafkaOperations<?, ?> kafkaOperations;
		private final BiPredicate<Integer, Exception> shouldRetryOn;

		public Properties(long delayMs, String suffix, Type type,
						int maxAttempts, int numPartitions,
						RetryTopicConfiguration.DltProcessingFailureStrategy dltProcessingFailureStrategy,
						KafkaOperations<?, ?> kafkaOperations,
						BiPredicate<Integer, Exception> shouldRetryOn) {
			this.delayMs = delayMs;
			this.suffix = suffix;
			this.type = type;
			this.maxAttempts = maxAttempts;
			this.numPartitions = numPartitions;
			this.dltProcessingFailureStrategy = dltProcessingFailureStrategy;
			this.kafkaOperations = kafkaOperations;
			this.shouldRetryOn = shouldRetryOn;

		}

		public Properties(Properties sourceProperties, String suffix, Type type) {
			this.delayMs = sourceProperties.delayMs;
			this.suffix = suffix;
			this.type = type;
			this.maxAttempts = sourceProperties.maxAttempts;
			this.numPartitions = sourceProperties.numPartitions;
			this.dltProcessingFailureStrategy = sourceProperties.dltProcessingFailureStrategy;
			this.kafkaOperations = sourceProperties.kafkaOperations;
			this.shouldRetryOn = sourceProperties.shouldRetryOn;
		}

		public boolean isDltTopic() {
			return Type.DLT.equals(this.type);
		}

		public String suffix() {
			return this.suffix;
		}

		public long delay() {
			return this.delayMs;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Properties that = (Properties) o;
			return this.delayMs == that.delayMs
					&& this.maxAttempts == that.maxAttempts
					&& this.numPartitions == that.numPartitions
					&& this.suffix.equals(that.suffix)
					&& this.type == that.type
					&& this.dltProcessingFailureStrategy == that.dltProcessingFailureStrategy
					&& this.kafkaOperations.equals(that.kafkaOperations);
		}

		@Override
		public int hashCode() {
			return Objects.hash(this.delayMs, this.suffix, this.type, this.maxAttempts, this.numPartitions,
					this.dltProcessingFailureStrategy, this.kafkaOperations);
		}
	}

	enum Type {
		MAIN, RETRY, SINGLE_TOPIC_RETRY, DLT, NO_OPS
	}
}
