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

import java.util.Arrays;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.retrytopic.BackOffValuesGenerator;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.util.StringUtils;

/**
 *
 * Creates a list of {@link DestinationTopic.Properties} based on the
 * provided configurations.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public class DestinationTopicPropertiesFactory {

	private final static String MAIN_TOPIC_SUFFIX = "";

	private final DestinationTopicSuffixes destinationTopicSuffixes;

	private final List<Long> backOffValues;

	private final BinaryExceptionClassifier exceptionClassifier;

	private final int numPartitions;

	private final int maxAttempts;

	private final KafkaOperations<?, ?> kafkaOperations;

	private final RetryTopicConfiguration.FixedDelayTopicStrategy fixedDelayTopicStrategy;

	private final RetryTopicConfiguration.DltProcessingFailureStrategy dltProcessingFailureStrategy;

	public DestinationTopicPropertiesFactory(String retryTopicSuffix, String dltSuffix, int maxAttempts,
											BackOffPolicy backOffPolicy, BinaryExceptionClassifier exceptionClassifier,
											int numPartitions, KafkaOperations<?, ?> kafkaOperations,
											RetryTopicConfiguration.FixedDelayTopicStrategy fixedDelayTopicStrategy,
											RetryTopicConfiguration.DltProcessingFailureStrategy dltProcessingFailureStrategy) {
		this.dltProcessingFailureStrategy = dltProcessingFailureStrategy;
		this.kafkaOperations = kafkaOperations;
		this.exceptionClassifier = exceptionClassifier;
		this.numPartitions = numPartitions;
		this.fixedDelayTopicStrategy = fixedDelayTopicStrategy;
		this.destinationTopicSuffixes = new DestinationTopicSuffixes(retryTopicSuffix, dltSuffix);
		this.backOffValues = new BackOffValuesGenerator(maxAttempts, backOffPolicy).generateValues();
		// Max Attempts include the initial try.
		this.maxAttempts = this.backOffValues.size() + 1;
	}

	public List<DestinationTopic.Properties> createProperties() {
		return isSingleTopicFixedDelay()
				? createPropertiesForFixedDelaySingleTopic()
				: createPropertiesForDefaultTopicStrategy();
	}

	private List<DestinationTopic.Properties> createPropertiesForFixedDelaySingleTopic() {
		return Arrays.asList(createMainTopicProperties(), createRetryDestinationTopic(1,
				DestinationTopic.Type.SINGLE_TOPIC_RETRY, getShouldRetryOn()), createDltProperties());
	}

	private boolean isSingleTopicFixedDelay() {
		return isFixedDelay() && isSingleTopicStrategy();
	}

	private boolean isSingleTopicStrategy() {
		return RetryTopicConfiguration.FixedDelayTopicStrategy.SINGLE_TOPIC.equals(this.fixedDelayTopicStrategy);
	}

	private List<DestinationTopic.Properties> createPropertiesForDefaultTopicStrategy() {
		return IntStream.rangeClosed(0, this.maxAttempts)
				.mapToObj(index -> createRetryOrDltTopicSuffixes(index))
				.collect(Collectors.toList());
	}

	private DestinationTopic.Properties createMainTopicProperties() {
		return new DestinationTopic.Properties(0, MAIN_TOPIC_SUFFIX, DestinationTopic.Type.MAIN, this.maxAttempts,
				this.numPartitions, this.dltProcessingFailureStrategy, this.kafkaOperations, getShouldRetryOn());
	}

	private DestinationTopic.Properties createRetryOrDltTopicSuffixes(int index) {
		BiPredicate<Integer, Exception> shouldRetryOn = getShouldRetryOn();
		return index == 0
				? createMainTopicProperties()
				: index < this.maxAttempts
					? createRetryDestinationTopic(index,
				DestinationTopic.Type.RETRY, shouldRetryOn)
					: createDltProperties();
	}

	private DestinationTopic.Properties createDltProperties() {
		return new DestinationTopic.Properties(0, this.destinationTopicSuffixes.getDltSuffix(),
		DestinationTopic.Type.DLT, this.maxAttempts, this.numPartitions, this.dltProcessingFailureStrategy,
				this.kafkaOperations, (a, e) -> false);
	}

	private BiPredicate<Integer, Exception> getShouldRetryOn() {
		return (attempt, exception) -> attempt < this.maxAttempts && this.exceptionClassifier.classify(exception);
	}

	private DestinationTopic.Properties createRetryDestinationTopic(int index,
																	DestinationTopic.Type topicType,
																	BiPredicate<Integer, Exception> shouldRetryOn) {
		int indexInBackoffValues = index - 1;
		String retrySuffix = this.destinationTopicSuffixes.getRetrySuffix();
		return hasDuplicates()
				? getProperties(topicType, shouldRetryOn, indexInBackoffValues,
						isSingleTopicStrategy() ? retrySuffix : joinWithRetrySuffix(indexInBackoffValues, retrySuffix))
				: getProperties(topicType, shouldRetryOn, indexInBackoffValues,
						joinWithRetrySuffix(this.backOffValues.get(indexInBackoffValues), retrySuffix));
	}

	private boolean hasDuplicates() {
		return this.backOffValues
				.stream()
				.distinct()
				.count() != this.backOffValues.size();
	}

	private DestinationTopic.Properties getProperties(DestinationTopic.Type topicType,
													BiPredicate<Integer, Exception> shouldRetryOn,
													int indexInBackoffValues,
													String suffix) {
		return new DestinationTopic.Properties(this.backOffValues.get(indexInBackoffValues), suffix,
				topicType, this.maxAttempts, this.numPartitions, this.dltProcessingFailureStrategy,
				this.kafkaOperations, shouldRetryOn);
	}

	private boolean isFixedDelay() {
		// If all values are the same, such as in NoBackOffPolicy and FixedBackoffPolicy
		return this.backOffValues.size() > 1 && this.backOffValues.stream().distinct().count() == 1;
	}

	private String joinWithRetrySuffix(long parameter, String retrySuffix) {
		return String.join("-", retrySuffix, String.valueOf(parameter));
	}

	public static class DestinationTopicSuffixes {
		/**
		 * Default suffix for retry topics.
		 */
		public static final String DEFAULT_RETRY_SUFFIX = "-retry";

		/**
		 * Default suffix for dlt.
		 */
		public static final String DEFAULT_DLT_SUFFIX = "-dlt";
		private final String retryTopicSuffix;
		private final String dltSuffix;

		DestinationTopicSuffixes(String retryTopicSuffix, String dltSuffix) {
			this.retryTopicSuffix = StringUtils.hasText(retryTopicSuffix) ? retryTopicSuffix : DEFAULT_RETRY_SUFFIX;
			this.dltSuffix = StringUtils.hasText(dltSuffix) ? dltSuffix : DEFAULT_DLT_SUFFIX;
		}

		public String getRetrySuffix() {
			return this.retryTopicSuffix;
		}

		public String getDltSuffix() {
			return this.dltSuffix;
		}
	}
}
