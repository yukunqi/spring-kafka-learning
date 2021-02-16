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

package org.springframework.kafka.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurer;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopicPropertiesFactory;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;

/**
 *
 * Annotation to create the retry and dlt topics for a {@link KafkaListener} annotated listener.
 * See {@link RetryTopicConfigurer for usage examples.}
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 * @see RetryTopicConfigurer
 */
@Target({ ElementType.METHOD }) //, ElementType.TYPE }) TODO: Enable class level annotation
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RetryableTopic {

	/**
	 * The number of attempts made before the message is sent to the DLT.
	 *
	 * @return the number of attempts.
	 */
	int attempts() default MaxAttemptsRetryPolicy.DEFAULT_MAX_ATTEMPTS;

	/**
	 * Specify the backoff properties for retrying this operation. The default is a simple
	 * {@link Backoff} specification with no properties - see it's documentation for
	 * defaults.
	 * @return a backoff specification
	 */
	Backoff backoff() default @Backoff;

	/**
	 *
	 * The bean name of the {@link org.springframework.kafka.core.KafkaTemplate} bean
	 * that will be used to forward the message to the retry and Dlt topics. If not specified,
	 * a bean with name retryTopicDefaultKafkaTemplate will be looked up.
	 *
	 * @return the kafkaTemplate bean name.
	 */
	String kafkaTemplate() default "";

	/**
	 *  The bean name of the {@link org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory} that will be used to create
	 *  the consumers for the retry and dlt topics. If none is provided, the one from the {@link KafkaListener} annotation
	 *  is used, or else a default one, if any.
	 *
	 * @return the listenerContainerFactory bean name.
	 */
	String listenerContainerFactory() default "";

	/**
	 * Whether or not the topic should be created after registration with the provided configurations.
	 * Not to be confused with the ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG from Kafka
	 * configuration, which is handled by the {@link org.apache.kafka.clients.consumer.KafkaConsumer}.
	 *
	 * @return the configuration.
	 */
	boolean autoCreateTopics() default true;

	/**
	 * The number of partitions for the automatically created topics.
	 *
	 * @return the number of partitions.
	 */
	int numPartitions() default 1;

	/**
	 * The replication factor for the automatically created topics.
	 *
	 * @return the replication factor.
	 */
	short replicationFactor() default 1;

	/**
	 * The exceptions that should be retried.
	 *
	 * @return the exceptions.
	 */
	Class<? extends Throwable>[] include() default {};

	/**
	 * The exceptions that should not be retried.
	 * When the message processing throws these exceptions
	 * the message goes straight to the DLT.
	 *
	 * @return the exceptions not to be retried.
	 */
	Class<? extends Throwable>[] exclude() default {};

	/**
	 * Whether or not the captured exception should be
	 * traversed to look for the exceptions provided above.
	 *
	 * @return the value.
	 */
	boolean traversingCauses() default false;

	/**
	 * The suffix that will be appended to the main topic in order to generate
	 * the retry topics. The corresponding delay value is also appended.
	 *
	 * @return the retry topics' suffix.
	 */
	String retryTopicSuffix() default DestinationTopicPropertiesFactory.DestinationTopicSuffixes.DEFAULT_RETRY_SUFFIX;

	/**
	 * The suffix that will be appended to the main topic in order to generate
	 * the dlt topic.
	 *
	 * @return the dlt suffix.
	 */
	String dltTopicSuffix() default DestinationTopicPropertiesFactory.DestinationTopicSuffixes.DEFAULT_DLT_SUFFIX;

	RetryTopicConfiguration.DltProcessingFailureStrategy dltProcessingFailureStrategy()
			default RetryTopicConfiguration.DltProcessingFailureStrategy.ALWAYS_RETRY;

	RetryTopicConfiguration.FixedDelayTopicStrategy fixedDelayTopicStrategy()
			default RetryTopicConfiguration.FixedDelayTopicStrategy.MULTIPLE_TOPICS;
}
