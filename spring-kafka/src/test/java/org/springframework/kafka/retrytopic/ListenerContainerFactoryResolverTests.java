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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;

/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
class ListenerContainerFactoryResolverTests {

	@Mock
	private BeanFactory beanFactory;

	@Mock
	private ConcurrentKafkaListenerContainerFactory<?, ?> factoryFromKafkaListenerAnnotation;

	@Mock
	private ConcurrentKafkaListenerContainerFactory<?, ?> factoryFromRetryTopicConfiguration;

	@Mock
	private ConcurrentKafkaListenerContainerFactory<?, ?> factoryFromBeanName;

	@Mock
	private ConcurrentKafkaListenerContainerFactory<?, ?> factoryFromOtherBeanName;

	@Mock
	private ConcurrentKafkaListenerContainerFactory<?, ?> factoryFromDefaultBeanName;

	private final static String factoryName = "testListenerContainerFactory";
	private final static String otherFactoryName = "otherTestListenerContainerFactory";

	@Test
	void shouldResolveWithKLAFactoryForMainEndpoint() {

		// setup
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver.Configuration(factoryFromRetryTopicConfiguration, factoryName);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory = listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(factoryFromKafkaListenerAnnotation, configuration);

		// then
		assertEquals(factoryFromKafkaListenerAnnotation, resolvedFactory);
	}

	@Test
	void shouldResolveWithRTConfigurationFactoryForMainEndpointIfKLAAbsent() {

		// setup
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver.Configuration(factoryFromRetryTopicConfiguration, factoryName);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory = listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null, configuration);

		// then
		assertEquals(factoryFromRetryTopicConfiguration, resolvedFactory);
	}

	@Test
	void shouldResolveFromBeanNameForMainEndpoint() {

		// setup
		given(beanFactory.getBean(factoryName, ConcurrentKafkaListenerContainerFactory.class)).willReturn(factoryFromBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, factoryName);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null, configuration);

		// then
		assertEquals(factoryFromBeanName, resolvedFactory);
	}

	@Test
	void shouldResolveFromDefaultBeanNameForMainEndpoint() {

		// setup
		given(beanFactory.getBean(RetryTopicInternalBeanNames.DEFAULT_LISTENER_FACTORY_BEAN_NAME,
				ConcurrentKafkaListenerContainerFactory.class)).willReturn(factoryFromBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, null);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null, configuration);

		// then
		assertEquals(factoryFromBeanName, resolvedFactory);
	}

	@Test
	void shouldFailIfNoneResolvedForMainEndpoint() {

		// setup
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);

		// given
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, null);

		// then
		assertThrows(IllegalArgumentException.class,
				() -> listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null, configuration));
	}

	@Test
	void shouldResolveWithRetryTopicConfigurationFactoryForRetryEndpoint() {

		// setup
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver.Configuration(factoryFromRetryTopicConfiguration, factoryName);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory = listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(factoryFromKafkaListenerAnnotation, configuration);

		// then
		assertEquals(factoryFromRetryTopicConfiguration, resolvedFactory);
	}

	@Test
	void shouldResolveFromBeanNameForRetryEndpoint() {

		// setup
		given(beanFactory.getBean(factoryName, ConcurrentKafkaListenerContainerFactory.class)).willReturn(factoryFromBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver.Configuration(null, factoryName);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory = listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(factoryFromKafkaListenerAnnotation, configuration);

		// then
		assertEquals(factoryFromBeanName, resolvedFactory);
	}

	@Test
	void shouldResolveWithKLAFactoryForRetryEndpoint() {

		// setup
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, null);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(factoryFromKafkaListenerAnnotation, configuration);

		// then
		assertEquals(factoryFromKafkaListenerAnnotation, resolvedFactory);
	}

	@Test
	void shouldResolveFromDefaultBeanNameForRetryEndpoint() {

		// setup
		given(beanFactory.getBean(RetryTopicInternalBeanNames.DEFAULT_LISTENER_FACTORY_BEAN_NAME,
				ConcurrentKafkaListenerContainerFactory.class)).willReturn(factoryFromBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, null);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(null, configuration);

		// then
		assertEquals(factoryFromBeanName, resolvedFactory);
	}

	@Test
	void shouldFailIfNoneResolvedForRetryEndpoint() {

		// setup
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);

		// given
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, null);

		// then
		assertThrows(IllegalArgumentException.class,
				() -> listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(null, configuration));
	}

	@Test
	void shouldGetFromCacheForMainEndpont() {

		// setup
		given(beanFactory.getBean(factoryName,
				ConcurrentKafkaListenerContainerFactory.class)).willReturn(factoryFromBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);

		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, factoryName);

		ListenerContainerFactoryResolver.Configuration configuration2 =
				new ListenerContainerFactoryResolver.Configuration(null, factoryName);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null, configuration);

		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory2 =
				listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null, configuration2);

		// then
		assertEquals(factoryFromBeanName, resolvedFactory);
		assertEquals(factoryFromBeanName, resolvedFactory2);
		then(beanFactory).should(times(1)).getBean(factoryName,
				ConcurrentKafkaListenerContainerFactory.class);
	}

	@Test
	void shouldGetFromCacheForRetryEndpont() {

		// setup
		given(beanFactory.getBean(factoryName,
				ConcurrentKafkaListenerContainerFactory.class)).willReturn(factoryFromBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);

		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, factoryName);

		ListenerContainerFactoryResolver.Configuration configuration2 =
				new ListenerContainerFactoryResolver.Configuration(null, factoryName);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(null, configuration);

		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory2 =
				listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(null, configuration2);

		// then
		assertEquals(factoryFromBeanName, resolvedFactory);
		assertEquals(factoryFromBeanName, resolvedFactory2);
		then(beanFactory).should(times(1)).getBean(factoryName,
				ConcurrentKafkaListenerContainerFactory.class);
	}

	@Test
	void shouldNotGetFromCacheForMainEndpont() {

		// setup
		given(beanFactory.getBean(factoryName,
				ConcurrentKafkaListenerContainerFactory.class)).willReturn(factoryFromBeanName);
		given(beanFactory.getBean(otherFactoryName,
				ConcurrentKafkaListenerContainerFactory.class)).willReturn(factoryFromOtherBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);

		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, factoryName);

		ListenerContainerFactoryResolver.Configuration configuration2 =
				new ListenerContainerFactoryResolver.Configuration(null, otherFactoryName);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null, configuration);

		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory2 =
				listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null, configuration2);

		// then
		assertEquals(factoryFromBeanName, resolvedFactory);
		assertEquals(factoryFromOtherBeanName, resolvedFactory2);
		then(beanFactory).should(times(1)).getBean(factoryName,
				ConcurrentKafkaListenerContainerFactory.class);
		then(beanFactory).should(times(1)).getBean(otherFactoryName,
				ConcurrentKafkaListenerContainerFactory.class);
	}

	@Test
	void shouldGetFromCacheWithSameConfiguration() {

		// setup
		ListenerContainerFactoryResolver.Cache cache = new ListenerContainerFactoryResolver.Cache();
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver.Configuration(factoryFromRetryTopicConfiguration, factoryName);

		// given
		cache.addIfAbsent(factoryFromKafkaListenerAnnotation, configuration, factoryFromDefaultBeanName);

		// then
		assertEquals(cache.fromCache(factoryFromKafkaListenerAnnotation, configuration), factoryFromDefaultBeanName);
	}

	@Test
	void shouldGetFromCacheWithEqualConfiguration() {

		// setup
		ListenerContainerFactoryResolver.Cache cache = new ListenerContainerFactoryResolver.Cache();
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver.Configuration(factoryFromRetryTopicConfiguration, factoryName);
		ListenerContainerFactoryResolver.Configuration configuration2 = new ListenerContainerFactoryResolver.Configuration(factoryFromRetryTopicConfiguration, factoryName);

		// given
		cache.addIfAbsent(factoryFromKafkaListenerAnnotation, configuration, factoryFromDefaultBeanName);

		// then
		assertEquals(cache.fromCache(factoryFromKafkaListenerAnnotation, configuration2), factoryFromDefaultBeanName);
	}

	@Test
	void shouldNotGetFromCacheWithDifferentConfiguration() {

		// setup
		ListenerContainerFactoryResolver.Cache cache = new ListenerContainerFactoryResolver.Cache();
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver.Configuration(factoryFromRetryTopicConfiguration, factoryName);
		ListenerContainerFactoryResolver.Configuration configuration2 = new ListenerContainerFactoryResolver.Configuration(factoryFromOtherBeanName, factoryName);

		// given
		cache.addIfAbsent(factoryFromKafkaListenerAnnotation, configuration, factoryFromDefaultBeanName);

		// then
		assertNull(cache.fromCache(factoryFromKafkaListenerAnnotation, configuration2));
	}
}
