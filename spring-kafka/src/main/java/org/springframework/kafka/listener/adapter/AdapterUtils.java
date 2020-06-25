/*
 * Copyright 2020 the original author or authors.
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

import org.springframework.expression.ParserContext;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.kafka.support.KafkaHeaders;

/**
 * Utilities for listener adapters.
 *
 * @author Gary Russell
 * @since 2.3.13
 *
 */
public final class AdapterUtils {

	/**
	 * Parser context for runtime SpEL using ! as the template prefix.
	 * @since 2.3.13
	 */
	public static final ParserContext PARSER_CONTEXT = new TemplateParserContext("!{", "}");

	private AdapterUtils() {
	}

	/**
	 * Return the default expression when no SendTo value is present.
	 * @return the expression.
	 * @since 2.3.13
	 */
	public static String getDefaultReplyTopicExpression() {
		return PARSER_CONTEXT.getExpressionPrefix() + "source.headers['"
				+ KafkaHeaders.REPLY_TOPIC + "']" + PARSER_CONTEXT.getExpressionSuffix();
	}

}
