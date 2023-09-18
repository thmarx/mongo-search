package com.github.thmarx.mongo.search.index.configuration;

import lombok.Getter;
import lombok.experimental.SuperBuilder;

import com.github.thmarx.mongo.search.mapper.FieldMapper;

import java.util.function.Supplier;
import lombok.Builder;

/*-
 * #%L
 * mongo-search-index
 * %%
 * Copyright (C) 2023 Marx-Software
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

/**
 * Class for field configurations.
 * An index implementation can have a custom subclass.
 *
 * @author t.marx
 */
@SuperBuilder
@Getter
public class FieldConfiguration {
	/**
	 * The name of the field in the mongodb document.
	 */
	private final String fieldName;
	/**
	 * The name of the field in the index.
	 */
	private final String indexFieldName;
	/**
	 * The mapper for the field value.
	 */
	private final FieldMapper<?> mapper;
	
	/**
	 * Supplier for default value if the mapper returns null of an empty list.
	 */
	@Builder.Default
	private Supplier<?> defaultValue = () -> null;
}
