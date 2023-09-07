/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.github.thmarx.mongo.search.adapters.elasticsearch;

/*-
 * #%L
 * monog-search-adapters-elasticsearch
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

import com.github.thmarx.mongo.search.index.configuration.FieldConfiguration;
import com.github.thmarx.mongo.search.index.configuration.IndexConfiguration;
import com.github.thmarx.mongo.search.index.utils.MultiMap;
import java.util.Collection;
import java.util.function.BiFunction;
import lombok.Getter;
import lombok.Setter;

/**
 *
 * @author t.marx
 */
public class ElasticsearchIndexConfiguration extends IndexConfiguration {
	final MultiMap<String, FieldConfiguration> fieldConfigurations = new MultiMap<>();
	
	@Getter
	@Setter
	private BiFunction<String, String, String> indexNameMapper = (database, collection) -> collection;
	
	public ElasticsearchIndexConfiguration addFieldConfiguration (final String collection, final FieldConfiguration fieldConfig) {
		fieldConfigurations.put(collection, fieldConfig);
		return this;
	}
	
	public Collection<FieldConfiguration> getFieldConfigurations (final String collection) {
		return fieldConfigurations.get(collection);
	}
	public boolean hasFieldConfigurations (final String collection) {
		return fieldConfigurations.containsKey(collection);
	}
}
