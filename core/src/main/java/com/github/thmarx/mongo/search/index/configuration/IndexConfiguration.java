/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.github.thmarx.mongo.search.index.configuration;

import com.github.thmarx.mongo.search.index.utils.MultiMap;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/*-
 * #%L
 * mongo-search-core
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
 *
 * @author t.marx
 */
public class IndexConfiguration<SD, TD, FCT extends FieldConfiguration> {
    @Getter
    @Setter
	private BiConsumer<SD, TD> documentExtender = (source, target) -> {};
	
	final MultiMap<String, FCT> fieldConfigurations = new MultiMap<>();
	
	@Getter
	@Setter
	private BiFunction<String, String, String> indexNameMapper = (database, collection) -> collection;
	
	public void addFieldConfiguration (final String collection, final FCT fieldConfig) {
		fieldConfigurations.put(collection, fieldConfig);
	}
	
	public Collection<FCT> getFieldConfigurations (final String collection) {
		return fieldConfigurations.get(collection);
	}
	public boolean hasFieldConfigurations (final String collection) {
		return fieldConfigurations.containsKey(collection);
	}
}
