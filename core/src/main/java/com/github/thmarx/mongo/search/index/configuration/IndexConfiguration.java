package com.github.thmarx.mongo.search.index.configuration;

import com.github.thmarx.mongo.search.index.utils.MultiMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
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
 * Base index configuration. Each index implementation can have a custom subclass.
 *
 * @author t.marx
 */
public class IndexConfiguration<SD, TD, FCT extends FieldConfiguration> {
	/**
	 * Default extender for the document. It's used to add fields to the index document which are not in the mongodb document.
	 */
    @Setter
	private BiConsumer<SD, TD> documentExtender = (source, target) -> {};
	/**
	 * Document extender configured per index.
	 */
	Map<String, BiConsumer<SD, TD>> documentExtendersPerIndex = new HashMap<>();
	
	/**
	 * All field configurations.
	 */
	final MultiMap<String, FCT> fieldConfigurations = new MultiMap<>();
	
	/**
	 * Mapper to create the name for the index, based on databasename and collectionname. 
	 * Default index name: name of the collection
	 * 
	 * Attention: This mapper has no effect on the LuceneAdapter.
	 */
	@Getter
	@Setter
	private BiFunction<String, String, String> indexNameMapper = (database, collection) -> collection;
	
	/**
	 * Add a field configuration for a collection.
	 * 
	 * @param collection
	 * @param fieldConfig 
	 */
	public void addFieldConfiguration (final String collection, final FCT fieldConfig) {
		fieldConfigurations.put(collection, fieldConfig);
	}
	
	/**
	 * Returns all field configurations for a collection.
	 * 
	 * @param collection
	 * @return 
	 */
	public Collection<FCT> getFieldConfigurations (final String collection) {
		return fieldConfigurations.get(collection);
	}
	/**
	 * Checks if there are field configurations for a collection.
	 * 
	 * @param collection
	 * @return 
	 */
	public boolean hasFieldConfigurations (final String collection) {
		return fieldConfigurations.containsKey(collection);
	}
	
	/**
	 * Add document extender for specific index.
	 * @param index
	 * @param extender 
	 */
	public void addDocumentExtender (final String index, BiConsumer<SD, TD> extender) {
		documentExtendersPerIndex.put(index, extender);
	}
	
	/**
	 * Get the document extender for an index.
	 * If no extender is configured, the default extender is used.
	 * 
	 * @param index
	 * @return 
	 */
	public BiConsumer<SD, TD> getDocumentExtender (final String index) {
		if (documentExtendersPerIndex.containsKey(index)) {
			return documentExtendersPerIndex.get(index);
		}
		
		
		return documentExtender;
	}
}
