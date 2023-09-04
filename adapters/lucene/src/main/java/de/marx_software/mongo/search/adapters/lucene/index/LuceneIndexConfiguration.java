package de.marx_software.mongo.search.adapters.lucene.index;

import de.marx_software.mongo.search.adapters.lucene.index.storage.Storage;
import de.marx_software.mongo.search.index.configuration.FieldConfiguration;
import de.marx_software.mongo.search.index.configuration.IndexConfiguration;
import de.marx_software.mongo.search.index.utils.MultiMap;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;

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
 *
 * @author t.marx
 */
public class LuceneIndexConfiguration extends IndexConfiguration {
	Storage storage;
	
	Analyzer defaultAnalyzer;
	
	final MultiMap<String, FieldConfiguration> fieldConfigurations = new MultiMap<>();
	
	public LuceneIndexConfiguration addFieldConfiguration (final String collection, final FieldConfiguration fieldConfig) {
		fieldConfigurations.put(collection, fieldConfig);
		return this;
	}
	
	public Collection<FieldConfiguration> getFieldConfigurations (final String collection) {
		return fieldConfigurations.get(collection);
	}
	public boolean hasFieldConfigurations (final String collection) {
		return fieldConfigurations.containsKey(collection);
	}
	
	public LuceneIndexConfiguration setStorage (final Storage storage) {
		this.storage = storage;
		return this;
	}
	
	public Storage getStorage () {
		return storage;
	}
	
	public Analyzer getDefaultAnalyzer () {
		return defaultAnalyzer;
	}
	
	public LuceneIndexConfiguration setDefaultAnalyzer (final Analyzer defaultAnalyzer) {
		this.defaultAnalyzer = defaultAnalyzer;
		return this;
	}
}
