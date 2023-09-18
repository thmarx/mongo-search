package com.github.thmarx.mongo.search.adapters.lucene.index;

import java.time.format.DateTimeFormatter;
import java.util.Collection;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetsConfig;

import com.github.thmarx.mongo.search.adapters.lucene.index.storage.Storage;
import com.github.thmarx.mongo.search.index.configuration.IndexConfiguration;
import com.github.thmarx.mongo.search.index.utils.MultiMap;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.Setter;

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
public class LuceneIndexConfiguration extends IndexConfiguration<org.bson.Document, Document, LuceneFieldConfiguration> {
	Storage storage;
	
	Analyzer analyzer;

	@Getter
	@Setter
	FacetsConfig facetsConfig;
	
	@Getter
	@Setter
	long commitDelaySeconds = 1;
	
	@Getter
	@Setter
	DateTimeFormatter defaultDateFormatter = DateTimeFormatter.ISO_LOCAL_DATE;
	
	public LuceneIndexConfiguration setStorage (final Storage storage) {
		this.storage = storage;
		return this;
	}
	
	public Storage getStorage () {
		return storage;
	}
	
	public Analyzer getAnalyzer () {
		return analyzer;
	}
	
	public LuceneIndexConfiguration setAnalyzer (final Analyzer analyzer) {
		this.analyzer = analyzer;
		return this;
	}
}
