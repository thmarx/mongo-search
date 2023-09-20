/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.github.thmarx.mongo.search.adapters.lucene.index;

/*-
 * #%L
 * mongo-search-adapters-lucene
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

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author t.marx
 */
@Slf4j
@RequiredArgsConstructor
public class LuceneIndexManager implements AutoCloseable {
	
	private ConcurrentMap<String, LuceneIndex> indices = new ConcurrentHashMap<>();
	
	private final LuceneIndexConfiguration indexConfiguration;
	
	public void dropAllIndices () {
		indices.entrySet().forEach(entry -> {
			try {
				entry.getValue().close();
				indexConfiguration.getStorage().deleteDirectoy(entry.getKey());
			} catch (Exception ex) {
				log.error("error dropping index", ex);
			}
		});
		indices.clear();
	}
	
	public LuceneIndex createOrGet (final String database, final String collection) throws IOException {
		var indexName = indexConfiguration.getIndexNameMapper().apply(database, collection);
		if (!indices.containsKey(indexName)) {
			createIndex(database, collection);
		}
		
		return indices.get(indexName);
	}

	private synchronized void createIndex(final String database, final String collection) throws IOException {
		
		var indexName = indexConfiguration.getIndexNameMapper().apply(database, collection);
		
		LuceneIndex index = new LuceneIndex(indexName, indexConfiguration);
		index.open();
		indices.put(indexName, index);
	}

	@Override
	public void close() throws Exception {
		indices.values().forEach(index -> {
			try {
				index.close();
			} catch (Exception ex) {
				log.error("error closing index", ex);
			}
		});
	}
	
	public void commit()  {
		indices.values().forEach(index -> {
			try {
				index.commit();
			} catch (Exception ex) {
				log.error("error closing index", ex);
			}
		});
	}
}
