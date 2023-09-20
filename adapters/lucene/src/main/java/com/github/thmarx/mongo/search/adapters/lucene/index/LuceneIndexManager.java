/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.github.thmarx.mongo.search.adapters.lucene.index;

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
