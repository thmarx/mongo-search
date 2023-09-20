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
	
	private ConcurrentMap<Key, LuceneIndex> indices = new ConcurrentHashMap<>();
	
	private final LuceneIndexConfiguration indexConfiguration;
	
	public boolean hasIndices (final String database) {
		return indices.keySet().stream().anyMatch(key -> key.database.equals(database));
	}
	
	public void dropIndices (final String database) {
		indices.entrySet().stream().filter((entry) -> entry.getKey().database.equals(database)).forEach(entry -> {
			try {
				var indexName = indexConfiguration.getIndexNameMapper().apply(entry.getKey().database, entry.getKey().collection);
				entry.getValue().close();
				indexConfiguration.getStorage().deleteDirectoy(indexName);
			} catch (Exception ex) {
				log.error("error deleting index", ex);
			}
		});
	}
	
	public LuceneIndex createOrGet (final String database, final String collection) throws IOException {
		var key = new Key(database, collection);
		if (!indices.containsKey(key)) {
			createIndex(database, collection);
		}
		
		return indices.get(key);
	}

	private synchronized void createIndex(final String database, final String collection) throws IOException {
		
		var key = new Key(database, collection);
		var indexName = indexConfiguration.getIndexNameMapper().apply(database, collection);
		
		LuceneIndex index = new LuceneIndex(indexName, indexConfiguration);
		index.open();
		indices.put(key, index);
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
	
	public record Key(String database, String collection){}
	
}
