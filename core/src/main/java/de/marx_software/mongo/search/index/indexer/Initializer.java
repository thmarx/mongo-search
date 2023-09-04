/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package de.marx_software.mongo.search.index.indexer;

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

import com.mongodb.client.MongoDatabase;
import de.marx_software.mongo.search.adapter.IndexAdapter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.bson.Document;

/**
 *
 * @author t.marx
 */
public class Initializer {
	
	private MongoDatabase database;
	private List<String> collections;
	
	private IndexAdapter indexAdapter;
	
	private CompletableFuture<Void> initFuture;
	
	public Initializer (final IndexAdapter indexAdapter, final MongoDatabase database, final List<String> collections) {
		this.database = database;
		this.collections = collections;
		this.indexAdapter = indexAdapter;
	}
	
	public boolean isRunning () {
		return !initFuture.isDone();
	}
	
	public void initialize (Consumer<Void> ready) {
		initFuture = CompletableFuture.runAsync(() -> {
			collections.forEach(this::initCollection);
			ready.accept(null);
		});
	}
	
	private void initCollection (final String collectionName) {
		database.getCollection(collectionName).find().forEach(doc -> {
			try {
				indexDocument(collectionName, doc);
			} catch (IOException ex) {
				throw new RuntimeException(ex);
			}
		});
	}
	
	private void indexDocument (final String collection, final Document document) throws IOException {
		
		indexAdapter.indexDocument(collection, document);
		

	}
}
