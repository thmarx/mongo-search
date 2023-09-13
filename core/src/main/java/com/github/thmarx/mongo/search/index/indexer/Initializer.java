/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.github.thmarx.mongo.search.index.indexer;

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

import lombok.extern.slf4j.Slf4j;

import com.github.thmarx.mongo.search.adapter.IndexAdapter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.bson.Document;

/**
 *
 * @author t.marx
 */
@Slf4j
public class Initializer implements Runnable {
	
	private MongoDatabase database;
	private List<String> collections;
	
	private IndexAdapter indexAdapter;
	
	private CompletableFuture<Void> initFuture;
	
	private Consumer<Void> readyCallback;

	public Initializer (final IndexAdapter indexAdapter, final MongoDatabase database, final List<String> collections, final Consumer<Void> readyCallback) {
		this.database = database;
		this.collections = collections;
		this.indexAdapter = indexAdapter;
		this.readyCallback = readyCallback;
	}
	
	public boolean isRunning () {
		return !initFuture.isDone();
	}
	
	@Override
	public void run () {
		log.debug("initial index collections");
		collections.forEach(this::initCollection);
		log.debug("all collections indexed");
		readyCallback.accept(null);
	}
	
	private void initCollection (final String collectionName) {
		log.debug("indexing collection " + collectionName);
		log.debug("documents " + database.getCollection(collectionName).countDocuments());
		database.getCollection(collectionName).find().forEach(doc -> {
			try {
				indexDocument(collectionName, doc);
			} catch (Exception ex) {
				log.error("", ex);
				throw new RuntimeException(ex);
			}
		});
		log.debug("indexing collection finished");
	}
	
	private void indexDocument (final String collection, final Document document) throws IOException {
		indexAdapter.indexDocument(database.getName(), collection, document);
	}
}
