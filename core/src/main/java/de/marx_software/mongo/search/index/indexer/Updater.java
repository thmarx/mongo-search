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

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import de.marx_software.mongo.search.adapter.IndexAdapter;
import de.marx_software.mongo.search.index.commands.DeleteCommand;
import de.marx_software.mongo.search.index.commands.IndexCommand;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.bson.Document;

/**
 *
 * @author t.marx
 */
@RequiredArgsConstructor
public class Updater implements AutoCloseable {
	
	private final IndexAdapter indexAdapter;
	private final MongoDatabase database;
	private final List<String> collections;
	
	private Thread watcher;
	
	public void connect () {
		ChangeStreamIterable<Document> watch = database.watch().fullDocument(FullDocument.UPDATE_LOOKUP);
		watcher = new Thread(() -> {
			try {
				watch.forEach(this::handle);
			} catch (Exception e) {
				// nichts zu tun
			}
		}, "ChangeStreamUpdated");
		
		watcher.start();
	}
	
	public void handle (ChangeStreamDocument<Document> document) {
		if (isCollectionRelevant(document)) {
			switch (document.getOperationType()) {
				case INSERT -> insert(document);
				case UPDATE -> insert(document);
				case DELETE -> delete(document);
			}
		}
	}

	private boolean isCollectionRelevant(ChangeStreamDocument<Document> document) {
		return document.getNamespace() != null && document.getNamespace().getCollectionName() != null && collections.contains(document.getNamespace().getCollectionName());
	}
	
	@Override
	public void close () {
		watcher.interrupt();
	}

	private void insert(ChangeStreamDocument<Document> document) {
		var uid = document.getDocumentKey().getObjectId("_id").getValue().toString();
		var collection = document.getNamespace().getCollectionName();
		
		
		var command = new IndexCommand(uid, collection, document.getFullDocument());
		indexAdapter.enqueueCommand(command);
	}

	private void delete(ChangeStreamDocument<Document> document) {
		var uid = document.getDocumentKey().getObjectId("_id").getValue().toString();
		var collection = document.getNamespace().getCollectionName();
		
		var command = new DeleteCommand(uid, collection);
		indexAdapter.enqueueCommand(command);
	}
}
