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
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.github.thmarx.mongo.search.adapter.IndexAdapter;
import com.github.thmarx.mongo.search.index.commands.DeleteCommand;
import com.github.thmarx.mongo.search.index.commands.DropCollectionCommand;
import com.github.thmarx.mongo.search.index.commands.DropDatabaseCommand;
import com.github.thmarx.mongo.search.index.commands.IndexCommand;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.bson.Document;

/**
 *
 * @author t.marx
 */
@RequiredArgsConstructor
public class Updater {

	private final IndexAdapter indexAdapter;
	private final List<String> collections;


	public void insert(ChangeStreamDocument<Document> document) {
		var collection = document.getNamespace().getCollectionName();
		
		if (!collections.contains(collection)) {
			return;
		}
		
		var uid = document.getDocumentKey().getObjectId("_id").getValue().toString();
		var databaseName = document.getDatabaseName();

		var command = new IndexCommand(uid, databaseName, collection, document.getFullDocument());
		indexAdapter.enqueueCommand(command);
	}

	public void delete(ChangeStreamDocument<Document> document) {
		var collection = document.getNamespace().getCollectionName();
		
		if (!collections.contains(collection)) {
			return;
		}
		
		var uid = document.getDocumentKey().getObjectId("_id").getValue().toString();
		var databaseName = document.getDatabaseName();

		var command = new DeleteCommand(uid, databaseName, collection);
		indexAdapter.enqueueCommand(command);
	}

	public void dropCollection(final String database, final String collection) {
		if (!collections.contains(collection)) {
			return;
		}
		
		var command = new DropCollectionCommand(database, collection);
		indexAdapter.enqueueCommand(command);
	}

	public void dropDatabase(ChangeStreamDocument<Document> document) {
		var databaseName = document.getDatabaseName();

		var command = new DropDatabaseCommand(databaseName);
		indexAdapter.enqueueCommand(command);
	}
}
