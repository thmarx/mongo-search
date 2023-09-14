/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.github.thmarx.mongo.search.index;

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

import com.github.thmarx.mongo.trigger.Event;
import com.github.thmarx.mongo.trigger.MongoTriggers;
import com.mongodb.client.MongoDatabase;

import lombok.extern.slf4j.Slf4j;

import com.github.thmarx.mongo.search.adapter.IndexAdapter;
import com.github.thmarx.mongo.search.index.commands.Command;
import com.github.thmarx.mongo.search.index.indexer.Updater;
import com.github.thmarx.mongo.trigger.CollectionTrigger;
import com.github.thmarx.mongo.trigger.DatabaseTrigger;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author t.marx
 */
@Slf4j
public class MongoSearch implements AutoCloseable {

	IndexAdapter indexAdapter;

	Updater updater;

	MongoTriggers mongoTriggers;

	ExecutorService executorService;

	MongoDatabase database;

	public MongoSearch() {
		this.executorService = Executors.newSingleThreadExecutor();
	}

	public IndexAdapter getIndexAdapter() {
		return indexAdapter;
	}

	@Override
	public void close() throws Exception {
		executorService.shutdown();
		mongoTriggers.close();
		indexAdapter.close();
	}

	public void execute (final Command command) {
		executorService.execute(() -> {
			try {
				command.execute(indexAdapter, database);
			} catch (Exception e) {
				log.error("error executing command", e);
			}
		});
	}

	public void open(IndexAdapter indexAdapter, MongoDatabase database, List<String> collections) throws IOException {
		this.indexAdapter = indexAdapter;
		this.database = database;

		updater = new Updater(indexAdapter, collections);

		mongoTriggers = new MongoTriggers();
		mongoTriggers.register((type, databasename) -> {

			if (DatabaseTrigger.Type.DROPPED.equals(type)) {
				updater.dropDatabase(databasename);
			}
		});
		mongoTriggers.register((type, databasename, collectionname) -> {
			if (CollectionTrigger.Type.DROPPED.equals(type)) {
				updater.dropCollection(databasename, collectionname);
			}
		});
		mongoTriggers.register(Event.INSERT, (databasename, collectionname, document) -> {
			updater.insert(document);
		});
		mongoTriggers.register(Event.DELETE, (databasename, collectionname, document) -> {
			updater.delete(document);
		});
		mongoTriggers.register(Event.UPDATE, (databasename, collectionname, document) -> {
			updater.update(document);
		});

		mongoTriggers.open(database);

		indexAdapter.startQueueWorker();
	}
}
