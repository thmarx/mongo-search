/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package de.marx_software.mongo.search.index;

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
import de.marx_software.mongo.search.index.indexer.Initializer;
import de.marx_software.mongo.search.index.indexer.Updater;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author t.marx
 */
public class MongoSearch implements AutoCloseable {

	IndexAdapter indexAdapter;

	Initializer initializer;

	Updater updater;
	Thread updaterThread;

	private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

	public MongoSearch() {
		scheduler.scheduleWithFixedDelay(() -> {
			indexAdapter.commit();
		}, 1, 1, TimeUnit.SECONDS);
	}
	
	public IndexAdapter getIndexAdapter () {
		return indexAdapter;
	}

	@Override
	public void close() throws Exception {
		updater.close();
		indexAdapter.close();
	}

	public void open(IndexAdapter indexAdapter, MongoDatabase database, List<String> collections) throws IOException {
		this.indexAdapter = indexAdapter;

		initializer = new Initializer(indexAdapter, database, collections);
		updater = new Updater(indexAdapter, database, collections);
		
		updaterThread = new Thread(() -> {
			updater.connect();
		});
		updaterThread.start();
		initializer.initialize((Void) -> {
			indexAdapter.startQueueWorker();
		});

	}
}
