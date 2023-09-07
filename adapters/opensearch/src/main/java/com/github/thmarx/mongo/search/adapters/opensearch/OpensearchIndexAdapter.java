/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.github.thmarx.mongo.search.adapters.opensearch;

/*-
 * #%L
 * monog-search-adapters-lucene
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
import com.github.thmarx.mongo.search.adapter.AbstractIndexAdapter;
import com.github.thmarx.mongo.search.index.commands.Command;
import com.github.thmarx.mongo.search.index.commands.DeleteCommand;
import com.github.thmarx.mongo.search.index.commands.DropCollectionCommand;
import com.github.thmarx.mongo.search.index.commands.InsertCommand;
import com.github.thmarx.mongo.search.index.commands.UpdateCommand;
import com.github.thmarx.mongo.search.index.utils.PausableThread;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.Document;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.DeleteResponse;
import org.opensearch.client.opensearch.core.IndexResponse;

/**
 *
 * @author t.marx
 */
public class OpensearchIndexAdapter extends AbstractIndexAdapter<OpensearchIndexConfiguration> {

	private Thread queueWorkerThread;

	private PausableThread queueWorker;

	private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

	OpenSearchClient osClient;

	public OpensearchIndexAdapter(final OpensearchIndexConfiguration configuration) {
		super(configuration);

	}

	@Override
	public void indexDocument(String database, String collection, Document document) throws IOException {

	}

	public void commit() {
	}

	@Override
	public void close() throws Exception {
		scheduler.shutdown();
		queueWorker.stop();
		queueWorkerThread.interrupt();
	}

	@Override
	public void startQueueWorker() {
		this.queueWorker.unpause();
	}

	public void open(OpenSearchClient osClient) throws IOException {

		this.osClient = osClient;

		queueWorker = new PausableThread(true) {
			@Override
			public void update() {
				try {
					Command command = getCommandQueue().take();

					if (command instanceof InsertCommand indexCommand) {
						index(indexCommand);
					} else if (command instanceof DeleteCommand deleteCommand) {
						delete(deleteCommand);
					} else if (command instanceof UpdateCommand updateCommand) {
						OpensearchIndexAdapter.this.update(updateCommand);
					} else if (command instanceof DropCollectionCommand dropCollectionCommand) {
						dropCollection(dropCollectionCommand);
					}

				} catch (Exception ex) {
				}
			}
		};
		queueWorkerThread = new Thread(queueWorker);
		queueWorkerThread.start();
	}

	private void index(InsertCommand command) {
		try {

			Map<String, Object> document = new HashMap<>();
			document.put("_database", command.database());
			document.put("_collection", command.collection());
			if (configuration.hasFieldConfigurations(command.collection())) {
				var fieldConfigs = configuration.getFieldConfigurations(command.collection());
				fieldConfigs.forEach((fc) -> {
					var value = fc.getRetriever().getFieldValue(fc.getFieldName(), command.document());
					document.put(fc.getIndexFieldName(), value);
				});
			}

			IndexResponse response = osClient.index(i -> i
					.index(configuration.getIndexNameMapper().apply(command.database(), command.collection()))
					.id(command.uid())
					.document(document)
			);
		} catch (IOException ex) {
			Logger.getLogger(OpensearchIndexAdapter.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	private void update(UpdateCommand command) {
		try {

			Map<String, Object> document = new HashMap<>();
			document.put("_database", command.database());
			document.put("_collection", command.collection());
			if (configuration.hasFieldConfigurations(command.collection())) {
				var fieldConfigs = configuration.getFieldConfigurations(command.collection());
				fieldConfigs.forEach((fc) -> {
					var value = fc.getRetriever().getFieldValue(fc.getFieldName(), command.document());
					document.put(fc.getIndexFieldName(), value);
				});
			}

			osClient.update(u -> 
					u.index(configuration.getIndexNameMapper().apply(command.database(), command.collection()))
					.id(command.uid())
					.doc(document)
					, Map.class
			);
		} catch (IOException ex) {
			Logger.getLogger(OpensearchIndexAdapter.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	private void delete(DeleteCommand command) {
		try {

			DeleteResponse response = osClient.delete(i -> i
					.index(command.collection())
					.id(command.uid())
			);
		} catch (IOException ex) {
			Logger.getLogger(OpensearchIndexAdapter.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	private void dropCollection(DropCollectionCommand command) {
		try {

			boolean exists = osClient.indices().exists(fn -> fn.index(configuration.getIndexNameMapper().apply(command.database(), command.collection()))).value();
			
			if (exists) {
				osClient.indices().delete(fn -> fn.index(configuration.getIndexNameMapper().apply(command.database(), command.collection())));
			}
		} catch (IOException ex) {
			Logger.getLogger(OpensearchIndexAdapter.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}
