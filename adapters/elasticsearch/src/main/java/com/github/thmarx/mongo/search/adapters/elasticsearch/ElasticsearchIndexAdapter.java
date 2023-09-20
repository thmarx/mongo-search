/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.github.thmarx.mongo.search.adapters.elasticsearch;

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
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.github.thmarx.mongo.search.adapter.AbstractIndexAdapter;
import com.github.thmarx.mongo.search.index.messages.Message;
import com.github.thmarx.mongo.search.index.messages.DeleteMessage;
import com.github.thmarx.mongo.search.index.messages.DropCollectionMessage;
import com.github.thmarx.mongo.search.index.messages.InsertMessage;
import com.github.thmarx.mongo.search.index.messages.UpdateMessage;
import com.github.thmarx.mongo.search.index.utils.PausableThread;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.Document;

/**
 *
 * @author t.marx
 */
public class ElasticsearchIndexAdapter extends AbstractIndexAdapter<ElasticsearchIndexConfiguration> {

	private Thread queueWorkerThread;

	private PausableThread queueWorker;

	private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

	ElasticsearchClient esClient;

	public ElasticsearchIndexAdapter(final ElasticsearchIndexConfiguration configuration) {
		super(configuration);

	}

	@Override
	public void indexDocument(String database, String collection, Document document) throws IOException {
		InsertMessage message = new InsertMessage(document.getObjectId("_id").toString(), database, collection, document);
		index(message);
	}

	@Override
	public void clearCollection(String database, String collection) throws IOException {
		DropCollectionMessage message = new DropCollectionMessage(database, collection);
		dropCollection(message);
	}

	public void commit() throws IOException {
		esClient.indices().refresh();
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

	@Override
	public void pauseQueueWorker() {
		this.queueWorker.pause();
	}

	public void open(ElasticsearchClient esClient) throws IOException {

		this.esClient = esClient;

		queueWorker = new PausableThread(true) {
			@Override
			public void update() {
				try {
					Message message = getMessageQueue().take();

					if (message instanceof InsertMessage indexCommand) {
						index(indexCommand);
					} else if (message instanceof DeleteMessage deleteCommand) {
						delete(deleteCommand);
					} else if (message instanceof UpdateMessage updateCommand) {
						updateDocument(updateCommand);
					} else if (message instanceof DropCollectionMessage dropCollectionCommand) {
						dropCollection(dropCollectionCommand);
					}

				} catch (Exception ex) {
				}
			}
		};
		queueWorkerThread = new Thread(queueWorker);
		queueWorkerThread.start();
	}

	private Map<String, Object> createDocument(final String database, final String collection, final Document document) {
		Map<String, Object> indexDocument = new HashMap<>();
		indexDocument.put("_database", database);
		indexDocument.put("_collection", collection);
		if (configuration.hasFieldConfigurations(collection)) {
			var fieldConfigs = configuration.getFieldConfigurations(collection);
			fieldConfigs.forEach((fc) -> {
				var value = fc.getMapper().getFieldValue(fc.getFieldName(), document);
				indexDocument.put(fc.getIndexFieldName(), value);
			});
		}

		var extender = configuration.getDocumentExtender(configuration.getIndexNameMapper().apply(database, collection));
		if (extender != null) {
			extender.accept(document, indexDocument);
		}

		return indexDocument;
	}

	private void index(InsertMessage command) {
		try {

			Map<String, Object> document = createDocument(command.database(), command.collection(), command.document());
			
			IndexResponse response = esClient.index(i -> i
					.index(configuration.getIndexNameMapper().apply(command.database(), command.collection()))
					.id(command.uid())
					.document(document));
		} catch (IOException | ElasticsearchException ex) {
			Logger.getLogger(ElasticsearchIndexAdapter.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	private void updateDocument(UpdateMessage command) {
		try {

			Map<String, Object> document = createDocument(command.database(), command.collection(), command.document());

			esClient.update(
					u -> u.index(configuration.getIndexNameMapper().apply(command.database(), command.collection()))
							.id(command.uid())
							.doc(document),
					Map.class);
		} catch (IOException | ElasticsearchException ex) {
			Logger.getLogger(ElasticsearchIndexAdapter.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	private void delete(DeleteMessage command) {
		try {

			DeleteResponse response = esClient.delete(i -> i
					.index(configuration.getIndexNameMapper().apply(command.database(), command.collection()))
					.id(command.uid()));
		} catch (IOException | ElasticsearchException ex) {
			Logger.getLogger(ElasticsearchIndexAdapter.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	/**
	 * In elasticsearch the index is not dropped, we just delete all documents
	 *
	 * @param command
	 */
	private void dropCollection(DropCollectionMessage command) {
		try {

			boolean exists = esClient.indices().exists(fn -> fn.index(configuration.getIndexNameMapper().apply(command.database(), command.collection()))).value();

			if (exists) {
				//esClient.indices().delete(fn -> fn.index(configuration.getIndexNameMapper().apply(command.database(), command.collection())));
				esClient.deleteByQuery(fn
						-> fn.index(configuration.getIndexNameMapper().apply(command.database(), command.collection()))
								.query(qb
										-> qb.match(t
										-> t.field("_collection").query(command.collection())
								)
								)
				);
			}
		} catch (IOException | ElasticsearchException ex) {
			Logger.getLogger(ElasticsearchIndexAdapter.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}
