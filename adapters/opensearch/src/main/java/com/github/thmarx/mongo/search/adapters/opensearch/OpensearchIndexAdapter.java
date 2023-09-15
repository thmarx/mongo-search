package com.github.thmarx.mongo.search.adapters.opensearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.Document;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch.core.DeleteResponse;
import org.opensearch.client.opensearch.core.IndexResponse;

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
import com.github.thmarx.mongo.search.index.messages.DeleteMessage;
import com.github.thmarx.mongo.search.index.messages.DropCollectionMessage;
import com.github.thmarx.mongo.search.index.messages.InsertMessage;
import com.github.thmarx.mongo.search.index.messages.Message;
import com.github.thmarx.mongo.search.index.messages.UpdateMessage;
import com.github.thmarx.mongo.search.index.utils.PausableThread;

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
		InsertMessage message = new InsertMessage(document.getObjectId("_id").toString(), database, collection, document);
		index(message);
	}

	@Override
	public void clearCollection(String database, String collection) throws IOException {
		DropCollectionMessage message = new DropCollectionMessage(database, collection);
		dropCollection(message);
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

	@Override
	public void pauseQueueWorker() {
		this.queueWorker.pause();
	}

	public void open(OpenSearchClient osClient) throws IOException {

		this.osClient = osClient;

		queueWorker = new PausableThread(true) {
			@Override
			public void update() {
				try {
					Message command = getMessageQueue().take();

					if (command instanceof InsertMessage indexCommand) {
						index(indexCommand);
					} else if (command instanceof DeleteMessage deleteCommand) {
						delete(deleteCommand);
					} else if (command instanceof UpdateMessage updateCommand) {
						updateDocument(updateCommand);
					} else if (command instanceof DropCollectionMessage dropCollectionCommand) {
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

		if (configuration.getDocumentExtender() != null) {
			configuration.getDocumentExtender().accept(document, indexDocument);
		}

		return indexDocument;
	}

	private void index(InsertMessage command) {
		try {

			Map<String, Object> document = createDocument(command.database(), command.collection(), command.document());

			IndexResponse response = osClient.index(i -> i
					.index(configuration.getIndexNameMapper().apply(command.database(), command.collection()))
					.id(command.uid())
					.document(document)
			);
		} catch (IOException ex) {
			Logger.getLogger(OpensearchIndexAdapter.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	private void updateDocument(UpdateMessage command) {
		try {
			Map<String, Object> document = createDocument(command.database(), command.collection(), command.document());

			osClient.update(u
					-> u.index(configuration.getIndexNameMapper().apply(command.database(), command.collection()))
							.id(command.uid())
							.doc(document),
					 Map.class
			);
		} catch (IOException ex) {
			Logger.getLogger(OpensearchIndexAdapter.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	private void delete(DeleteMessage command) {
		try {

			DeleteResponse response = osClient.delete(i -> i
					.index(configuration.getIndexNameMapper().apply(command.database(), command.collection()))
					.id(command.uid())
			);
		} catch (IOException ex) {
			Logger.getLogger(OpensearchIndexAdapter.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	/**
	 * For OpenSearch the collection will not be dropped!
	 *
	 * @param command
	 */
	private void dropCollection(DropCollectionMessage command) {
		try {

			boolean exists = osClient.indices().exists(fn -> fn.index(configuration.getIndexNameMapper().apply(command.database(), command.collection()))).value();

			if (exists) {
				//osClient.indices().delete(fn -> fn.index(configuration.getIndexNameMapper().apply(command.database(), command.collection())));
				osClient.deleteByQuery(fn
						-> fn.index(configuration.getIndexNameMapper().apply(command.database(), command.collection()))
								.query(qb
										-> qb.match(t
										-> t.field("_collection").query(FieldValue.of(command.collection()))
								)
								)
				);
			}
		} catch (IOException ex) {
			Logger.getLogger(OpensearchIndexAdapter.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}
