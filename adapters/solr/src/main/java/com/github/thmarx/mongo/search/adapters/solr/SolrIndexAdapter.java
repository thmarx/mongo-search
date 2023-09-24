/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.github.thmarx.mongo.search.adapters.solr;

/*-
 * #%L
 * monog-search-adapters-solr
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
import com.github.thmarx.mongo.search.index.configuration.DocumentExtender;
import com.github.thmarx.mongo.search.index.messages.Message;
import com.github.thmarx.mongo.search.index.messages.DeleteMessage;
import com.github.thmarx.mongo.search.index.messages.DropCollectionMessage;
import com.github.thmarx.mongo.search.index.messages.InsertMessage;
import com.github.thmarx.mongo.search.index.messages.UpdateMessage;
import com.github.thmarx.mongo.search.index.utils.PausableThread;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.bson.Document;

/**
 *
 * @author t.marx
 */
@Slf4j
public class SolrIndexAdapter extends AbstractIndexAdapter<SolrIndexConfiguration> {

	private Thread queueWorkerThread;

	private PausableThread queueWorker;

	SolrClient indexClient;

	public SolrIndexAdapter(final SolrIndexConfiguration configuration) {
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
		try {
			indexClient.commit();
		} catch (SolrServerException | IOException ex) {
			log.error("", ex);
		}
	}

	@Override
	public void close() throws Exception {
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

	public void open(SolrClient indexClient) throws IOException {

		this.indexClient = indexClient;

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
				} catch (InterruptedException ie) {
					// nothing to to here
				} catch (Exception ex) {
					log.error("", ex);
				}
			}
		};
		queueWorkerThread = new Thread(queueWorker);
		queueWorkerThread.start();
	}

	private SolrInputDocument createDocument(final String uuid, final String database, final String collection, final Document document) {
		SolrInputDocument indexDocument = new SolrInputDocument();
		
		indexDocument.addField("id", uuid);
		indexDocument.addField("_database", database);
		indexDocument.addField("_collection", collection);
		if (configuration.hasFieldConfigurations(collection)) {
			var fieldConfigs = configuration.getFieldConfigurations(collection);
			fieldConfigs.forEach((fc) -> {
				var value = fc.getMapper().getFieldValue(fc.getFieldName(), document);
				indexDocument.addField(fc.getIndexFieldName(), value);
			});
		}

		if (configuration.getDocumentExtender() != null) {
			configuration.getDocumentExtender().extend(new DocumentExtender.Context(database, collection), document, indexDocument);
		}

		return indexDocument;
	}

	private void index(InsertMessage command) {
		try {

			SolrInputDocument document = createDocument(command.uid(), command.database(), command.collection(), command.document());
			
			if (configuration.commitWithin.isPresent()) {
				indexClient.add(
					configuration.getIndexNameMapper().apply(command.database(), command.collection())
					, document
					, configuration.commitWithin.get());
			} else {
				indexClient.add(
					configuration.getIndexNameMapper().apply(command.database(), command.collection())
					, document);
			}
		} catch (IOException | SolrServerException ex) {
			log.error("error " ,ex);
		}
	}

	private void updateDocument(UpdateMessage command) {
		try {

			SolrInputDocument document = createDocument(command.uid(), command.database(), command.collection(), command.document());
			
			if (configuration.commitWithin.isPresent()) {
				indexClient.add(
					configuration.getIndexNameMapper().apply(command.database(), command.collection())
					, document
					, configuration.commitWithin.get());
			} else {
				indexClient.add(
					configuration.getIndexNameMapper().apply(command.database(), command.collection())
					, document);
			}
		} catch (IOException | SolrServerException ex) {
			log.error("error " ,ex);
		}
	}

	private void delete(DeleteMessage command) {
		try {

			if (configuration.commitWithin.isPresent()) {
				indexClient.deleteById(
					configuration.getIndexNameMapper().apply(command.database(), command.collection()), 
					command.uid(),
					configuration.commitWithin.get());
			} else {
				indexClient.deleteById(
					configuration.getIndexNameMapper().apply(command.database(), command.collection()), 
					command.uid());
			}
			
		} catch (IOException | SolrServerException ex) {
			log.error("error " ,ex);
		}
	}

	/**
	 * In elasticsearch the index is not dropped, we just delete all documents
	 *
	 * @param command
	 */
	private void dropCollection(DropCollectionMessage command) {
		try {

			String query = "_database : '%s' AND _collection: '%s' ".formatted(command.database(), command.collection());
			
			if (configuration.commitWithin.isPresent()) {
				indexClient.deleteByQuery(
					configuration.getIndexNameMapper().apply(command.database(), command.collection()), 
					query,
					configuration.commitWithin.get());
			} else {
				indexClient.deleteByQuery(
					configuration.getIndexNameMapper().apply(command.database(), command.collection()), 
					query);
			}
			
		} catch (IOException | SolrServerException ex) {
			log.error("error " ,ex);
		}
	}
}
