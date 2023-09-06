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
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.github.thmarx.mongo.search.adapter.AbstractIndexAdapter;
import com.github.thmarx.mongo.search.index.commands.Command;
import com.github.thmarx.mongo.search.index.commands.DeleteCommand;
import com.github.thmarx.mongo.search.index.commands.IndexCommand;
import com.github.thmarx.mongo.search.index.utils.PausableThread;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.bson.Document;
import org.elasticsearch.client.RestClient;

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

	public void open(ElasticsearchClient esClient) throws IOException {

		this.esClient = esClient;
		

		queueWorker = new PausableThread(true) {
			@Override
			public void update() {
				try {
					Command command = getCommandQueue().take();

					if (command instanceof IndexCommand indexCommand) {
						index(indexCommand);
					}

				} catch (Exception ex) {
					// nothing to do
					ex.printStackTrace();
				}
			}
		};
		queueWorkerThread = new Thread(queueWorker);
		queueWorkerThread.start();
	}

	private void index(IndexCommand command) {
		try {

			Map<String, Object> document = new HashMap<>();
			if (configuration.hasFieldConfigurations(command.collection())) {
				var fieldConfigs = configuration.getFieldConfigurations(command.collection());
				fieldConfigs.forEach((fc) -> {
					var value = fc.getRetriever().getFieldValue(fc.getFieldName(), command.document());
					document.put(fc.getIndexFieldName(), value);
				});
			}

			IndexResponse response = esClient.index(i -> i
					.index(command.collection())
					.id(command.uid())
					.document(document)
			);
			System.out.println(response.id());
		} catch (IOException | ElasticsearchException ex) {
			Logger.getLogger(ElasticsearchIndexAdapter.class.getName()).log(Level.SEVERE, null, ex);
			ex.printStackTrace();
		}
	}

	private void delete(DeleteCommand command) {
		try {

			DeleteResponse response = esClient.delete(i -> i
					.index(command.collection())
					.id(command.uid())
			);
		} catch (IOException | ElasticsearchException ex) {
			Logger.getLogger(ElasticsearchIndexAdapter.class.getName()).log(Level.SEVERE, null, ex);
			ex.printStackTrace();
		}
	}
}
