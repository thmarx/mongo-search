/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.github.thmarx.mongo.search.adapters.elasticsearch;

/*-
 * #%L
 * monog-search-adapters-elasticsearch
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

import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

import java.time.Duration;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;

/**
 *
 * @author t.marx
 */
public class AbstractContainerTest {

	protected ElasticsearchContainer elasticSearchContainer;
	protected MongoDBContainer mongdbContainer;
	protected MongoClient mongoClient;

	RestClient restClient;
	ElasticsearchTransport transport;
	protected ElasticsearchClient esClient;

	@BeforeClass
	public void up() {
		elasticSearchContainer = new ElasticsearchContainer(DockerImageName.parse(
				"docker.elastic.co/elasticsearch/elasticsearch:8.9.2"));
		elasticSearchContainer.withStartupTimeout(Duration.ofSeconds(120));
		elasticSearchContainer.start();

		mongdbContainer = new MongoDBContainer(DockerImageName.parse(
				"mongo:6.0.9"));
		mongdbContainer.start();
		mongoClient = MongoClients.create(mongdbContainer.getConnectionString());

		String protocol = elasticSearchContainer.caCertAsBytes().isPresent() ? "https://" : "http://";

		restClient = RestClient
				.builder(HttpHost.create(protocol + elasticSearchContainer.getHttpHostAddress()))
				.setHttpClientConfigCallback((clientBuilder) -> {

					if (elasticSearchContainer.caCertAsBytes().isPresent()) {
						clientBuilder.setSSLContext(elasticSearchContainer.createSslContextFromCa());
					}

					final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
					UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("elastic", ElasticsearchContainer.ELASTICSEARCH_DEFAULT_PASSWORD);
					credentialsProvider.setCredentials(AuthScope.ANY, credentials);

					return clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
				})
				.build();

// Create the transport with a Jackson mapper
		transport = new RestClientTransport(
				restClient, new JacksonJsonpMapper());

// And create the API client
		esClient = new ElasticsearchClient(transport);
	}

	@AfterClass
	public void down() {
		esClient.shutdown();
		elasticSearchContainer.stop();
		mongoClient.close();
		mongdbContainer.stop();
	}
}
