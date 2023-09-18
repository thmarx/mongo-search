/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.github.thmarx.mongo.search.adapters.opensearch;

/*-
 * #%L
 * monog-search-adapters-opensearch
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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.opensearch.client.RestClient;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.opensearch.testcontainers.OpensearchContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 *
 * @author t.marx
 */
public class AbstractContainerTest {

	protected OpensearchContainer opensearchContainer;
	protected MongoDBContainer mongdbContainer;
	
	protected MongoClient mongoClient;
	protected OpenSearchClient osClient;
	
	protected MongoDatabase database;;


	@BeforeClass
	public void up() throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
		opensearchContainer = new OpensearchContainer(DockerImageName.parse(
				"opensearchproject/opensearch:2.9.0"
		));
		opensearchContainer.start();
		
		mongdbContainer = new MongoDBContainer(DockerImageName.parse(
				"mongo:6.0.9"
		));
		mongdbContainer.start();
		
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(opensearchContainer.getUsername(), opensearchContainer.getPassword());
		credentialsProvider.setCredentials(AuthScope.ANY, credentials);

		final HttpHost host = HttpHost.create(opensearchContainer.getHttpHostAddress());

		final SSLContext sslcontext = SSLContextBuilder.create()
            .loadTrustMaterial(null, new TrustAllStrategy())
            .build();
		//Initialize the client with SSL and TLS enabled
		final RestClient restClient = RestClient.builder(host).
				setHttpClientConfigCallback((HttpAsyncClientBuilder httpClientBuilder) -> 
					httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setSSLContext(sslcontext)
		).build();

		final OpenSearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
		this.osClient = new OpenSearchClient(transport);

		//client = MongoClients.create(connectionString);
		mongoClient = MongoClients.create(mongdbContainer.getConnectionString());

		database = mongoClient.getDatabase("search");
	}

	@AfterClass
	public void down() {
		
		mongoClient.close();
		osClient.shutdown();
		
		opensearchContainer.stop();
		
		mongdbContainer.stop();
	}
}
