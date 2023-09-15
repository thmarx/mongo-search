/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.github.thmarx.mongo.search.adapters.solr;

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
import java.io.IOException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.SolrContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;

/**
 *
 * @author t.marx
 */
public class AbstractContainerTest {

	protected SolrContainer solrContainer;
	protected MongoDBContainer mongdbContainer;

	protected MongoClient mongoClient;
	protected SolrClient solrClient;
	protected MongoDatabase database;


	
	@BeforeClass
	public void up() {
		solrContainer = new SolrContainer(DockerImageName.parse(
				"solr:9.3"
		)).withCommand("schemaless");
		solrContainer.start();
		
		mongdbContainer = new MongoDBContainer(DockerImageName.parse(
				"mongo:6.0.9"
		));
		mongdbContainer.start();
		
		mongoClient = MongoClients.create(mongdbContainer.getConnectionString());
		database = mongoClient.getDatabase("search");

		// Do whatever you want with the client ...
		solrClient = new Http2SolrClient.Builder(
				"http://" + solrContainer.getHost() + ":" + solrContainer.getSolrPort() + "/solr"
		).build();
	}

	@AfterClass
	public void down() throws IOException {
		mongoClient.close();
		solrClient.close();
		
		solrContainer.stop();
		mongdbContainer.stop();
	}
}
