/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.github.thmarx.mongo.search.adapters.elasticsearch;

import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

/**
 *
 * @author t.marx
 */
public class AbstractContainerTest {

	protected ElasticsearchContainer elasticSearchContainer;
	protected MongoDBContainer mongdbContainer;

	@BeforeTest
	public void up() {
		elasticSearchContainer = new ElasticsearchContainer(DockerImageName.parse(
				"docker.elastic.co/elasticsearch/elasticsearch:8.9.1-amd64"
		));
		elasticSearchContainer.start();
		
		mongdbContainer = new MongoDBContainer(DockerImageName.parse(
				"mongo:6.0.9"
		));
		mongdbContainer.start();
	}

	@AfterTest
	public void down() {
		elasticSearchContainer.stop();
		
		mongdbContainer.stop();
	}
}
