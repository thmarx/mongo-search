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

import org.opensearch.testcontainers.OpensearchContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

/**
 *
 * @author t.marx
 */
public class AbstractContainerTest {

	protected OpensearchContainer opensearchContainer;
	protected MongoDBContainer mongdbContainer;

	@BeforeTest
	public void up() {
		opensearchContainer = new OpensearchContainer(DockerImageName.parse(
				"opensearchproject/opensearch:2.9.0"
		));
		opensearchContainer.start();
		
		mongdbContainer = new MongoDBContainer(DockerImageName.parse(
				"mongo:6.0.9"
		));
		mongdbContainer.start();
	}

	@AfterTest
	public void down() {
		opensearchContainer.stop();
		
		mongdbContainer.stop();
	}
}
