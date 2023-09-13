package com.github.thmarx.mongo.search.adapter;

/*-
 * #%L
 * mongo-search-core
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

import com.github.thmarx.mongo.search.index.configuration.IndexConfiguration;
import com.github.thmarx.mongo.search.index.messages.Message;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.Getter;

/**
 *
 * @author t.marx
 */
public abstract class AbstractIndexAdapter<C extends IndexConfiguration> implements IndexAdapter {

	protected final C configuration;
	
	@Getter
	protected BlockingQueue<Message> messageQueue;
	
	protected AbstractIndexAdapter (final C configuration) {
		this.configuration = configuration;
		this.messageQueue = new LinkedBlockingQueue<>();
	}
	
	@Override
	public void enqueueMessage (Message command) {
		messageQueue.add(command);
	}
}
