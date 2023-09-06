/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Interface.java to edit this template
 */
package de.marx_software.mongo.search.adapter;

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

import de.marx_software.mongo.search.index.commands.Command;
import java.io.IOException;
import org.bson.Document;

/**
 *
 * @author t.marx
 */
public interface IndexAdapter extends AutoCloseable {

	void indexDocument (final String database, final String collection, final Document document) throws IOException;
	
	void enqueueCommand (Command command);
	
	void startQueueWorker();
}
