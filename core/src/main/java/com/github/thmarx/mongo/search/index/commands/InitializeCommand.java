package com.github.thmarx.mongo.search.index.commands;

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

import java.io.IOException;
import java.util.List;

import org.bson.Document;

import com.github.thmarx.mongo.search.adapter.IndexAdapter;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class InitializeCommand implements Command {

    private final List<String> collections;

    @Override
    public void execute(final IndexAdapter indexAdapter, final MongoDatabase database) {
        log.debug("run initialize command");
        try {
            indexAdapter.pauseQueueWorker();

            collections.forEach((collection) -> {
                log.debug("initialize collection " + collection);
                MongoCollection<Document> dbCol = database.getCollection(collection);
                if (dbCol != null) {
                    try {
                        log.debug("clear collection " + collection);
                        indexAdapter.clearCollection(database.getName(), collection);
                    } catch (IOException e) {
                        log.error("error clearing collection", e);
                    }
                    log.debug("index collection " + collection);
					log.debug(dbCol.countDocuments() + " documents to index");
                    dbCol.find(Filters.empty()).forEach((document) -> {
                        try {
                            indexAdapter.indexDocument(database.getName(), collection, document);
                        } catch (IOException e) {
                            log.error("error indexing document", e);
                        }
                    });
					log.debug("collection indexed");
                }
            });

        } finally {
            indexAdapter.startQueueWorker();
        }
    }

}
