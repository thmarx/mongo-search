//package com.github.thmarx.mongo.search.adapters.lucene.index.storage;
//
///*-
// * #%L
// * monog-search-adapters-lucene
// * %%
// * Copyright (C) 2023 Marx-Software
// * %%
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// * 
// *      http://www.apache.org/licenses/LICENSE-2.0
// * 
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// * #L%
// */
//
//import java.io.IOException;
//
//import org.apache.lucene.store.Directory;
//import org.lumongo.storage.lucene.DistributedDirectory;
//import org.lumongo.storage.lucene.MongoDirectory;
//
//import com.mongodb.client.MongoClient;
//
//import lombok.RequiredArgsConstructor;
//
//@RequiredArgsConstructor
//public class MongoDbStorage implements Storage {
//
//    private Directory directory;
//
//    private final MongoClient mongoClient;
//    private final String database;
//    private final String indexName;
//    private boolean sharding = false;
//
//    public MongoDbStorage (final MongoClient client, final String database, final String indexName, final boolean sharding) {
//        this(client, database, indexName);
//    }
//    
//
//    @Override
//    public void close() throws Exception {
//        if (directory != null) {
//            directory.close();
//        }
//    }
//
//    @Override
//    public Directory getDirectory() {
//        return directory;
//    }
//
//    @Override
//    public void open() throws IOException {
//        directory = new DistributedDirectory(new MongoDirectory(mongoClient, database, indexName));
//    }
//    
//}
