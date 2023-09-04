/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package de.marx_software.mongo.search.adapters.lucene;

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

import de.marx_software.mongo.search.adapter.AbstractIndexAdapter;
import de.marx_software.mongo.search.adapters.lucene.index.Documents;
import de.marx_software.mongo.search.adapters.lucene.index.LuceneIndex;
import de.marx_software.mongo.search.adapters.lucene.index.LuceneIndexConfiguration;
import de.marx_software.mongo.search.index.commands.Command;
import de.marx_software.mongo.search.index.commands.DeleteCommand;
import de.marx_software.mongo.search.index.commands.IndexCommand;
import de.marx_software.mongo.search.index.utils.PausableThread;
import java.io.IOException;
import java.util.List;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.bson.Document;

/**
 *
 * @author t.marx
 */
public class LuceneIndexAdapter extends AbstractIndexAdapter<LuceneIndexConfiguration> {
	
	LuceneIndex luceneIndex;
	
	private Thread queueWorkerThread;
	
	private PausableThread queueWorker;
	
	private Documents documentHelper;

	public LuceneIndexAdapter (final LuceneIndexConfiguration configuration) {
		super(configuration);
		
		this.documentHelper = new Documents(configuration);
	}
	
	@Override
	public void indexDocument(String collection, Document document) throws IOException {
		org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
		doc.add(new StringField("_id", document.getObjectId("_id").toString(), Field.Store.YES));
		doc.add(new StringField("_collection", collection, Field.Store.YES));
		
		luceneIndex.index(doc);
	}

	@Override
	public void commit() {
		luceneIndex.commit();
	}

	@Override
	public void close() throws Exception {
		queueWorker.stop();
		queueWorkerThread.interrupt();
		luceneIndex.close();
	}
	
	@Override
	public void startQueueWorker () {
		this.queueWorker.unpause();
	}
	
	public void open () throws IOException {
		
		luceneIndex = new LuceneIndex(configuration);
		luceneIndex.open();
		
		queueWorker = new PausableThread(true) {
			@Override
			public void update() {
				try {
					Command command = getCommandQueue().take();
					
					if (command instanceof IndexCommand index) {
						org.apache.lucene.document.Document doc = documentHelper.build(index);
						
						Query query = new BooleanQuery.Builder()
								.add(new TermQuery(new Term("_id", index.uid())), BooleanClause.Occur.MUST)
								.add(new TermQuery(new Term("_collection", index.collection())), BooleanClause.Occur.MUST)
								.build();
						luceneIndex.deleteDocuments(query);
						
						luceneIndex.index(doc);
					} else if (command instanceof DeleteCommand delete) {
						Query query = new BooleanQuery.Builder()
								.add(new TermQuery(new Term("_id", delete.uid())), BooleanClause.Occur.MUST)
								.add(new TermQuery(new Term("_collection", delete.collection())), BooleanClause.Occur.MUST)
								.build();
						luceneIndex.deleteDocuments(query);
					}
				} catch (InterruptedException ex) {
					// nothing to do
				} catch (IOException ex) {
					throw new RuntimeException(ex);
				}
			}
		};
		queueWorkerThread = new Thread(queueWorker);
		queueWorkerThread.start();
	}

	@Override
	public int size(String collection) {
		try {
			return luceneIndex.size(collection);
		} catch (IOException ex) {
			return 0;
		}
	}
	
}
