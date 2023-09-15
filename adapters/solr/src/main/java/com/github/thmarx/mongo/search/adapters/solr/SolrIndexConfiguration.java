/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.github.thmarx.mongo.search.adapters.solr;

/*-
 * #%L
 * monog-search-adapters-solr
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

import com.github.thmarx.mongo.search.index.configuration.FieldConfiguration;
import com.github.thmarx.mongo.search.index.configuration.IndexConfiguration;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import org.apache.solr.common.SolrInputDocument;
import org.bson.Document;

/**
 *
 * @author t.marx
 */
public class SolrIndexConfiguration extends IndexConfiguration<Document, SolrInputDocument, FieldConfiguration> {
	
	protected Optional<Integer> commitWithin = Optional.empty();
	
	public void setCommitWithin (int commitWithinMS) {
		commitWithin = Optional.of(commitWithinMS);
	}
}
