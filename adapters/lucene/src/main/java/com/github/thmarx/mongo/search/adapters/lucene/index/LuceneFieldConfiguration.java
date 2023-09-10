package com.github.thmarx.mongo.search.adapters.lucene.index;

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

import org.bson.Document;

import com.github.thmarx.mongo.search.index.configuration.FieldConfiguration;

import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;


@SuperBuilder
@Getter
public class LuceneFieldConfiguration extends FieldConfiguration<Document, org.apache.lucene.document.Document> {

    /**
     * Field value is stored.
     * This is importend, if you like to have the value in the search result.
     */
    @Builder.Default
	private boolean stored = false;

    /**
     * Keyword fields are not tokenized. It's only relevant for fields holding text values.
     * If the value should be used for full text search, do not set keyword to true.
     * usage e.q. postalcodes or tags
     * default = false
     */
    @Builder.Default
    private boolean keyword = false;
}
