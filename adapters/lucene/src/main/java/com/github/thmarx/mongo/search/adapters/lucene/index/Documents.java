/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package de.marx_software.mongo.search.adapters.lucene.index;

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

import de.marx_software.mongo.search.index.commands.IndexCommand;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;

/**
 *
 * @author t.marx
 */
@RequiredArgsConstructor
public class Documents {

	private final LuceneIndexConfiguration configuration;

	public Document build(final IndexCommand command) {
		var doc = new Document();

		doc.add(new StringField("_id", command.uid(), Field.Store.YES));
		doc.add(new StringField("_collection", command.collection(), Field.Store.YES));
		doc.add(new StringField("_database", command.database(), Field.Store.YES));

		if (configuration.hasFieldConfigurations(command.collection())) {
			var fieldConfigs = configuration.getFieldConfigurations(command.collection());
			fieldConfigs.forEach((fc) -> {
				var value = fc.getRetriever().getFieldValue(fc.getFieldName(), command.document());
				if (value instanceof String stringValue) {
					doc.add(new StringField(fc.getIndexFieldName(), stringValue, Field.Store.YES));
				} else if (value instanceof List listValue) {
					listValue.stream().map(String.class::cast).forEach((stringValue) -> {
						doc.add(new StringField(fc.getIndexFieldName(), (String) stringValue, Field.Store.YES));
					});
				}

			});
		}

		return doc;
	}
}
