package com.github.thmarx.mongo.search.adapters.lucene.index;

import java.time.ZoneId;
import java.util.Date;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;

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

import com.github.thmarx.mongo.search.index.commands.InsertCommand;

import lombok.RequiredArgsConstructor;

/**
 *
 * @author t.marx
 */
@RequiredArgsConstructor
public class Documents {

	private final LuceneIndexConfiguration configuration;

	public Document build(final String database, final String collection, final String uid, final org.bson.Document document) {
		var doc = new Document();

		doc.add(new StringField("_id", uid, Field.Store.YES));
		doc.add(new StringField("_collection", collection, Field.Store.YES));
		doc.add(new StringField("_database", database, Field.Store.YES));

		if (configuration.hasFieldConfigurations(collection)) {
			var fieldConfigs = configuration.getFieldConfigurations(collection);
			fieldConfigs.forEach((fc) -> {
				var value = fc.getMapper().getFieldValue(fc.getFieldName(), document);

				if (
					value == null 
					|| (value instanceof List<?> listValue && listValue.isEmpty())) {
					value = fc.getDefaultValue().get();
				}

				if (value instanceof List<?> listValue && !listValue.isEmpty()) {
					addListField(listValue, doc, fc);
				} else if (value != null)  {
					addField(value, doc, fc);
				}
			});
		}

		if (configuration.getDocumentExtender() != null) {
			configuration.getDocumentExtender().accept(document, doc);
		}

		return doc;
	}
	public Document build(final InsertCommand command) {

		return build(command.database(), command.collection(), command.uid(), command.document());
	}

	private void addListField(List<?> value, Document document, LuceneFieldConfiguration fc) {
		if (value == null || value.isEmpty()) {
			return;
		}

		value.forEach((item) -> addField(item, document, fc));
	}

	private void addField(Object value, Document document, LuceneFieldConfiguration fc) {
		if (value == null) {
			return;
		}
		if (value instanceof String stringValue) {
			if (fc.isKeyword()) {
				document.add(new StringField(fc.getIndexFieldName(), stringValue, Field.Store.NO));
			} else {
				document.add(new TextField(fc.getIndexFieldName(), stringValue, Field.Store.NO));
			}
			if (fc.isStored()) {
				document.add(new StoredField(fc.getIndexFieldName(), stringValue));
			}
		} else if (value instanceof Integer numberValue) {
			document.add(new IntPoint(fc.getIndexFieldName(), numberValue));
			if (fc.isStored()) {
				document.add(new StoredField(fc.getIndexFieldName(), numberValue));
			}
		} else if (value instanceof Long numberValue) {
			document.add(new LongPoint(fc.getIndexFieldName(), numberValue));
			if (fc.isStored()) {
				document.add(new StoredField(fc.getIndexFieldName(), numberValue));
			}
		} else if (value instanceof Float numberValue) {
			document.add(new FloatPoint(fc.getIndexFieldName(), numberValue));
			if (fc.isStored()) {
				document.add(new StoredField(fc.getIndexFieldName(), numberValue));
			}
		} else if (value instanceof Double numberValue) {
			document.add(new DoublePoint(fc.getIndexFieldName(), numberValue));
			if (fc.isStored()) {
				document.add(new StoredField(fc.getIndexFieldName(), numberValue));
			}
		} else if (value instanceof Date dateValue) {
			String formattedValue;
			if (fc.getDateFormatter() != null) {
				formattedValue = fc.getDateFormatter().format(dateValue.toInstant().atZone(ZoneId.of("UTC")));
			} else {
				formattedValue = configuration.getDefaultDateFormatter().format(dateValue.toInstant().atZone(ZoneId.of("UTC")));
			}
			document.add(new StringField(fc.getIndexFieldName(), formattedValue, Field.Store.NO));
			
			if (fc.isStored()) {
				document.add(new StoredField(fc.getIndexFieldName(), formattedValue));
			}
		}
	}

}
