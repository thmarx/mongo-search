/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package de.marx_software.mongo.search.retriever;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.bson.Document;

/**
 *
 * @author t.marx
 */
public class FieldValueRetrievers {

	public static String getStringFieldValue (final String name, final Document document) {
		var names = name.split("\\.");
		
		if (names.length > 1) {
			return document.getEmbedded(Arrays.asList(names), String.class);
		} else {
			return document.getString(names[0]);
		}
	}
	
	public static List<String> getStringArrayFieldValue (final String name, final Document document) {
		var names = name.split("\\.");
		
		if (names.length > 1) {
			return getValues(name, document, String.class);
		} else {
			return document.getList(names[0], String.class);
		}
	}
	
	static <T> List<T> getValues (final String query, final Document document, final Class<T> type) {
		var parts = query.split("\\.");
		if (parts.length == 1) {
			var value = document.get(query);
			if (value instanceof List list) {
				return list;
			} else {
				return List.of(type.cast(value));
			}
		}
		
		var subquery = query.replace(parts[0] + ".", "");
		
		List values = new ArrayList<>();
		Object temp = document.get(parts[0]);
		if (temp instanceof Document doc) {
			values.addAll(getValues(subquery, doc, type));
		} else if (temp instanceof List list) {
			list.forEach((element) -> {
				if (element instanceof Document doc) {
					values.addAll(getValues(subquery, doc, type));
				}
			});
		} else if (temp instanceof String) {
			
		} else if (temp instanceof Date) {
			
		} else if (temp instanceof Double) {
			
		} else if (temp instanceof Integer) {
			
		} else if (temp instanceof Long) {
			
		}
		
		return values;
	}
}
