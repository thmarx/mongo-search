package com.github.thmarx.mongo.search.mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;

public abstract class BaseFieldMapper {
    protected static <T> T getFieldValue (final String name, final Document document, Class<T> type) {
		var names = name.split("\\.");
		
		if (names.length > 1) {
			return document.getEmbedded(Arrays.asList(names), type);
		} else {
			return document.get(names[0], type);
		}
	}

	protected static <T> List<T> getArrayFieldValue (final String name, final Document document, final Class<T> type) {
		var names = name.split("\\.");
		
		if (names.length > 1) {
			return getValues(name, document, type);
		} else {
			return document.getList(names[0], type);
		}
	}

	protected static <T> List<T> getValues (final String query, final Document document, final Class<T> type) {
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
		
		List<T> values = new ArrayList<>();
		Object temp = document.get(parts[0]);
		if (temp instanceof Document doc) {
			values.addAll(getValues(subquery, doc, type));
		} else if (temp instanceof List<?> list) {
			list.forEach((element) -> {
				if (element instanceof Document doc) {
					values.addAll(getValues(subquery, doc, type));
				}
			});
		}
		
		return values;
	}
}
