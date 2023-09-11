package com.github.thmarx.mongo.search.mapper;

import java.util.Date;

import org.bson.Document;

/**
 *
 * @author t.marx
 */
public class FieldMappers extends BaseFieldMapper {

	public static String getNullFieldValue (final String name, final Document document) {
		return null;
	}

	public static String getStringFieldValue (final String name, final Document document) {
		return getFieldValue(name, document, String.class);
	}
	
	public static Date getDataFieldValue (final String name, final Document document) {
		return getFieldValue(name, document, Date.class);
	}

	public static Integer getIntegerFieldValue (final String name, final Document document) {
		return getFieldValue(name, document, Integer.class);
	}

	public static Long getLongFieldValue (final String name, final Document document) {
		return getFieldValue(name, document, Long.class);
	}

	public static Float getFloatFieldValue (final String name, final Document document) {
		return getFieldValue(name, document, Float.class);
	}

	public static Double getDoubleFieldValue (final String name, final Document document) {
		return getFieldValue(name, document, Double.class);
	}

	
}
