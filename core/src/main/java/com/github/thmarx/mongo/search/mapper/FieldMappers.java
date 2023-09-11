package com.github.thmarx.mongo.search.mapper;

import java.util.Date;

import org.bson.Document;

/**
 *
 * @author t.marx
 */
public class FieldMappers extends BaseFieldMapper {

	public static String toNull (final String name, final Document document) {
		return null;
	}

	public static String toString (final String name, final Document document) {
		return getFieldValue(name, document, String.class);
	}
	
	public static Date toDate (final String name, final Document document) {
		return getFieldValue(name, document, Date.class);
	}

	public static Boolean toBoolean (final String name, final Document document) {
		return getFieldValue(name, document, Boolean.class);
	}

	public static Integer toInteger (final String name, final Document document) {
		return getFieldValue(name, document, Integer.class);
	}

	public static Long toLong (final String name, final Document document) {
		return getFieldValue(name, document, Long.class);
	}

	public static Float toFloat (final String name, final Document document) {
		return getFieldValue(name, document, Float.class);
	}

	public static Double toDouble (final String name, final Document document) {
		return getFieldValue(name, document, Double.class);
	}

	
}
