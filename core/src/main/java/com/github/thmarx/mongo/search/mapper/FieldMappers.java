package com.github.thmarx.mongo.search.mapper;

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
