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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.bson.Document;

/**
 *
 * @author t.marx
 */
public class ListFieldMappers extends BaseFieldMapper {

	public static List<String> getStringArrayFieldValue (final String name, final Document document) {
		return getArrayFieldValue(name, document, String.class);
	}
	
	
	public static List<Integer> getIntegerArrayFieldValue (final String name, final Document document) {
		return getArrayFieldValue(name, document, Integer.class);
	}

	public static List<Long> getLongArrayFieldValue (final String name, final Document document) {
		return getArrayFieldValue(name, document, Long.class);
	}
	
	public static List<Float> getFloatArrayFieldValue (final String name, final Document document) {
		return getArrayFieldValue(name, document, Float.class);
	}
	
	public static List<Double> getDoubleArrayFieldValue (final String name, final Document document) {
		return getArrayFieldValue(name, document, Double.class);
	}

}
