/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/UnitTests/EmptyTestNGTest.java to edit this template
 */
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

import com.github.thmarx.mongo.search.mapper.FieldMappers;
import com.mongodb.BasicDBObject;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.bson.Document;
import static org.testng.Assert.*;
import org.testng.annotations.Test;

/**
 *
 * @author t.marx
 */
public class FieldMapperNGTest {

	@Test
	public void test_simple() {
		Document document = new Document();
		document.put("name", "Hallo Leute");
		document.put("int", 45);
		document.put("float", 4.5f);
		document.put("long", 45l);
		document.put("double", 45.5d);

		Assertions.assertThat(
			FieldMappers.getStringFieldValue("name", document)
		).isEqualTo("Hallo Leute");
	
		Assertions.assertThat(
			FieldMappers.getIntegerFieldValue("int", document)
		).isEqualTo(45);
		Assertions.assertThat(
			FieldMappers.getFloatFieldValue("float", document)
		).isEqualTo(4.5f);
		Assertions.assertThat(
			FieldMappers.getLongFieldValue("long", document)
		).isEqualTo(45l);
		Assertions.assertThat(
			FieldMappers.getDoubleFieldValue("double", document)
		).isEqualTo(45.5d);
	}

	@Test
	public void test_embedded_string() {
		Document document = new Document();
		document.put("name", "Hallo Leute");

		Document emb = new Document();
		emb.put("name", "Hallo ihr da draußen");
		document.put("emb", emb);

		String value = FieldMappers.getStringFieldValue("emb.name", document);
		Assertions.assertThat(value).isEqualTo("Hallo ihr da draußen");
	}

	@Test
	public void test_simple_string_array() {
		Document document = new Document();
		document.put("names", List.of("eins", "zwei"));

		List<String> value = FieldMappers.getStringArrayFieldValue("names", document);

		Assertions.assertThat(value).containsExactly("eins", "zwei");
	}

	@Test
	public void test_embedded_string_array() {
		Document document = new Document();
		document.put("name", "Hallo Leute");

		Document emb = new Document();
		emb.put("names", List.of("eins", "zwei"));
		document.put("emb", emb);

		List<String> value = FieldMappers.getStringArrayFieldValue("emb.names", document);
		Assertions.assertThat(value).containsExactly("eins", "zwei");
	}

	@Test
	public void test_embedded_string_object_array() {
		Document document = new Document();
		document.put("name", "Hallo Leute");

		Document emb = new Document();
		emb.put("cities", List.of(
				new Document(Map.of("name", "bochum")),
				new Document(Map.of("name", "essen")),
				new Document(Map.of("name", "dortmund"))
		));
		document.put("emb", emb);

		List<String> value = FieldMappers.getStringArrayFieldValue("emb.cities.name", document);
		Assertions.assertThat(value).containsExactly("bochum", "essen", "dortmund");
	}

}
