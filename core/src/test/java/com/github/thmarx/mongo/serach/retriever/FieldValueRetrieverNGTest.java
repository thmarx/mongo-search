/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/UnitTests/EmptyTestNGTest.java to edit this template
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
public class FieldValueRetrieverNGTest {

	@Test
	public void test_simple_string() {
		Document document = new Document();
		document.put("name", "Hallo Leute");

		String value = FieldValueRetrievers.getStringFieldValue("name", document);

		Assertions.assertThat(value).isEqualTo("Hallo Leute");
	}

	@Test
	public void test_embedded_string() {
		Document document = new Document();
		document.put("name", "Hallo Leute");

		Document emb = new Document();
		emb.put("name", "Hallo ihr da draußen");
		document.put("emb", emb);

		String value = FieldValueRetrievers.getStringFieldValue("emb.name", document);
		Assertions.assertThat(value).isEqualTo("Hallo ihr da draußen");
	}

	@Test
	public void test_simple_string_array() {
		Document document = new Document();
		document.put("names", List.of("eins", "zwei"));

		List<String> value = FieldValueRetrievers.getStringArrayFieldValue("names", document);

		Assertions.assertThat(value).containsExactly("eins", "zwei");
	}

	@Test
	public void test_embedded_string_array() {
		Document document = new Document();
		document.put("name", "Hallo Leute");

		Document emb = new Document();
		emb.put("names", List.of("eins", "zwei"));
		document.put("emb", emb);

		List<String> value = FieldValueRetrievers.getStringArrayFieldValue("emb.names", document);
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

		List<String> value = FieldValueRetrievers.getStringArrayFieldValue("emb.cities.name", document);
		Assertions.assertThat(value).containsExactly("bochum", "essen", "dortmund");
	}

}
