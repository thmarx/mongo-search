/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package de.marx_software.mongo.search.adapters.lucene.index.storage;

/*-
 * #%L
 * mongo-search-index
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

import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 *
 * @author t.marx
 */
public class FileSystemStorage implements Storage {

	private Directory directory;
	
	private final Path path;
	
	public FileSystemStorage (final Path path) {
		this.path = path;
	}
	
	@Override
	public Directory getDirectory() {
		return directory;
	}

	@Override
	public void open() throws IOException {
		directory = FSDirectory.open(path);
	}

	@Override
	public void close() throws Exception {
		this.directory.close();
	}
	
}
