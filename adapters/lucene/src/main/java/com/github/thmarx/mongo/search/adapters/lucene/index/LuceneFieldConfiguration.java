package com.github.thmarx.mongo.search.adapters.lucene.index;

import org.bson.Document;

import com.github.thmarx.mongo.search.index.configuration.FieldConfiguration;

import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;


@SuperBuilder
@Getter
public class LuceneFieldConfiguration extends FieldConfiguration<Document, org.apache.lucene.document.Document> {

    @Builder.Default
	private boolean stored = false;
}
