
# mongo-connect

mongo-search hilft dabei deine mongodb datenbank und einen SuchIndex synchron zu halten. 
Änderungen in der Datenbank werden dabei nach einer bestimmten Konfiguration in den SuchIndex übernommen.

## Unterstütze Operationen

Das Hinzufügen, Aktualisieren und Löschen von Dokumenten wird unterstützt.
Das löschen einer Collection oder einer Datenbank führt zum löschen der jeweiligen Dokumente aus dem SuchIndex.

## SuchIndex Implementierungen

Es stehen verschiedene ADapter für unterschiedliche SuchIndicies zu Verfügung.
Untestützt werden Lucene, ElasticSearch, OpenSearch und Apache Solr.

### Lucene Adapter

Der Lucene Adapter synchronisert die Änderungen in einen lokalen Lucene Index.

### ElasticSearch Adapter

Der ElasticSearch Adapter synchronisiert die Änderungen in eine remote ElasticSearch Instanz.
Die Verwaltung des Schemas muss extern über einen Administrator erfolgen und wird nicht von mongo-search übernommen.

### OpenSearch Adapter

Der OpenSearch Adapter synchronisiert die Änderungen in eine remote OpenSearch Instanz.
Die Verwaltung des Schemas muss extern über einen Administrator erfolgen und wird nicht von mongo-search übernommen.

### Apache Solr Adapter

Der OpenSearch Adapter synchronisiert die Änderungen in eine remote OpenSearch Instanz.
Die Verwaltung des Schemas muss extern über einen Administrator erfolgen und wird nicht von mongo-search übernommen.

## Examples

### lucene

```java

LuceneIndexConfiguration configuration = new LuceneIndexConfiguration();

facetConfig.setMultiValued("tags", true);
PerFieldAnalyzerWrapper perFieldAnalyzerWrapper = new PerFieldAnalyzerWrapper(new StandardAnalyzer(),
		Map.of("name", new GermanAnalyzer()));
configuration.setAnalyzer(perFieldAnalyzerWrapper);						// configure the analyzer
configuration.setCommitDelaySeconds(1);									// delay in seconds to commit changes
configuration.setStorage(new FileSystemStorage(Path.of("/tmp/index"))); // config the storage
configuration.setFacetsConfig(facetConfig);								// add a facet config if necessary
configuration.setDocumentExtender((source, target) -> {					// extending a document with custom fields
	var values = ListFieldMappers.toString("tags", source);
	if (values != null && !values.isEmpty()) {
		values.forEach(value -> {
			target.add(new SortedSetDocValuesFacetField("tags", value));
		});
	}
});

configuration.addFieldConfiguration("documents-collection", LuceneFieldConfiguration.builder()
		.fieldName("name")
		.indexFieldName("name")
		.stored(true)
		.mapper(FieldMappers::toString)
		.build());

configuration.addFieldConfiguration("documents-collection", LuceneFieldConfiguration.builder()
		.fieldName("tags")
		.indexFieldName("tags")
		.stored(true)
		.keyword(true)
		.mapper(ListFieldMappers::toString)
		.build());

var luceneIndexAdapter = new LuceneIndexAdapter(configuration);
luceneIndexAdapter.open();

mongoSearch = new MongoSearch();
mongoSearch.open(luceneIndexAdapter, database, List.of("dokumente", "bilder"));
mongoSearch.execute(new InitializeCommand(List.of("dokumente", "bilder")));

var searcher = luceneIndexAdapter.getIndex().getSearcherManager().acquire()
try {
} finally {
	luceneIndexAdapter.getIndex().getSearcherManager().release(searcher)
}
```

### ElasticSearch

```java
ElasticsearchIndexConfiguration configuration = new ElasticsearchIndexConfiguration();
configuration.addFieldConfiguration(COLLECTION_DOKUMENTE, FieldConfiguration.builder()
		.fieldName("name")
		.indexFieldName("name")
		.mapper(FieldMappers::toString)
		.build()
);
configuration.addFieldConfiguration(COLLECTION_DOKUMENTE, FieldConfiguration.builder()
		.fieldName("tags")
		.indexFieldName("tags")
		.mapper(ListFieldMappers::toString)
		.build()
);
configuration.addFieldConfiguration(COLLECTION_DOKUMENTE, FieldConfiguration.builder()
		.fieldName("cities.name")
		.indexFieldName("cities")
		.mapper(ListFieldMappers::toString)
		.build()
);

indexAdapter = new ElasticsearchIndexAdapter(configuration);
indexAdapter.open(esClient);

mongoSearch = new MongoSearch();
mongoSearch.open(indexAdapter, database, List.of(COLLECTION_DOKUMENTE));
```

### OpenSearch

```java
OpensearchIndexConfiguration configuration = new OpensearchIndexConfiguration();
configuration.addFieldConfiguration(COLLECTION_DOKUMENTE, FieldConfiguration.builder()
		.fieldName("name")
		.indexFieldName("name")
		.mapper(FieldMappers::toString)
		.build()
);
configuration.addFieldConfiguration(COLLECTION_DOKUMENTE, FieldConfiguration.builder()
		.fieldName("tags")
		.indexFieldName("tags")
		.mapper(ListFieldMappers::toString)
		.build()
);
configuration.addFieldConfiguration(COLLECTION_DOKUMENTE, FieldConfiguration.builder()
		.fieldName("cities.name")
		.indexFieldName("cities")
		.mapper(ListFieldMappers::toString)
		.build()
);

indexAdapter = new OpensearchIndexAdapter(configuration);
indexAdapter.open(osClient);

mongoSearch = new MongoSearch();
mongoSearch.open(indexAdapter, database, List.of(COLLECTION_DOKUMENTE));
```

### OpenSearch

```java
SolrIndexConfiguration configuration = new SolrIndexConfiguration();
configuration.addFieldConfiguration(COLLECTION_DOKUMENTE, FieldConfiguration.builder()
		.fieldName("name")
		.indexFieldName("name")
		.mapper(FieldMappers::toString)
		.build()
);
configuration.addFieldConfiguration(COLLECTION_DOKUMENTE, FieldConfiguration.builder()
		.fieldName("tags")
		.indexFieldName("tags")
		.mapper(ListFieldMappers::toString)
		.build()
);
configuration.addFieldConfiguration(COLLECTION_DOKUMENTE, FieldConfiguration.builder()
		.fieldName("cities.name")
		.indexFieldName("cities")
		.mapper(ListFieldMappers::toString)
		.build()
);

indexAdapter = new SolrIndexAdapter(configuration);
indexAdapter.open(osClient);

mongoSearch = new MongoSearch();
mongoSearch.open(indexAdapter, database, List.of(COLLECTION_DOKUMENTE));
```

## Commands

In manchen Situationen ist es nötig

### InitializeCommand
The InitializeCommand iterates over all documents in the list of collections and adds them to the index.
```java
mongoSearch.executeCommand(new InitializeCommand(List.of("collectionA", "collectionB")));
```

### ReIndexCollectionCommand
The ReIndexCollectionCommand is used to index only documents of a single collection.
```java
mongoSearch.executeCommand(new ReIndexCollectionCommand("collectionA"));
```