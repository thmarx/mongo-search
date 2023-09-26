
# mongo-search

mongo-search keeps your search index in sync with any mongo database.
For this it uses mongodb change streams, which are only available in the replica sets.
For more information of mongo change stream [read here](https://www.mongodb.com/docs/manual/changeStreams/).

# mongo-search-service

If you're looking for a command line tool instead of implementing it yourself:
[mongo-search-service](https://github.com/thmarx/monog-search-service).

## Supported operations

Adding, updating and deleting documents is supported.
Deleting a collection or a database results in the relevant documents being deleted from the search index.

## Search index implementations

There are different adapters available for different search indices.
Lucene, ElasticSearch, OpenSearch and Apache Solr are supported.

### Lucene adapter

The Lucene adapter synchronizes the changes into a local Lucene index.

### ElasticSearch adapter

The ElasticSearch adapter synchronizes the changes to a remote ElasticSearch instance.
Management of the schema must be done externally by an administrator and is not handled by mongo-search.

### OpenSearch adapter

The OpenSearch adapter synchronizes the changes to a remote OpenSearch instance.
Management of the schema must be done externally by an administrator and is not handled by mongo-search.

### Apache Solr adapter

The Solr adapter synchronizes the changes to a remote Solr instance.
Management of the schema must be done externally by an administrator and is not handled by mongo-search.

## Examples

### lucene

```java

LuceneIndexConfiguration configuration = new LuceneIndexConfiguration();

facetConfig.setMultiValued("tags", true);
PerFieldAnalyzerWrapper perFieldAnalyzerWrapper = new PerFieldAnalyzerWrapper(new StandardAnalyzer(),
		Map.of("name", new GermanAnalyzer()));
configuration.setDefaultAnalyzer(perFieldAnalyzerWrapper);				// configure the analyzer
configuration.setCommitDelaySeconds(1);									// delay in seconds to commit changes
configuration.setStorage(new FileSystemStorage(Path.of("/tmp/index"))); // config the storage
configuration.setDefaultFacetsConfig(facetConfig);						// add a facet config if necessary
configuration.setDocumentExtender((context, source, target) -> {		// extending a document with custom fields
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

## Actions

mongo-search makes use of mongodb ChangeStream feature to add new documents to the index.
So in some situations you may have the need to index all documents. 
This may be the case mongo-search was down for a longer time and the mongodb oplog doesn't contain all changes.
Here you can use the IndexCollectionsAction.

### IndexCollectionsAction

The IndexCollectionsAction iterates over all documents in the list of collections and adds them to the index.
```java
mongoSearch.execute(new IndexCollectionsAction(List.of("collectionA", "collectionB")));
```