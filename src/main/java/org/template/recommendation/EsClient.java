package org.template.recommendation;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.predictionio.data.storage.elasticsearch.StorageClient;
import org.apache.predictionio.data.store.java.OptionHelper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.EmptyRDD;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.UnmodifiableIterator;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.search.SearchHit;
import org.json4s.JsonAST;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.predictionio.data.storage.*;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.spark.rdd.EsSpark;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.joda.time.DateTime;
import org.json4s.jackson.JsonMethods.*;
//import org.elasticsearch.spark.*;
import org.elasticsearch.node.NodeBuilder.*;
import org.elasticsearch.search.SearchHits;
import scala.Double;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//import org.json4s.JValue;

/** Elasticsearch notes:
 *  1) every query clause will affect scores unless it has a constant_score and boost: 0
 *  2) the Spark index writer is fast but must assemble all data for the index before the write occurs
 *  3) many operations must be followed by a refresh before the action takes effect--sortof like a transaction commit
 *  4) to use like a DB you must specify that the index of fields are `not_analyzed` so they won't be lowercased,
 *    stemmed, tokenized, etc. Then the values are literal and must match exactly what is in the query (no analyzer)
 */
public final class EsClient {
    private transient static Logger logger = LoggerFactory.getLogger(EsClient.class);
    private static TransportClient client = null;

    private static final EsClient INSTANCE = new EsClient();

    private EsClient() {
    }

    public static EsClient getInstance() {
        if (client == null) {
            if (Storage.getConfig("ELASTICSEARCH").nonEmpty())
                client = new StorageClient(Storage.getConfig("ELASTICSEARCH").get()).client();
            else
                throw new IllegalStateException(
                        "No Elasticsearch client configuration detected, check your pio-env.sh for " +
                                "proper configuration settings");
        }

        return INSTANCE;
    }

    /** Delete all data from an instance but do not commit it. Until the "refresh" is done on the index
     *  the changes will not be reflected.
     *  @param indexName will delete all types under this index, types are not used by the UR
     *  @param refresh should the index be refreshed so the create is committed
     *  @return true if all is well
     */
    public boolean deleteIndex(String indexName, boolean refresh) {
        if (client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists()) {
            DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
            if (!delete.isAcknowledged()) {
                logger.info("Index " + indexName + " wasn't deleted, but may have quietly failed.");
            } else {
                // now refresh to get it 'committed'
                // todo: should do this after the new index is created so no index downtime
                if (refresh) {refreshIndex(indexName);}
            }

            return true;
        } else {
            logger.warn("Elasticsearch index: " + indexName +
                    " wasn't deleted because it didn't exist. This may be an error.");
            return false;
        }
    }

    /** Creates a new empty index in Elasticsearch and initializes mappings for fields that will be used
     *  @param indexName elasticsearch name
     *  @param indexType names the type of index, usually use the item name
     *  @param fieldNames ES field names
     *  @param typeMappings indicates which ES fields are to be not_analyzed without norms
     *  @param refresh should the index be refreshed so the create is committed
     *  @return true if all is well
     */
    public boolean createIndex(
            String indexName,
            String indexType,
            List<String> fieldNames,
            Map<String, String> typeMappings,
            boolean refresh) {

        if (!client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists()) {
            StringBuilder mappings = new StringBuilder();

            String mappingsHead = "" +
                    "{" +
                    "  \"properties\": {";
            mappings.append(mappingsHead);

            for (String fieldName : fieldNames) {
                mappings.append(fieldName);
                if (typeMappings.containsKey(fieldName))
                    mappings.append(mappingsField(typeMappings.get(fieldName)));
                else // unspecified fields are treated as not_analyzed strings
                    mappings.append(mappingsField("string"));
            }

            String mappingsTail = "" +
                    "    \"id\": {" +
                    "      \"type\": \"string\"," +
                    "      \"index\": \"not_analyzed\"," +
                    "      \"norms\" : {" +
                    "        \"enabled\" : false" +
                    "      }" +
                    "    }" +
                    "  }" +
                    "}";
            mappings.append(mappingsTail); // any other string is not_analyzed

            CreateIndexRequest cir = new CreateIndexRequest(indexName).mapping(indexType, mappings);
            CreateIndexResponse create = client.admin().indices().create(cir).actionGet();
            if (!create.isAcknowledged()) {
                logger.info("Index " + indexName + " wasn't created, but may have quietly failed.");
            }
            else {
                // now refresh to get it 'committed'
                // todo: should do this after the new index is created so no index downtime
                if (refresh) {refreshIndex(indexName);}
            }

            return true;
        } else {
            logger.warn("Elasticsearch index: " +  indexName + " wasn't created because it already exists. This may be an error.");
            return false;
        }
    }

    /** Commits any pending changes to the index */
    public void refreshIndex(String indexName) {
        client.admin().indices().refresh(new RefreshRequest(indexName)).actionGet();
    }

    private String mappingsField(String type) {
        return "" +
                "    : {" +
                "      \"type\": \"" + type + "\"," +
                "      \"index\": \"not_analyzed\"," +
                "      \"norms\" : {" +
                "        \"enabled\" : false" +
                "       }" +
                "    },";
    }

    /**
     * Create a new index and hot-swap the new after it's indexed and ready to take over, then delete the old
     * @param alias
     * @param typeName
     * @param indexRDD
     * @param fieldNames
     * @param typeMappings
     */
    public void hotSwap(String alias, String typeName, JavaRDD<Map<String, Object>> indexRDD, List<String> fieldNames, Map<String, String> typeMappings){
        // get index for alias, change a char, create new one with new id and index it, swap alias and delete old
        ImmutableOpenMap<String, List<AliasMetaData>> aliasMetadata = client.admin().indices().prepareGetAliases(alias).get().getAliases();
        String newIndex = alias + "_" + String.valueOf(DateTime.now().getMillis());

        logger.debug("Create new index: " + newIndex + ",  " + typeName + ", "+ fieldNames + ", "+ typeMappings);
        boolean refresh = false;
        boolean response = createIndex(newIndex, typeName, fieldNames, typeMappings, refresh);

        String newIndexURI = "/" + newIndex + "/" + typeName;

        Map<String, String> m = new HashMap<String, String>();
        m.put("es.mapping.id", "id");
        EsSpark.saveToEs(indexRDD, newIndexURI, scala.collection.JavaConverters.mapAsScalaMap(m));

        if((!aliasMetadata.isEmpty()) && (aliasMetadata.get(alias) != null)
                && (aliasMetadata.get(alias).get(0) != null)){ // was alias so remove the old one
            // append the DateTime to the alias to create an index name
            String oldIndex = aliasMetadata.get(alias).get(0).getIndexRouting();
            client.admin().indices().prepareAliases()
                    .removeAlias(oldIndex, alias).addAlias(newIndex, alias)
                    .execute().actionGet();
            deleteIndex(oldIndex, refresh); // now can safely delete the old one since it's not used
        }
        else {
            // to-do: could be more than one index with 'alias' so no alias so add one
            // to clean up any indexes that exist with the alias name
            String indices = client.admin().indices().prepareGetIndex().get().indices()[0];
            if (indices.contains(alias)){
                deleteIndex(alias, refresh);
            }
            client.admin().indices()
                    .prepareAliases()
                    .addAlias(newIndex, alias)
                    .execute().actionGet();
        }
        // clean out any old index that were the product of a failed train
        String indices = client.admin().indices().prepareGetIndex().get().indices()[0];


        if (indices.contains(alias) && indices != newIndex){
            deleteIndex(indices, false);
        }
    }

    /**
     * Performs a search using the JSON query String
     * @param query: the JSON query string parable by ElasticSearch
     * @param indexName: the index to search
     * @return a [PredictedResults] collection
     */
    public SearchHits search(String query, String indexName){
        SearchResponse sr = client.prepareSearch(indexName).setSource(query).get();

        return sr.isTimedOut() ? null : sr.getHits();
    }

    /**
     * Gets the "source" field of an Elasticsearch document
     * @param indexName index that contains the doc/item
     * @param typeName type name used to construct ES REST URI
     * @param doc for the UR item id
     * @return source Map<String, Object>  of field names to any valid field values or null if empty
     */
    public Map<String, Object> getSource(String indexName, String typeName, String doc){
        return client.prepareGet(indexName, typeName, doc).execute().actionGet().getSource();
    }


    public String getIndexName(String alias){
         ImmutableOpenMap<String, List<AliasMetaData>> allIndicesMap = client.admin().indices().getAliases(new GetAliasesRequest(alias)).actionGet().getAliases();

         if( allIndicesMap.size() == 1) { // must be a 1-1 mapping of alias <-> index
             String indexName = "";
             UnmodifiableIterator<String> itr = allIndicesMap.keysIt();
             while (itr.hasNext()) {
                 indexName = itr.next();
             }
             // the one index that alias points to
             return indexName.equals("") ? null : indexName;
         }else {
             // delete all the indices that are pointed to by the alias, they can't be used
             logger.warn("There is no 1-1 mapping of index to alias so deleting the old indexes that are reference by\"" +
                    "alias. This may have been caused by a crashed or stopped \"pio train\" operation so try running it again");
             if ( !allIndicesMap.isEmpty() ){
                 boolean refresh = true;
                 for (ObjectCursor<String> indexName : allIndicesMap.keys()){

                     deleteIndex(indexName.value, refresh);
                 }
             }
             return null; // if more than one abort, need to clean up bad aliases
         }
    }


    public JavaPairRDD<String, Map<String, JsonAST.JValue>> getRDD(String alias, String typeName, SparkContext sc) {
        String indexName = getIndexName(alias);
        if (indexName == null || indexName.equals("")){
            return null;
        }
        else {
            String resource = alias + "/" + typeName;
            JavaRDD<Tuple2<String, String>> tmp = EsSpark.esJsonRDD(sc, resource).toJavaRDD();
            JavaPairRDD<String,String> esrdd = JavaPairRDD.<String,String>fromJavaRDD(tmp);

            JavaPairRDD<String, Map<String, JsonAST.JValue>> rtn = esrdd.<String,Map<String,JsonAST.JValue>>mapToPair(t ->
                    new Tuple2<String, Map<String, JsonAST.JValue>> (t._1(),
                            JavaConverters.mapAsJavaMapConverter(
                                    DataMap.apply(t._2())
                                            .fields()).asJava()));
            return rtn;
        }
    }
}