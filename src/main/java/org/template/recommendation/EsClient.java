package org.template.recommendation;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.predictionio.data.storage.elasticsearch.StorageClient;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
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
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.joda.time.DateTime;
import org.json4s.jackson.JsonMethods.*;
//import org.elasticsearch.spark.*;
import org.elasticsearch.node.NodeBuilder.*;
import org.elasticsearch.search.SearchHits;

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
}



