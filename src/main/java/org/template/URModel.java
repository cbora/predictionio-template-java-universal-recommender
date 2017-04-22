package org.template;

import com.google.common.collect.Iterables;
import lombok.AllArgsConstructor;
import org.apache.mahout.math.*;
import org.apache.mahout.math.drm.CheckpointedDrm;
import org.apache.mahout.math.indexeddataset.IndexedDataset;
import org.apache.mahout.sparkbindings.drm.CheckpointedDrmSparkOps;
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.elasticsearch.common.joda.time.DateTime;
import org.json4s.JsonAST;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.template.indexeddataset.IndexedDatasetJava;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.util.*;

/** Universal Recommender models to save in ES */
@AllArgsConstructor
public class URModel {

    private transient static final Logger logger = LoggerFactory.getLogger(URModel.class);

    private final List<Tuple2<String, IndexedDataset>> coocurrenceMatrices;
    private final List<JavaPairRDD<String, HashMap<String,JsonAST.JValue>>> propertiesRDDs;
    private final Map<String,String> typeMappings;
    private final boolean nullModel;
    private final SparkContext sc;

    /** Save all fields to be indexed by Elasticsearch and queried for recs
     *  This will is something like a table with row IDs = item IDs and separate fields for all
     *  cooccurrence and cross-cooccurrence correlators and metadata for each item. Metadata fields are
     *  limited to text term collections so vector types. Scalar values can be used but depend on
     *  Elasticsearch's support. One exception is the Data scalar, which is also supported
     *  @return always returns true since most other reasons to not save cause exceptions
     */
    public boolean save(final List<String> dateNames, String esIndex, String esType) {
        logger.debug("Start save model");

        if (nullModel)
            throw new IllegalStateException("Saving a null model created from loading an old one.");

        // for ES we need to create the entire index in an rdd of maps, one per item so we'll use
        // convert cooccurrence matrices into correlators as RDD[(itemID, (actionName, Seq[itemID])]
        // do they need to be in Elasticsearch format
        logger.info("Converting cooccurrence matrices into correlators");

        final List<JavaPairRDD<String, HashMap<String,JsonAST.JValue>>> correlatorRDDs = new LinkedList<>();

        for (Tuple2<String,IndexedDataset> t : this.coocurrenceMatrices) {
            final String actionName = t._1();
            final IndexedDataset dataset = t._2();


            /** Chris Fifty sanity check */
            /**
             * Size of teh vector: 5,5,0,5,6,2,4 and RDD size = 7...


                CheckpointedDrm temp = dataset.matrix();
                CheckpointedDrmSparkOps temp2 = new CheckpointedDrmSparkOps(temp);
                JavaPairRDD<Integer, org.apache.mahout.math.Vector> temp3 = JavaPairRDD.fromJavaRDD(temp2.rdd().toJavaRDD());
                System.out.println("here is the RDD size " + temp3.rdd().count());
                temp3.foreach(entry -> {

                    System.out.println("here is the size of the vector.nonZeros " + Iterables.size(entry._2().nonZeroes()));

                });
                if(!nullModel) {
                    throw new IllegalStateException("MUST SEE THIS");
                }

            */
            /** end chris fifty sanity check */

            final IndexedDatasetSpark IDSpark = (IndexedDatasetSpark) dataset;
            final IndexedDatasetJava IDJava = new IndexedDatasetJava(IDSpark);

            /** Chris Fifty sanity check #2 */
            /**
             * size of the vector: 5,4,0,4,5,2,4 and size of the RDD = 7

            CheckpointedDrm temp = IDJava.matrix();
            CheckpointedDrmSparkOps temp2 = new CheckpointedDrmSparkOps(temp);
            JavaPairRDD<Integer, org.apache.mahout.math.Vector> temp3 = JavaPairRDD.fromJavaRDD(temp2.rdd().toJavaRDD());
            System.out.println("here is the RDD size " + temp3.rdd().count());
            temp3.foreach(entry -> {

                System.out.println("here is the size of the vector.nonZeros " + Iterables.size(entry._2().nonZeroes()));

            });
            if(!nullModel) {
                throw new IllegalStateException("MUST SEE THIS");
            }
             */
            /** End chris fifty sanity check 2 */
            final Conversions.IndexedDatasetConversions IDConvert = new Conversions.IndexedDatasetConversions(IDJava);
            correlatorRDDs.add(IDConvert.toStringMapRDD(actionName));
        }

        logger.info("Group all properties RDD");

        final List<JavaPairRDD<String, HashMap<String,JsonAST.JValue>>> allRDDs = new LinkedList<>();
        allRDDs.addAll(correlatorRDDs);
        allRDDs.addAll(propertiesRDDs);

        final JavaPairRDD<String, HashMap<String,JsonAST.JValue>> groupedRDD = groupAll(allRDDs);
        final JavaRDD<Map<String, Object>> esRDD = groupedRDD.mapPartitions(iter ->
                {
                    final List<Map<String, Object>> result = new LinkedList<>();
                    while(iter.hasNext()) {
                        final Tuple2<String, HashMap<String, JsonAST.JValue>> t = iter.next();
                        final String itemId = t._1();
                        final HashMap<String, JsonAST.JValue> itemProps = t._2();
                        final Map<String,Object> propsMap = new HashMap<>();

                        for (Map.Entry<String, JsonAST.JValue> entry : itemProps.entrySet()) {
                            final String propName = entry.getKey();
                            final JsonAST.JValue propValue = entry.getValue();
                            propsMap.put(propName, URModel.extractJvalue(dateNames, propName, propValue));
                        }
                        propsMap.put("id", itemId);
                        result.add(propsMap);
                    }
                    return result;
                }
        );
        if(esRDD == null) {
            throw new IllegalStateException("esRDD is null and this is a big problem");
        }
        final List<String> esFields = esRDD.flatMap(Map::keySet).distinct().collect();

        logger.info("ES fields[" + esFields.size() + "]:" +  esFields);

        EsClient.getInstance().hotSwap(esIndex, esType, esRDD, esFields, typeMappings);
        return true;
    }
    private JavaPairRDD<String, HashMap<String,JsonAST.JValue>> groupAll(
            List<JavaPairRDD<String, HashMap<String,JsonAST.JValue>>> fields) {
        final JavaPairRDD<String,HashMap<String,JsonAST.JValue>> tmp = RDDUtils.unionAllPair(fields,sc);
        return RDDUtils.combineHashMapByKey(tmp);
    }

    private static Object extractJvalue(List<String> dateNames, String key, JsonAST.JValue value) {
        if (value instanceof JsonAST.JArray) {

            final List<Object> list = new LinkedList<>();
            JsonAST.JArray temp = (JsonAST.JArray) value;
            int counter = 0;
            while(counter < temp.values().size()){
                list.add(extractJvalue(dateNames,key,temp.apply(counter)));
                counter++;
            }
            return list;

        } else if (value instanceof JsonAST.JString) {
            final String s = ((JsonAST.JString) value).s();

            if (dateNames.contains(key))
                return new DateTime(s).toDate();
            else if (RankingFieldName.toList().contains(key))
                return Double.parseDouble(s);
            else
                return s;
        } else if (value instanceof JsonAST.JDouble) {
            return ((JsonAST.JDouble) value).num();
        } else if (value instanceof JsonAST.JInt) {
            return ((JsonAST.JInt) value).num();
        } else if (value instanceof JsonAST.JBool) {
            return ((JsonAST.JBool) value).value();
        } else {
            return value;
        }
    }
}
















