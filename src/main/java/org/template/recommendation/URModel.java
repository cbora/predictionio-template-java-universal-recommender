package org.template.recommendation;

//type UserID = String
//type ActionID = String
//type ItemID = String
//type ItemProps = Map[String, JValue]

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.elasticsearch.common.joda.time.DateTime;
import org.json4s.JsonAST;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

/** Universal Recommender models to save in ES */
public class URModel {
    // TODO: use real IndexedDataSet / IndexedDataSetSpark
    private class IndexedDataSet {}
    private class IndexedDataSetSpark extends IndexedDataSet {
        public JavaPairRDD<String, Map<String,JsonAST.JValue>> toStringMapRDD(String s) {
            return null;
        }
    }

    private transient static final Logger logger = LoggerFactory.getLogger(URModel.class);

    private final List<Tuple2<String, IndexedDataSet>> coocurrenceMatrices;
    private final List<JavaPairRDD<String, Map<String,JsonAST.JValue>>> propertiesRDDs;
    private final Map<String,String> typeMappings;
    private final boolean nullModel;
    private final SparkContext sc;

    public URModel(
            List<Tuple2<String, IndexedDataSet>> coocurrenceMatrices,
            List<JavaPairRDD<String, Map<String,JsonAST.JValue>>> propertiesRDDs,
            Map<String,String> typeMappings,
            boolean nullModel,
            SparkContext sc) {
        this.coocurrenceMatrices = coocurrenceMatrices;
        this.propertiesRDDs = propertiesRDDs;
        this.typeMappings = typeMappings;
        this.nullModel = nullModel;
        this.sc = sc;
    }

    /** Save all fields to be indexed by Elasticsearch and queried for recs
     *  This will is something like a table with row IDs = item IDs and separate fields for all
     *  cooccurrence and cross-cooccurrence correlators and metadata for each item. Metadata fields are
     *  limited to text term collections so vector types. Scalar values can be used but depend on
     *  Elasticsearch's support. One exception is the Data scalar, which is also supported
     *  @return always returns true since most other reasons to not save cause exceptions
     */
    public boolean save(List<String> dateNames, String esIndex, String esType) {
        logger.debug("Start save model");

        if (nullModel)
            throw new IllegalStateException("Saving a null model created from loading an old one.");

        // for ES we need to create the entire index in an rdd of maps, one per item so we'll use
        // convert cooccurrence matrices into correlators as RDD[(itemID, (actionName, Seq[itemID])]
        // do they need to be in Elasticsearch format
        logger.info("Converting cooccurrence matrices into correlators");

        List<JavaPairRDD<String, Map<String,JsonAST.JValue>>> correlatorRDDs = new LinkedList<>();
        for (Tuple2<String,IndexedDataSet> t : this.coocurrenceMatrices) {
            String actionName = t._1();
            IndexedDataSet dataset = t._2();
            correlatorRDDs.add(((IndexedDataSetSpark) dataset).toStringMapRDD(actionName) );
        }

        logger.info("Group all properties RDD");

        List<JavaPairRDD<String, Map<String,JsonAST.JValue>>> allRDDs = new LinkedList<>();
        allRDDs.addAll(correlatorRDDs);
        allRDDs.addAll(propertiesRDDs);
        JavaPairRDD<String, Map<String,JsonAST.JValue>> groupedRDD = groupAll(allRDDs);

        JavaRDD<Map<String, Object>> esRDD = groupedRDD.mapPartitions(new EsRDDBuilder(dateNames));

        List<String> esFields = esRDD.flatMap(x -> x.keySet()).distinct().collect();

        logger.info("ES fields[" + esFields.size() + "]:" +  esFields);

        EsClient.getInstance().hotSwap(esIndex, esType, esRDD, esFields, typeMappings);
        return true;
    }

    private JavaPairRDD<String, Map<String,JsonAST.JValue>> groupAll(
            List<JavaPairRDD<String, Map<String,JsonAST.JValue>>> fields) {
        // fields.fold(sc.emptyRDD[(ItemID, ItemProps)])(_ ++ _).reduceByKey(_ ++ _)
        return null;
    }

    private static Object extractJvalue(List<String> dateNames, String key, Object value) {
        if (value instanceof JsonAST.JArray) {
            List<Object> list = new LinkedList<>();
            scala.collection.Iterator<Object> iter = ((JsonAST.JArray) value).values().iterator();
            while (iter.hasNext())
                    list.add(extractJvalue(dateNames, key, iter.next()));
            return list;
        }
        else if (value instanceof JsonAST.JString) {
            String s = ((JsonAST.JString) value).s();
            if (dateNames.contains(key)) {
                return new DateTime(s).toDate();
            }
            else if (RankingFieldName.toList().contains(key)) {
                return Double.parseDouble(s);
            }
            else {
                return s;
            }
        }
        else if (value instanceof JsonAST.JDouble) {
            return ((JsonAST.JDouble) value).num();
        }
        else if (value instanceof JsonAST.JInt) {
            return ((JsonAST.JInt) value).num();
        }
        else if (value instanceof JsonAST.JBool) {
            return ((JsonAST.JBool) value).value();
        }
        else {
            return value;
        }
    }

    private class EsRDDBuilder implements FlatMapFunction
            <Iterator<Tuple2<String, Map<String, JsonAST.JValue>>>, Map<String, Object>> {

        private List<String> dateNames;

        public EsRDDBuilder(List<String> dateNames) {
            this.dateNames = dateNames;
        }

        @Override
        public Iterable<Map<String, Object>> call(Iterator<Tuple2<String, Map<String, JsonAST.JValue>>> iter) {
            List<Map<String, Object>> result = new LinkedList<>();
            while(iter.hasNext()) {
                Tuple2<String, Map<String, JsonAST.JValue>> t = iter.next();
                String itemId = t._1();
                Map<String, JsonAST.JValue> itemProps = t._2();
                Map<String,Object> propsMap = new HashMap<>();

                for (Map.Entry<String, JsonAST.JValue> entry : itemProps.entrySet()) {
                    String propName = entry.getKey();
                    JsonAST.JValue propValue = entry.getValue();
                    propsMap.put(propName, URModel.extractJvalue(dateNames, propName, propValue));
                }
                propsMap.put("id", itemId);
                result.add(propsMap);
            }
            return result;
        }
    }
}
















