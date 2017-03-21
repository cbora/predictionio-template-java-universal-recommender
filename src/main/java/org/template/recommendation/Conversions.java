package org.template.recommendation;


import javafx.util.Pair;
import org.apache.mahout.math.Vector;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.json4s.JsonAST;
import org.slf4j.Logger;
import org.template.recommendation.indexeddataset.BiDictionaryJava;
import org.template.recommendation.indexeddataset.IndexedDatasetJava;
import scala.Tuple2;
import java.util.*;
import org.slf4j.LoggerFactory;
import org.apache.mahout.sparkbindings.SparkDistributedContext;
import scala.collection.*;
import scala.collection.Map;
import scala.reflect.ClassTag;


/** Utility conversions for IndexedDatasetSpark */
public class Conversions {

    /**
     * Print stylized ActionML title
     * @param logger Logger to print with
     */
    public static void drawActionML(Logger logger) {
        String actionML = "" +
                "\n\t" +
                "\n\t               _   _             __  __ _" +
                "\n\t     /\\       | | (_)           |  \\/  | |" +
                "\n\t    /  \\   ___| |_ _  ___  _ __ | \\  / | |" +
                "\n\t   / /\\ \\ / __| __| |/ _ \\| '_ \\| |\\/| | |" +
                "\n\t  / ____ \\ (__| |_| | (_) | | | | |  | | |____" +
                "\n\t /_/    \\_\\___|\\__|_|\\___/|_| |_|_|  |_|______|" +
                "\n\t" +
                "\n\t";

        logger.info(actionML);
    }

    /**
     * Print informational chart
     * @param title title of information chart
     * @param dataMap list of (key, value) pairs to print as rows
     * @param logger Logger to use for printing
     */
    public static void drawInfo(String title, List<Tuple2<String, Object>> dataMap, Logger logger) {
        String leftAlignFormat = "║ %-30s%-28s ║";

        String line = strMul("═", 60);

        String preparedTitle = String.format("║ %-58s ║", title);

        StringBuilder data = new StringBuilder();
        for (Tuple2<String, Object> t : dataMap) {
            data.append(String.format(leftAlignFormat, t._1, t._2));
            data.append("\n\t");
        }

        String info = "" +
                "\n\t╔" + line + "╗" +
                "\n\t"  + preparedTitle +
                "\n\t"  + data.toString().trim() +
                "\n\t╚" + line + "╝" +
                "\n\t";
        logger.info(info);
    }

    /**
     * Append n copies of str to create new String
     * @param str String to copy
     * @param n How many times to copy
     * @return String created by combining n copies of str
     */
    private static String strMul(String str, int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++)
            sb.append(str);
        return sb.toString();
    }

    public class OptionCollection<T>{
        private final Optional<List<T>> collectionOpt;

        public OptionCollection(Optional<List<T>> collectionOpt) {
            this.collectionOpt = collectionOpt;
        }
        public List<T> getOrEmpty(){
            if(!collectionOpt.isPresent()){
                return new ArrayList<T>();
            }
            return collectionOpt.get();
        }
    }


    public class IndexedDatasetConversions {
        private final IndexedDatasetJava indexedDataset;
        private transient final Logger logger = LoggerFactory.getLogger(IndexedDatasetConversions.class);


        public IndexedDatasetConversions(IndexedDatasetJava indexedDataset) {
            this.indexedDataset = indexedDataset;
        }


        public JavaPairRDD<String,Map<String,JsonAST.JValue>> toStringMapRDD(String actionName){
            BiDictionaryJava rowIDDictionary = indexedDataset.getRowIds();
            SparkDistributedContext temp = (SparkDistributedContext) indexedDataset.getMatrix().context();
            SparkContext sc = temp.sc();

            ClassTag<BiDictionaryJava> tag = scala.reflect.ClassTag$.MODULE$.apply(BiDictionaryJava.class);
            Broadcast<BiDictionaryJava> rowIDDictionary_bcast = sc.broadcast(rowIDDictionary,tag);

            BiDictionaryJava columnIDDictionary = indexedDataset.getColIds();
            Broadcast<BiDictionaryJava> columnIDDictionary_bcast = sc.broadcast(columnIDDictionary,tag);

            // may want to mapPartition and create bulk updates as a slight optimization
            // creates an RDD of (itemID, Map[correlatorName, list-of-correlator-values])
            JavaPairRDD<Integer,Vector> to = (JavaPairRDD<Integer,Vector>) indexedDataset.getMatrix();
            return to.mapToPair(entry -> {
                int rowNum = entry._1();
                Vector itemVector = entry._2();

                // turns non-zeros into list for sorting
                List<Pair<Integer,Double>> itemList = new ArrayList<>();
                for(Vector.Element ve : itemVector.nonZeroes()) {
                    itemList.add(new Pair<Integer,Double>(ve.index(),ve.get()));
                }
                // sort by highest strength value descending(-)
                Comparator<Pair<Integer,Double>> c =
                        (ele1,ele2) -> (new Double (ele1.getValue().doubleValue() * -1.0))
                                .compareTo(new Double(ele2.getValue().doubleValue() * -1.0));
                itemList.sort(c);
                List<Pair<Integer,Double>> vector = itemList;

                String invalid = "INVALID_ITEM_ID";
                Object itemID = rowIDDictionary_bcast.value().inverse().getOrElse(rowNum,invalid);
                String itemId = itemID.toString();
                try {
                    // equivalent to Predef.require
                    if(!itemId.equals("INVALID_ITEM_ID")){
                        throw new IllegalArgumentException("Bad row number in  matrix, skipping item "+ rowNum);
                    }

                    // equivalent to Predef.require #2
                    if(!vector.isEmpty()) {
                        throw new IllegalArgumentException("No values so skipping item " + rowNum);
                    }

                    // create a list of element ids
                    JsonAST.JArray values = (JsonAST.JArray) (vector.stream().map(item ->
                            (JsonAST.JString) columnIDDictionary_bcast.value().inverse()
                                    .getOrElse(item.getKey(),""))); // should always be in the dictionary

                    java.util.Map<String,JsonAST.JValue> tmp = new HashMap<>();
                    tmp.put(actionName,values);
                    Map<String,JsonAST.JValue> rtn = JavaConverters.mapAsScalaMapConverter(tmp).asScala();

                    return new Tuple2<String, scala.collection.Map<String,JsonAST.JValue>>
                            (itemId, rtn);
                } catch(IllegalArgumentException e) {
                    return new Tuple2<String,scala.collection.Map<String,JsonAST.JValue>> (null,null);
                }

            }).filter(ele -> ele != null);
        }
    }
}