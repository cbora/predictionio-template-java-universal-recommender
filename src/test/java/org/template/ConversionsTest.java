package org.template;

import org.apache.predictionio.data.storage.PropertyMap;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.DateTime;
import org.json4s.JsonAST;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.template.indexeddataset.IndexedDatasetJava;
import scala.Predef;
import scala.Predef$;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.mutable.WrappedArray;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.*;

import static junit.framework.TestCase.assertTrue;

/**
 * Created by fifty on 4/29/17.
 */
public class ConversionsTest {
    private SparkContext sc;
    private PreparedData PD;

    @Before
    public void setUp() {

        // create spark context
        SparkConf conf = new SparkConf().setAppName("myApp").setMaster("local");
        sc = new SparkContext(conf);


        // create fieldsRDD
        JsonAST.JValue JValue1 = new JsonAST.JBool(true);
        final Tuple2[] tuple2Array1 = { new Tuple2<>("t", JValue1)};
        final WrappedArray wa = Predef.wrapRefArray(tuple2Array1);
        scala.collection.immutable.Map<String,JsonAST.JValue> Map1 = Predef$.MODULE$.Map().apply(wa);
        PropertyMap PropMap1 = PropertyMap.apply(Map1, DateTime.now(),DateTime.now());

        List<Tuple2<String, PropertyMap>> fieldsList = Arrays.asList(new Tuple2<>("truth expression", PropMap1));


        Seq<Tuple2<String, PropertyMap>> fieldsSeq = JavaConversions.asScalaBuffer(fieldsList).toSeq();
        ClassTag<Tuple2<String, PropertyMap>> tag = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDD<Tuple2<String, PropertyMap>> _fieldsRDD =
                sc.parallelize(fieldsSeq, sc.defaultParallelism(), tag).toJavaRDD();
        JavaPairRDD<String, PropertyMap> fieldsRDD = JavaPairRDD.fromJavaRDD(_fieldsRDD);


        // create actions
        List<Tuple2<String,String>> firstAction = Arrays.asList(new Tuple2<>("alpha", "1"),
                new Tuple2<>("beta","2"), new Tuple2<>("gamma","3"), new Tuple2<>("delta","4"),
                new Tuple2<>("INVALID_ITEM_ID","0"));
        Seq<Tuple2<String, String>> firstSeq = JavaConversions.asScalaBuffer(firstAction).toSeq();
        ClassTag<Tuple2<String,String>> tag1 = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDD<Tuple2<String, String>> _firstAction =
                sc.parallelize(firstSeq, sc.defaultParallelism(), tag1).toJavaRDD();
        JavaPairRDD<String,String> action1 = JavaPairRDD.fromJavaRDD(_firstAction);
        Tuple2<String,JavaPairRDD<String,String>> first = new Tuple2<>("first",action1);

        List<Tuple2<String,JavaPairRDD<String,String>>> actions = Arrays.asList(first);

        // create Training Data
        TrainingData trainingData = new TrainingData(actions,fieldsRDD);

        // generate the prepared data
        PD = (new Preparator()).prepare(sc,trainingData);
    }

    @After
    public void tearDown() {
        sc.stop();
    }

    @Test
    public void datasetConversionsTest() throws Exception {

        // todo: complete if integration tests fail.  Difficult to tests validity of IndexedDatasetsJava
        /**
        // extract IndexedDatasetsJava from the PreparedData
        IndexedDatasetJava datasetjava = PD.getActions().get(0)._2();

        Conversions.IndexedDatasetConversions idConversions = new Conversions.IndexedDatasetConversions(datasetjava);
        JavaPairRDD<String,HashMap<String,JsonAST.JValue>> convIDs = idConversions.toStringMapRDD("test1");

        String actionName = convIDs.first()._1();
        assertTrue(actionName.equals("test1"));

        HashMap<String,JsonAST.JValue> testMap = convIDs.first()._2();
        System.out.println("Here is the size of the testMap " + testMap.size());
        System.out.println();
        System.out.println();
         */
    }


    @Test
    public void OptionCollectorTest() throws Exception {
        List<String> stringList = new ArrayList<>();
        stringList.add("predictionIO");
        stringList.add("is");
        stringList.add("amazing");
        Conversions.OptionCollection<String> OptCol1 = new Conversions.OptionCollection<>(Optional.of(stringList));
        assertTrue(OptCol1.getOrEmpty().equals(stringList));

        Conversions.OptionCollection<String> OptCol2 = new Conversions.OptionCollection<>(Optional.empty());
        assertTrue(OptCol2.getOrEmpty().equals(new ArrayList<String>()));
    }
}
