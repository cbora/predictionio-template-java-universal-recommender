package org.template;


import org.apache.predictionio.data.storage.PropertyMap;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.DateTime;
import org.json4s.JsonAST;
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PreparatorTest {
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
        PropertyMap PropMap1 = PropertyMap.apply(Map1,DateTime.now(),DateTime.now());

        JsonAST.JValue JValue2 = new JsonAST.JBool(false);
        final Tuple2[] tuple2Array2 = { new Tuple2<> ("f",JValue2) };
        final WrappedArray wa2 = Predef.wrapRefArray(tuple2Array2);
        scala.collection.immutable.Map<String,JsonAST.JValue> Map2 = Predef$.MODULE$.Map().apply(wa2);
        PropertyMap PropMap2 = PropertyMap.apply(Map2,DateTime.now(),DateTime.now());

        List<Tuple2<String, PropertyMap>> fieldsList = Arrays.asList(
                new Tuple2<>("truth expression", PropMap1),
                new Tuple2<>("false expression", PropMap2)
        );


        Seq<Tuple2<String, PropertyMap>> fieldsSeq = JavaConversions.asScalaBuffer(fieldsList).toSeq();
        ClassTag<Tuple2<String, PropertyMap>> tag = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDD<Tuple2<String, PropertyMap>> _fieldsRDD =
                sc.parallelize(fieldsSeq, sc.defaultParallelism(), tag).toJavaRDD();
        JavaPairRDD<String, PropertyMap> fieldsRDD = JavaPairRDD.fromJavaRDD(_fieldsRDD);


        // create actions
        List<Tuple2<String,String>> firstAction = Arrays.asList(new Tuple2<>("alpha", "1"));
        Seq<Tuple2<String, String>> firstSeq = JavaConversions.asScalaBuffer(firstAction).toSeq();
        ClassTag<Tuple2<String,String>> tag1 = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDD<Tuple2<String, String>> _firstAction =
                sc.parallelize(firstSeq, sc.defaultParallelism(), tag1).toJavaRDD();
        JavaPairRDD<String,String> action1 = JavaPairRDD.fromJavaRDD(_firstAction);
        Tuple2<String,JavaPairRDD<String,String>> first = new Tuple2<>("first",action1);

        List<Tuple2<String,String>> secondAction = Arrays.asList(new Tuple2<>("beta", "2"));
        Seq<Tuple2<String, String>> secondSeq = JavaConversions.asScalaBuffer(secondAction).toSeq();
        ClassTag<Tuple2<String,String>> tag2 = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDD<Tuple2<String, String>> _secondAction =
                sc.parallelize(secondSeq, sc.defaultParallelism(), tag2).toJavaRDD();
        JavaPairRDD<String,String> action2 = JavaPairRDD.fromJavaRDD(_secondAction);
        Tuple2<String,JavaPairRDD<String,String>> second = new Tuple2<>("second",action2);

        List<Tuple2<String,JavaPairRDD<String,String>>> actions = Arrays.asList(first,second);

        // create Training Data
        TrainingData trainingData = new TrainingData(actions,fieldsRDD);

        // create Preparator
        Preparator myPreparator = new Preparator();

        // create the PreparedData
        PD = myPreparator.prepare(sc,trainingData);
    }

    @Test
    public void preparatorTest() throws Exception {
        JavaPairRDD<String,HashMap<String,JsonAST.JValue>> PDFields = PD.getFieldsRDD();

        Map<String,JsonAST.JValue> truthMap = (PDFields.lookup("truth expression")).get(0);
        Map<String,JsonAST.JValue> falseMap = (PDFields.lookup("false expression")).get(0);
        JsonAST.JValue JTrue = truthMap.get("t");
        JsonAST.JValue JFalse = falseMap.get("f");
        assertTrue((boolean) JTrue.values());
        assertFalse((boolean) JFalse.values());

        Tuple2<String,IndexedDatasetJava> first = PD.getActions().get(0);
        Tuple2<String,IndexedDatasetJava> second = PD.getActions().get(1);

        // check that the eventName key was converted correctly
        assertTrue(first._1().equals("first"));
        assertTrue(second._1().equals("second"));

        // extract the IndexedDatasetsJava from the tuple2
        IndexedDatasetJava firstData = first._2();
        IndexedDatasetJava secondData = second._2();

        //todo: include more tests here if the integration testing fails on Preparator
        //todo: difficult to test correctness of conversion from RDD -> indexedDatasetJava
    }
}