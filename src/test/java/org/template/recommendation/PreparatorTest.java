package org.template.recommendation;


import javafx.util.Pair;
import org.apache.avro.data.Json;
import org.apache.mahout.math.indexeddataset.IndexedDataset;
import org.apache.predictionio.data.storage.Event;
import org.apache.predictionio.data.storage.PropertyMap;
import org.apache.predictionio.data.store.java.OptionHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.json4s.JsonAST;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import scala.Predef;
import scala.Predef$;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.immutable.*;
import scala.collection.mutable.WrappedArray;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import org.json4s.*;

import java.util.*;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class PreparatorTest {
    private SparkContext sc;
    private TrainingData TD;
    private PreparedData PD;

    @Before
    //@SupressWarnings({ "rawtypes", "unchecked" })
    public void setUp() {

        // create spark context
        SparkConf conf = new SparkConf().setAppName("myApp").setMaster("local");
        sc = new SparkContext(conf);


        // create fieldsRDD
        JsonAST.JValue JValue1 = new JsonAST.JBool(true);
        final Tuple2[] tuple2Array1 = { new Tuple2<String,JsonAST.JValue>("t", JValue1)};
        final WrappedArray wa = Predef.wrapRefArray(tuple2Array1);
        scala.collection.immutable.Map<String,JsonAST.JValue> Map1 = Predef$.MODULE$.Map().apply(wa);
        PropertyMap PropMap1 = PropertyMap.apply(Map1,DateTime.now(),DateTime.now());

        JsonAST.JValue JValue2 = new JsonAST.JBool(false);
        final Tuple2[] tuple2Array2 = { new Tuple2<> ("f",JValue2) };
        scala.collection.immutable.Map<String,JsonAST.JValue> Map2 = Predef$.MODULE$.Map().apply(wa);
        PropertyMap PropMap2 = PropertyMap.apply(Map2,DateTime.now(),DateTime.now());

        List<Tuple2<String, PropertyMap>> fieldsList = Arrays.asList(
                new Tuple2<>("truth expression", PropMap1),
                new Tuple2<>("false expression", PropMap2)
        );

        Seq<Tuple2<String, PropertyMap>> fieldsSeq = JavaConversions.asScalaBuffer(fieldsList).toSeq();
        ClassTag<Tuple2<String, PropertyMap>> tag = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDD<Tuple2<String, PropertyMap>> _fieldsRDD = sc.parallelize(fieldsSeq, sc.defaultParallelism(), tag).toJavaRDD();
        JavaPairRDD<String, PropertyMap> fieldsRDD = JavaPairRDD.fromJavaRDD(_fieldsRDD);


        // create actions
        List<Tuple2<String,String>> firstAction = Arrays.asList(new Tuple2<>("alpha", "1"));
        Seq<Tuple2<String, String>> firstSeq = JavaConversions.asScalaBuffer(firstAction).toSeq();
        ClassTag<Tuple2<String,String>> tag1 = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDD<Tuple2<String, String>> _firstAction = sc.parallelize(firstSeq, sc.defaultParallelism(), tag1).toJavaRDD();
        JavaPairRDD<String,String> action1 = JavaPairRDD.fromJavaRDD(_firstAction);
        Tuple2<String,JavaPairRDD<String,String>> first = new Tuple2<>("first",action1);

        List<Tuple2<String,String>> secondAction = Arrays.asList(
                new Tuple2<String,String>("beta", "2"));
        Seq<Tuple2<String, String>> secondSeq = JavaConversions.asScalaBuffer(secondAction).toSeq();
        ClassTag<Tuple2<String,String>> tag2 = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDD<Tuple2<String, String>> _secondAction = sc.parallelize(secondSeq, sc.defaultParallelism(), tag2).toJavaRDD();
        JavaPairRDD<String,String> action2 = JavaPairRDD.fromJavaRDD(_secondAction);
        Tuple2<String,JavaPairRDD<String,String>> second = new Tuple2<>("second",action2);

        List<Tuple2<String,JavaPairRDD<String,String>>> actions = Arrays.asList(first,second);

        // create Training Data
        TrainingData trainingData = new TrainingData(actions,fieldsRDD);

        // create Preparator
        Preparator myPreparator = new Preparator();

        // create the PreparedData
        PreparedData preparedData = myPreparator.prepare(sc,trainingData);

        //List<Tuple2<String, IndexedDataset>> preparedActions = preparedData.getActions();
        //JavaPairRDD<String,PropertyMap> preparedFieldsRDD = preparedData.getFieldsRDD();

        // sanity check that the fieldsRDD hasn't changed
        //assertTrue(preparedFieldsRDD.equals(fieldsRDD));

        // todo: check that preparedActions converted correctly to IndexedDataSets


    }

    @Test
    public void fieldsConversion() throws Exception {
        JavaPairRDD<String,Map<String,JsonAST.JValue>> PDFields = PD.getFieldsRDD();
        Map<String,JsonAST.JValue> truthMap = (PDFields.lookup("truth expression")).get(0);
        Map<String,JsonAST.JValue> falseMap = (PDFields.lookup("false expression")).get(0);
        JsonAST.JValue JTrue = truthMap.get("t");
        JsonAST.JValue JFalse = falseMap.get("f");

        assertTrue((boolean) JTrue.values());
        assertFalse((boolean) JFalse.values());
    }


}
