package org.template.recommendation;


import org.apache.mahout.math.indexeddataset.IndexedDataset;
import org.apache.predictionio.data.storage.DataMap;
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
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class PreparatorTest {
    private SparkContext sc;

    @Before
    public void setUp() {

        // create spark context
        SparkConf conf = new SparkConf().setAppName("myApp").setMaster("local");
        sc = new SparkContext(conf);

        // create fieldsRDD: from Richie's code
        List<Tuple2<String, PropertyMap>> fieldsList = Arrays.asList(
                new Tuple2<String, PropertyMap>("a", null),
                new Tuple2<String, PropertyMap>("e", null)
        );
        Seq<Tuple2<String, PropertyMap>> fieldsSeq = JavaConversions.asScalaBuffer(fieldsList).toSeq();
        ClassTag<Tuple2<String, PropertyMap>> tag = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDD<Tuple2<String, PropertyMap>> _fieldsRDD = sc.parallelize(fieldsSeq, sc.defaultParallelism(), tag).toJavaRDD();
        JavaPairRDD<String, PropertyMap> fieldsRDD = JavaPairRDD.fromJavaRDD(_fieldsRDD);

        // create actions
        List<Tuple2<String,String>> firstAction = Arrays.asList(
                new Tuple2<String,String>("alpha", "1"));
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



}
