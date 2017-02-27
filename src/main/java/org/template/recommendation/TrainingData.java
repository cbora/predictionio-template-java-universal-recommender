package org.template.recommendation;

import java.util.List;
import org.apache.predictionio.data.storage.PropertyMap;
import org.apache.predictionio.controller.SanityCheck;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.io.Serializable;


public class TrainingData implements Serializable, SanityCheck {
    private final List<Tuple2<String, JavaRDD<Tuple2<String,String>>>> actions;
    private final JavaRDD<Tuple2<String,PropertyMap>> fieldsRDD;


    public TrainingData(List<Tuple2<String, JavaRDD<Tuple2<String,String>>>> actions, JavaRDD<Tuple2<String,PropertyMap>> fieldsRDD) {
        this.actions = actions;
        this.fieldsRDD = fieldsRDD;
    }

    public List<Tuple2<String, JavaRDD<Tuple2<String,String>>>> getActions() {
        return actions;
    }
    public JavaRDD<Tuple2<String,PropertyMap>> getFieldsRDD() {
        return fieldsRDD;
    }

    @Override
    public void sanityCheck() {
        if (actions.isEmpty()) {
            throw new AssertionError("Actions List is empty");
        }
        if (fieldsRDD.isEmpty()) {
            throw new AssertionError("fieldsRDD data is empty");
        }
    }

    @Override
    public String toString() {

        /***JavaSparkContext jsc = new JavaSparkContext();
        JavaRDD<Tuple2<String, JavaRDD<Tuple2<String, String>>>> as = jsc.parallelize(actions);
        String a = as.map( t -> {
            String answer = t._1() + " actions: [count: " + t._2().count() + " ] + sample : " + t._2().take(2).toList();
            return answer;
        });

        String b = "Item metadata: [count: "+ fieldsRDD.count() + " sample: " + fieldsRDD.take(2).toList();

        return a + b;
         **/
        return "";
    }
}