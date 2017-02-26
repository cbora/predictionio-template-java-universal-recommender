package org.template.recommendation;

import java.util.List;
import org.apache.predictionio.data.storage.PropertyMap;
import org.apache.predictionio.controller.SanityCheck;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.io.Serializable;


public class TrainingData implements Serializable, SanityCheck {
    private final List<Tuple2<String, RDD<Tuple2<String,String>>>> actions;
    private final RDD<Tuple2<String,PropertyMap>> fieldsRDD;


    public TrainingData(List<Tuple2<String, RDD<Tuple2<String,String>>>> actions, RDD<Tuple2<String,PropertyMap>> fieldsRDD) {
        this.actions = actions;
        this.fieldsRDD = fieldsRDD;
    }

    public List<Tuple2<String, RDD<Tuple2<String,String>>>> getActions() {
        return actions;
    }
    public RDD<Tuple2<String,PropertyMap>> getFieldsRDD() {
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
}
