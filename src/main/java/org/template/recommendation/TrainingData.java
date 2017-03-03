package org.template.recommendation;

import java.util.List;
import org.apache.predictionio.data.storage.PropertyMap;
import org.apache.predictionio.controller.SanityCheck;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.Serializable;


public class TrainingData implements Serializable, SanityCheck {
    private final List<Tuple2<String, JavaPairRDD<String,String>>> actions;
    private final JavaPairRDD<String,PropertyMap> fieldsRDD;


    public TrainingData(List<Tuple2<String, JavaPairRDD<String,String>>> actions, JavaPairRDD<String,PropertyMap> fieldsRDD) {
        this.actions = actions;
        this.fieldsRDD = fieldsRDD;
    }

    public List<Tuple2<String, JavaPairRDD<String,String>>> getActions() {
        return actions;
    }
    public JavaPairRDD<String,PropertyMap> getFieldsRDD() {
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