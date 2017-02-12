package org.template.recommendation;

import java.util.Map;
import org.apache.predictionio.data.storage.PropertyMap;
import org.apache.predictionio.controller.SanityCheck;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.Serializable;


public class TrainingData implements Serializable, SanityCheck {
    private final Map<String, JavaPairRDD<String,String>> actions;
    private final JavaPairRDD<String,PropertyMap> fieldsRDD;


    public TrainingData(Map<String,JavaPairRDD<String,String>> actions, JavaPairRDD<String,PropertyMap> fieldsRDD) {
        this.actions = actions;
        this.fieldsRDD = fieldsRDD;
    }

    public Map<String,JavaPairRDD<String, String>> getActions() {
        return actions;
    }
    public JavaPairRDD<String,PropertyMap> getFieldsRDD() {
        return fieldsRDD;
    }

    @Override
    public void sanityCheck() {
        if (actions.isEmpty()) {
            throw new AssertionError("Actions Map is empty");
        }
        if (fieldsRDD.isEmpty()) {
            throw new AssertionError("fieldsRDD data is empty");
        }
    }
}
