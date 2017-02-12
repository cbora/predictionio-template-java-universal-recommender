package org.template.recommendation;

import org.apache.predictionio.data.storage.PropertyMap;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.Serializable;
import java.util.Map;

public class PreparedData implements Serializable {
    private final Map<String, JavaPairRDD<String,String>> actions;
    private final JavaPairRDD<String,PropertyMap> fieldsRDD;

    public PreparedData(Map<String,JavaPairRDD<String,String>> actions, JavaPairRDD<String,PropertyMap> fieldsRDD) {
        this.actions = actions;
        this.fieldsRDD = fieldsRDD;
    }

    public Map<String,JavaPairRDD<String,String>> getActions() {
        return actions;
    }

    public JavaPairRDD<String,PropertyMap> getFieldsRDD() {
        return fieldsRDD;
    }
}


