package org.template.recommendation;


import org.apache.predictionio.data.storage.PropertyMap;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.Serializable;
import java.util.List;
import javafx.util.Pair;

public class PreparedData implements Serializable {
    private final List<Pair<String,JavaPairRDD<String,String>>> actions;
    //private final Map<String, JavaPairRDD<String,String>> actions;
    private final JavaPairRDD<String,PropertyMap> fieldsRDD;

    public PreparedData(List<Pair<String,JavaPairRDD<String,String>>> actions, JavaPairRDD<String,PropertyMap> fieldsRDD) {
        this.actions = actions;
        this.fieldsRDD = fieldsRDD;
    }

    public List<Pair<String,JavaPairRDD<String,String>>> getActions() {
        return actions;
    }

    public JavaPairRDD<String,PropertyMap> getFieldsRDD() {
        return fieldsRDD;
    }
}


