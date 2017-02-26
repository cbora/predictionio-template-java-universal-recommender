package org.template.recommendation;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.predictionio.data.storage.PropertyMap;
import org.apache.predictionio.controller.SanityCheck;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;


import java.io.Serializable;

@AllArgsConstructor
public class TrainingData implements Serializable, SanityCheck {
    @Getter private final List<Tuple2<String, JavaRDD<Tuple2<String,String>>>> actions;
    @Getter private final JavaRDD<Tuple2<String,PropertyMap>> fieldsRDD;

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
