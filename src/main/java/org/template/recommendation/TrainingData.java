package org.template.recommendation;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.predictionio.controller.SanityCheck;
import org.apache.predictionio.data.storage.PropertyMap;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

@AllArgsConstructor
public class TrainingData implements Serializable, SanityCheck {
    @Getter private final List<Tuple2<String, JavaPairRDD<String,String>>> actions;
    @Getter private final JavaPairRDD<String,PropertyMap> fieldsRDD;

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
