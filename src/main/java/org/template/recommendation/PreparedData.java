package org.template.recommendation;


import org.apache.mahout.math.indexeddataset.IndexedDataset;
import org.apache.predictionio.data.storage.PropertyMap;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import java.io.Serializable;
import java.util.List;

public class PreparedData implements Serializable {
    private final List<Tuple2<String, IndexedDataset>> actions;
    private final JavaPairRDD<String,PropertyMap> fieldsRDD;

    public PreparedData(List<Tuple2<String, IndexedDataset>> actions, JavaPairRDD<String,PropertyMap> fieldsRDD) {
        this.actions = actions;
        this.fieldsRDD = fieldsRDD;
    }

    public List<Tuple2<String, IndexedDataset>> getActions() {
        return actions;
    }

    public JavaPairRDD<String,PropertyMap> getFieldsRDD() {
        return fieldsRDD;
    }
}