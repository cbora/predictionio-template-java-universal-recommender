package org.template.recommendation;


import org.apache.mahout.math.indexeddataset.IndexedDataset;
import org.apache.predictionio.data.storage.PropertyMap;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import java.io.Serializable;
import java.util.List;

public class PreparedData implements Serializable {
    private final List<Tuple2<String, IndexedDataset>> actions;
    private final RDD<Tuple2<String,PropertyMap>> fieldsRDD;

    public PreparedData(List<Tuple2<String, IndexedDataset>> actions, RDD<Tuple2<String,PropertyMap>> fieldsRDD) {
        this.actions = actions;
        this.fieldsRDD = fieldsRDD;
    }

    public List<Tuple2<String, IndexedDataset>> getActions() {
        return actions;
    }

    public RDD<Tuple2<String,PropertyMap>> getFieldsRDD() {
        return fieldsRDD;
    }
}


