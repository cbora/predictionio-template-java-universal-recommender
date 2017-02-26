package org.template.recommendation;


import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.mahout.math.indexeddataset.IndexedDataset;
import org.apache.predictionio.data.storage.PropertyMap;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import java.io.Serializable;
import java.util.List;

@AllArgsConstructor
public class PreparedData implements Serializable {
    @Getter private final List<Tuple2<String, IndexedDataset>> actions;
    @Getter private final RDD<Tuple2<String,PropertyMap>> fieldsRDD;
}


