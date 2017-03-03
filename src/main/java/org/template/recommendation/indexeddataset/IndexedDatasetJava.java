package org.template.recommendation.indexeddataset;

import org.apache.mahout.math.indexeddataset.BiDictionary;
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark;
import org.apache.mahout.math.drm.CheckpointedDrm;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Optional;

/**
 * Created by Alvin Zhu on 3/1/17.
 */
public class IndexedDatasetJava {
    IndexedDatasetSpark ids;

    public IndexedDatasetJava(){}

    public IndexedDatasetJava(CheckpointedDrm drm,
                              BiDictionaryJava rowIds,
                              BiDictionaryJava colIds){
        ids = new IndexedDatasetSpark(drm, rowIds.bdict, colIds.bdict);
    }

    public IndexedDatasetJava(IndexedDatasetSpark ids){
        this.ids = ids;
    }

    public void dfsWrite(String dest, SparkContext sc){
        // TODO: figure out what should the distributed context be
        ids.dfsWrite(dest, ids.dfsWrite$default$2(), sc);
    }

    public static IndexedDatasetJava apply(JavaRDD<Tuple2<String, String>> rdd,
                                           BiDictionaryJava existingRowIDs,
                                           SparkContext sc){
        // TODO: make existingRowIds optional
        Optional<BiDictionary> op = Optional.of(existingRowIDs.bdict);
        return IndexedDatasetSpark.apply(rdd, op, sc);
    }
}
