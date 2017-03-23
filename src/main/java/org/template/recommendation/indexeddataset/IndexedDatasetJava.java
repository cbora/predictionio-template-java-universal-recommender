package org.template.recommendation.indexeddataset;

import org.apache.mahout.math.drm.DistributedContext;
import org.apache.mahout.math.indexeddataset.BiDictionary;
import org.apache.mahout.math.indexeddataset.IndexedDataset;
import org.apache.mahout.math.indexeddataset.Schema;
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark;
import org.apache.mahout.math.drm.CheckpointedDrm;
import org.apache.spark.SparkContext;
import org.apache.mahout.sparkbindings.SparkDistributedContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.predictionio.data.store.java.OptionHelper;

import java.util.Optional;

/**
 * Created by Alvin Zhu on 3/1/17.
 */
public class IndexedDatasetJava implements IndexedDataset {
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

    public CheckpointedDrm getMatrix(){
        return ids.matrix();
    }

    public BiDictionaryJava getRowIds(){
        return new BiDictionaryJava(ids.rowIDs());
    }

    public BiDictionaryJava getColIds(){
        return new BiDictionaryJava(ids.columnIDs());
    }

    public BiDictionary columnIDs(){return ids.columnIDs();}
    public BiDictionary rowIDs(){return ids.rowIDs();}
    public void dfsWrite(String s, Schema schema, DistributedContext dc){
        ids.dfsWrite(s, schema, dc);
    }
    public IndexedDataset create(CheckpointedDrm<Object> drm, BiDictionary colIds, BiDictionary rowIds){
        return ids.create(drm, colIds, rowIds);
    }
    public CheckpointedDrm matrix() {return ids.matrix();}

    public void dfsWrite(String dest, SparkDistributedContext sc){
        ids.dfsWrite(dest, ids.dfsWrite$default$2(), sc);
    }

    public static IndexedDatasetJava apply(JavaPairRDD<String,String> rdd,
                                           Optional<BiDictionaryJava> existingRowIDs,
                                           SparkContext sc){
        if (!existingRowIDs.isPresent()){
            throw new NullPointerException("No BiDictionary found");
        }
        Optional<BiDictionary> op = Optional.of(existingRowIDs.get().bdict);
        IndexedDatasetSpark newids =  IndexedDatasetSpark.apply(rdd.rdd(),
                        OptionHelper.<BiDictionary>some(existingRowIDs.get().bdict), sc);
        return new IndexedDatasetJava(newids);
    }

    public IndexedDatasetJava newRowCardinality(int n){
        return new IndexedDatasetJava(ids.matrix().newRowCardinality(n),
                new BiDictionaryJava(ids.rowIDs()),
                new BiDictionaryJava(ids.columnIDs()));
    }
}