package org.template.recommendation.similarity;

import java.util.List;

import org.apache.mahout.math.cf.DownsamplableCrossOccurrenceDataset;
import org.apache.mahout.math.cf.ParOpts;
import org.apache.mahout.math.indexeddataset.IndexedDataset;
import org.apache.mahout.math.cf.SimilarityAnalysis;

/**
 * Created by zhuyifan on 3/17/17.
 */
public class SimilarityAnalysisJava {

    public static List<IndexedDataset> crossOccurrenceDownsampled(
            List<DownsamplableCrossOccurrenceDataset> datasets,
            int seed
    ){
        scala.collection.immutable.List l =  SimilarityAnalysis.crossOccurrenceDownsampled(
                scala.collection.JavaConverters.collectionAsScalaIterableConverter(datasets).asScala().toList(),
                seed);
        return ( List<IndexedDataset>) scala.collection.JavaConverters.asJavaListConverter(l).asJava();
    }

    public static List<IndexedDataset> cooccurrencesIDSs(
            IndexedDataset[] indexedDatasets,
            Integer randomSeed,
            Integer maxInterestingItemsPerThing,
            Integer maxNumInteractions,
            ParOpts parOpts) {
        scala.collection.immutable.List l =
                SimilarityAnalysis.cooccurrencesIDSs(indexedDatasets, randomSeed, maxInterestingItemsPerThing,
                        maxNumInteractions, parOpts);
        return (List<IndexedDataset>) scala.collection.JavaConverters.asJavaListConverter(l).asJava();
    }
}
