/**
 * Created by Alvin Zhu on 2/22/17.
 */
import org.apache.mahout.math.cf.SimilarityAnalysis;
import org.apache.mahout.math.drm.CheckpointedDrm;
import org.apache.mahout.math.drm.DistributedContext;
import org.apache.mahout.math.drm.DrmLike;
import scala.Enumeration;
import scala.collection.immutable.List;
import org.apache.mahout.h2obindings.H2OEngine;
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark;
import org.apache.mahout.math.indexeddataset.BiDictionary;
import org.apache.mahout.math.indexeddataset.IndexedDataset;


public class ScalaTest {
    public static void main(String[] args){
        BiDictionary bd = new BiDictionary();
        int j = SimilarityAnalysis.cooccurrences$default$2();
        System.out.println("Running");
        System.out.println("The second default argument for cooccurrences() is "+j);
    }
}
