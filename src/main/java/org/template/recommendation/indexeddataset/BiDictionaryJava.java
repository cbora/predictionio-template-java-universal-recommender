package org.template.recommendation.indexeddataset;

import org.apache.mahout.math.indexeddataset.BiDictionary;
import java.util.List;
import java.util.HashMap;
import scala.collection.immutable.Map;
import scala.collection.JavaConverters;
import scala.Predef;
import scala.Tuple2;

/**
 * Created by Alvin Zhu on 2/27/17.
 * Mini-wrapper for org.apache.mahout.math.indexeddataset.Bimap
 *
 * TODO: We skipped toSeq() function.
 */
public class BiDictionaryJava{

    BiDictionary bdict;

    public BiDictionaryJava(List<String> l){
        HashMap<String,Integer> m = new HashMap<>();
        for (int i=0;i<l.size(); i++){
            m.put(l.get(i),i);
        }
        Map<String, Integer> xScala = JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(
                Predef.<Tuple2<String, Integer>>conforms()
        );
        this.bdict = new BiDictionary(xScala, null);
    }

    public BiDictionaryJava merge(List<String> keys){
        
    }

}
