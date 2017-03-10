package org.template.recommendation.indexeddataset;

import org.apache.mahout.math.indexeddataset.BiDictionary;
import java.util.List;
import java.util.HashMap;

import scala.collection.JavaConverters$;
import scala.collection.immutable.Map;
import scala.collection.JavaConverters;
import scala.Predef;
import scala.Tuple2;

/**
 * Created by Alvin Zhu on 2/27/17.
 * Mini-wrapper for org.apache.mahout.math.indexeddataset.Bimap
 *
 * TODO: We skipped toSeq() function.
 * TODO: Make BiDictionaryJava a subclass of BiMapJava
 */
public class BiDictionaryJava extends BiMapJava{

    BiDictionary bdict;

    public BiDictionaryJava(BiDictionary bdict){
        bdict = bdict;
    }

    public BiDictionaryJava(List<String> l){
        HashMap<String,Object> m = new HashMap<>();
        for (int i=0;i<l.size(); i++){
            m.put(l.get(i),i);
        }
        Map<String, Object> xScala = JavaConverters$.MODULE$.mapAsScalaMapConverter(m).asScala().toMap(
                scala.Predef$.MODULE$.conforms()
        );
        this.bdict = new BiDictionary(xScala, null);
    }

    public BiDictionaryJava merge(List<String> keys){
        BiDictionary newBdict = bdict.merge(JavaConverters.asScalaIterableConverter(keys).asScala().toSeq());
        return new BiDictionaryJava(newBdict);
    }

}
