package org.template.indexeddataset;

import org.apache.mahout.math.indexeddataset.BiDictionary;
import scala.collection.JavaConverters;
import scala.collection.JavaConverters$;
import scala.collection.immutable.Map;

import java.util.HashMap;
import java.util.List;

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
        this.bdict = bdict;
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

    @Override
    public int size(){
        return bdict.size();
    }

    @Override
    public String toString(){
        return bmap.toString();
    }
}