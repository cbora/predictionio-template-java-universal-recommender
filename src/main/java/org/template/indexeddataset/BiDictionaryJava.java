package org.template.indexeddataset;

import org.apache.mahout.math.indexeddataset.BiDictionary;
import org.apache.mahout.math.indexeddataset.BiMap;
import scala.Option;
import scala.Some;
import scala.collection.JavaConverters;
import scala.collection.JavaConverters$;
import scala.collection.immutable.Map;
import org.apache.mahout.math.indexeddataset.BiMap;
import scala.collection.JavaConverters;
import scala.collection.JavaConverters$;
import scala.collection.immutable.Map;

import java.util.HashMap;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;



/**
 * Created by Alvin Zhu on 2/27/17.
 * Mini-wrapper for org.apache.mahout.math.indexeddataset.Bimap
 *
 * TODO: We skipped toSeq() function.
 * TODO: Make BiDictionaryJava a subclass of BiMapJava
 */
public class BiDictionaryJava {
    BiDictionary bdict;

    // Constructor #1
    public BiDictionaryJava(BiDictionary bdict){
        this.bdict = bdict;
    }

    // Constructor #2
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

    // special BiDict only function
    public BiDictionaryJava merge(List<String> keys){
        BiDictionary newBdict = bdict.merge(JavaConverters.asScalaIterableConverter(keys).asScala().toSeq());
        return new BiDictionaryJava(newBdict);
    }

    public BiDictionaryJava inverse(){
        Map<String, Object> map = new scala.collection.immutable.HashMap<>();
        Option<BiMap<Object, String>> inversed = new Some(bdict.inverse());
        return new BiDictionaryJava(new BiDictionary(map, inversed));
    }

    public Object get(String key) {
        return bdict.get(key).get();
    }

    public Object getOrElse(String key, Object dflt) {
        if(bdict.get(key) != scala.Option.apply(null)) {
            return bdict.get(key).get();
        }
        return dflt;
    }

    public boolean contains(String s) {
        return bdict.contains(s);
    }

    public Object apply(String key) {
        return bdict.apply(key);
    }

    public java.util.Map<String,Object> toMap() {
        java.util.Map<String,Object> mJava = JavaConverters.mapAsJavaMapConverter(bdict.toMap()).asJava();
        return mJava;
    }

    public int size(){
        return bdict.size();
    }

    public String toString(){
        return bdict.toString();
    }

    public BiDictionaryJava take(int n) {
        Map<String, Object> map = new scala.collection.immutable.HashMap<>();
        Option<BiMap<Object, String>> temp = new Some(bdict.take(n));
        return new BiDictionaryJava(new BiDictionary(map,temp));
    }
}