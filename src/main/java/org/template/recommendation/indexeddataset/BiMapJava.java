package org.template.recommendation.indexeddataset;

import java.util.HashMap;
import org.apache.mahout.math.indexeddataset.BiMap;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;
import scala.collection.JavaConverters$;

/**
 * Created by Alvin Zhu on 2/27/17.
 * Mini-wrapper for org.apache.mahout.math.indexeddataset.Bimap
 *
 */
/**
 * Immutable Bi-directional Map.
 * @param m Map to use for forward reference
 * @param i optional reverse map of value to key, will create one lazily if none is provided
 *          and is required to have no duplicate reverse mappings.
 */
public class BiMapJava<K,V> {
    BiMap<K,V> bmap;

    public BiMapJava(){}

    public BiMapJava(BiMap bmap){
        this.bmap = bmap;
    }

    /*
    http://stackoverflow.com/questions/11903167/convert-java-util-hashmap-to-scala-collection-immutable-map-in-java
     */
    public BiMapJava(HashMap<K,V> x){
        Map xScala = JavaConverters$.MODULE$.mapAsScalaMapConverter(x).asScala().toMap(
                scala.Predef$.MODULE$.<scala.Tuple2<K, V>>conforms()
        );
/*
scala.collection.mutable.Map<K,V> xMut = JavaConverters.mapAsScalaMapConverter(x).asScala();
Map<K,V> xScala = new scala.collection.immutable.HashMap(JavaConverters.mapAsScalaMapConverter(x).asScala().toSeq());
*/
        bmap = new BiMap(xScala, null);
    }

    public BiMapJava<V,K> inverse(){
        return new BiMapJava(bmap.inverse());
    }

    public V get(K key){
        return bmap.get(key).get();
    }

    public V getOrElse(K key, V dflt){
        return bmap.get(key).get();
    }

    public boolean contains(K k){
        return bmap.contains(k);
    }

    public V apply(K k){
        return bmap.apply(k);
    }

    public java.util.Map<K,V> toMap(){
        java.util.Map<K,V> mJava = JavaConverters.mapAsJavaMapConverter(bmap.toMap()).asJava();
        return mJava;
    }

    public int size(){
        return bmap.size();
    }

    /*
    Returns any n key-value pairs as a new Bimap
     */
    public BiMapJava<K,V> take(int n){
        return new BiMapJava<K,V>(bmap.take(n));
    }

    @Override
    public String toString(){
        return bmap.toString();
    }
}