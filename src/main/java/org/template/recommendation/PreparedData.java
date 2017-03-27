package org.template.recommendation;

import org.apache.spark.api.java.JavaPairRDD;
import org.json4s.JsonAST;
import org.template.recommendation.indexeddataset.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class PreparedData implements Serializable {

    @Getter private final List<Tuple2<String, IndexedDatasetJava>> actions;
    @Getter private final JavaPairRDD<String,Map<String,JsonAST.JValue>> fieldsRDD;

}