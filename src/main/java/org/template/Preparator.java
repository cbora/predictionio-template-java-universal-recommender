/*
 * Copyright ActionML, LLC under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * ActionML licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



package org.template;

import org.apache.predictionio.controller.java.PJavaPreparator;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.json4s.JsonAST;
import org.template.indexeddataset.BiDictionaryJava;
import org.template.indexeddataset.IndexedDatasetJava;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Preparator extends PJavaPreparator<TrainingData, PreparedData> {

    /** Create [[org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark]] rdd backed
     * "distributed row matrices" from the input string keyed rdds.
     * @param sc Spark context
     * @param trainingData list of (actionName, actionRDD)
     * @return list of (correlatorName, correlatorIndexedDataset)
     */
    @Override
    public PreparedData prepare(SparkContext sc, TrainingData trainingData) {
        // now that we have all actions in separate RDDs we must merge any user dictionaries and
        // make sure the same user ids map to the correct events
        Optional<BiDictionaryJava> userDictionary = Optional.empty();

        List<Tuple2<String,IndexedDatasetJava>> indexedDatasets = new ArrayList<>();

        // make sure the same user ids map to the correct events for merged user dictionaries
        for(Tuple2<String,JavaPairRDD<String,String>> entry : trainingData.getActions()) {

            String eventName = entry._1();
            JavaPairRDD<String,String> eventIDS = entry._2();

            // passing in previous row dictionary will use the values if they exist
            // and append any new ids, so after all are constructed we have all user ids in the last dictionary
            IndexedDatasetJava ids = IndexedDatasetJava.apply(eventIDS, userDictionary, sc);
            userDictionary = Optional.of(ids.getRowIds());

            // append the transformation to the indexedDatasets list
            indexedDatasets.add(new Tuple2<>(eventName, ids));
        }

        List<Tuple2<String,IndexedDatasetJava>> rowAdjustedIds = new ArrayList<>();

        // check to see that there are events in primary event IndexedDataset and return an empty list if not
        if(userDictionary.isPresent()){

            // now make sure all matrices have identical row space since this corresponds to all users
            // with the primary event since other users do not contribute to the math
            for(Tuple2<String,IndexedDatasetJava> entry : indexedDatasets) {
                String eventName = entry._1();
                IndexedDatasetJava eventIDS = entry._2();
                rowAdjustedIds.add(new Tuple2<>(eventName,
                        (new IndexedDatasetJava(eventIDS.getMatrix(),userDictionary.get(),eventIDS.getColIds())).
                                newRowCardinality(userDictionary.get().size())));
            }
        }

        JavaPairRDD<String, Map<String,JsonAST.JValue>>fieldsRDD =
                trainingData.getFieldsRDD().mapToPair(entry -> new Tuple2<>(
                        entry._1(), JavaConverters.mapAsJavaMapConverter(entry._2().fields()).asJava()));

        return new PreparedData(rowAdjustedIds, fieldsRDD);
    }
}