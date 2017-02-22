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



package org.template.recommendation;

import javafx.util.Pair;
import org.apache.predictionio.controller.java.PJavaPreparator;
import org.apache.spark.SparkContext;

// todo: add mahout java imports
// todo: need IndexedDataset, BiDictionary objects

import org.apache.spark.api.java.JavaPairRDD;

import java.util.*;

//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;


public class Preparator extends PJavaPreparator<TrainingData, PreparedData> {

    /** Create [[org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark]] rdd backed
     * "distributed row matrices" from the input string keyed rdds.
     * @param sc Spark context
     * @param trainingData list of (actionName, actionRDD)
     * @return list of (correlatorName, correlatorIndexedDataset)
     */
    @Override
    public PreparedData prepare(SparkContext sc, TrainingData trainingData) {


        // todo: make userDiction here
        // todo: BiDictionary userDictionary = new BiDictionary();

        // todo: List<Pair<String,IndexedDatasetSpark>> indexedDatasets = new ArrayList<>();

        // make sure the same user ids map to the correct events for merged user dictionaries
        for(Pair<String,JavaPairRDD<String,String>> entry : trainingData.getActions()) {

            String eventName = entry.getKey();
            JavaPairRDD<String,String> eventIDS = entry.getValue();

            // todo: IndexedDatasetSpark ids = new IndexedDatasetSpark(eventIDS,userDictionary)(sc);


            // passing in previous row dictionary will use the values if they exist
            // and append any new ids, so after all are constructed we have all user ids in the last dictionary
            // todo: update the user dictionary...
            // todo: userDictionary = ids.rowIDs;


            // todo: append the transformation to the indexedDatasets Map
            // todo: indexedDatasets.add(Pair.of(eventName,ids));
        }

        // now make sure all matrices have identical row space since this corresponds to all users
        // todo: int numUsers = userDictionary.size();
        // todo: long numPrimary = indexedDatasets.head._2.matrix.nrow();

        // later todo: check to see that there are events in primary event IndexedDataset and abort if not.

        // todo: List<Pair<String,IndexedDataset>> rowAdjustedIds = new ArrayList<>();

        /* todo: the following when we have spark datasets
        for(Pair<String,IndexedDatasetSpark> entry : indexedDatasets) {
            String eventName = entry.getKey();
            IndexedDatasetSpark eventIDS = entry.getValue();
            rowAdjustedIds.add(Pair.of(eventName,eventIDS.create(eventIDS.matrix, userDictionary.get, eventIDS.columnIDs).newRowCardinality(numUsers)));
        }
        */

        // todo: return new PreparedData(rowAdjustedIds, trainingData.getFieldsRDD());
        return null;
    }
}
