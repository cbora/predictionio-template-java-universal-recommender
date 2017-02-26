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

import scala.Option;
import scala.Tuple2;
import org.apache.spark.rdd.RDD;
import org.apache.predictionio.controller.java.PJavaPreparator;
import org.apache.spark.SparkContext;
import org.apache.mahout.math.indexeddataset.IndexedDataset;
import org.apache.mahout.math.indexeddataset.BiDictionary;
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark;
import java.util.*;


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
        // note: scala.Option.apply(null) is java's version of None
        Option<BiDictionary> userDictionary = scala.Option.apply(null);

        List<Tuple2<String,IndexedDatasetSpark>> indexedDatasets = new ArrayList<>();

        // make sure the same user ids map to the correct events for merged user dictionaries
        for(Tuple2<String,RDD<Tuple2<String,String>>> entry : trainingData.getActions()) {

            String eventName = entry._1;
            RDD<Tuple2<String,String>> eventIDS = entry._2;

            // passing in previous row dictionary will use the values if they exist
            // and append any new ids, so after all are constructed we have all user ids in the last dictionary
            IndexedDatasetSpark ids = IndexedDatasetSpark.apply(eventIDS, userDictionary, sc);
            userDictionary = scala.Option.apply(ids.rowIDs());

            // append the transformation to the indexedDatasets list
            indexedDatasets.add(new Tuple2<>(eventName, ids));
        }

        // now make sure all matrices have identical row space since this corresponds to all users
        int numUsers = userDictionary.get().size();
        try {
            long numPrimary = indexedDatasets.get(0)._2.matrix().nrow();
        }
        catch(IndexOutOfBoundsException E) {
            System.err.println("IndexOutOfBoundsException: Preparator class, indexedDatasets is empty");
        }

        // todo: check to see that there are events in primary event IndexedDataset and abort if not.

        List<Tuple2<String,IndexedDataset>> rowAdjustedIds = new ArrayList<>();

        for(Tuple2<String,IndexedDatasetSpark> entry : indexedDatasets) {
            String eventName = entry._1;
            IndexedDatasetSpark eventIDS = entry._2;
            rowAdjustedIds.add(new Tuple2<>(eventName,(eventIDS.create(eventIDS.matrix(), userDictionary.get(), eventIDS.columnIDs()).newRowCardinality(numUsers))));
        }
        return new PreparedData(rowAdjustedIds, trainingData.getFieldsRDD());
    }
}
