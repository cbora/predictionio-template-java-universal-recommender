// package org.template.recommendation
import com.google.common.collect.ImmutableMap;
import io.prediction.data.store.java.PJavaEventStore;
import scala.collection.JavaConversion;
import scala.collection.Seq;
import io.prediction.core.EventWindow;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class DataSource extends PDataSource<TrainingData, EmptyParams, Query, Set<String>>{    

    public final DataSourceParams dsp; // Data source param object

    public DataSource(DataSourceParams dsp) {

        this.dsp = dsp;
        // Draw info
        /***
        drawInfo("Init DataSource", Seq(
                                        ("===================", "==================="),
                                        ("App name", dsp.getAppName()),
                                        ("Event window", dsp.getEventWindow()),
                                        ("Event names", dsp.getEventNames())
                                        ))
        **/
        
    }

    /* Getter
     * @return appName
     */
    public String getAppName() {
        return dsp.getAppName();
    }

    /* Getter
     * @return EventWindow
     */
    public EventWindow getEventWindow() {
        return dsp.getEventWindow();
    }

    /* Getter
     * @retrun new TrainingData object
     */
    public TrainingData readTraining(SparkContext sc) {

        ArrayList<String> EventNames = dsp.getEventNames(); // get event names

        // find events associated with the particular app name and eventNames
        JavaRDD<Event> eventsRDD = PJavaEventStore.find(
                dsp.getAppName(),                       // app name
                OptionHelper.<~>none(),                 // channel name
                OptionHelper.<DateTime>none(),          // start time
                OptionHelper.<DateTime>none(),          // end time
                OptionHelper.some("user"),              // entity type
                OptionHelper.<~>none(),                 // entity id
                dsp.getEventNames(),                    // event names
                OptionHelper.some("item"),              // target entity type
                OptionHelper.<Option<String>>none(),    // target entity id
                sc                                      // spark context
        ).repartition(sc.defaultParallelism);



        // Now separate events by event name
        JavaPairRDD<String, JavaPairRDD<String, String>> eventRDD =
            eventNames.mapToPair(
                                 new PairFunction<String, String, Tuple2<String, JavaPairRDD<String, String>>() {
                                     public Tuple2<String, JavaPairRDD> call(String e) {
                                         singleEventRDD = eventsRDD.filter(
                                                                           new Function<JavaRDD<Event>, Boolean>() {
                                                                               public Boolean call(JavaRDD<Event> event){
                                                                                   // missing some checks
                                                                                   return e.isEqual(event.event());
                                                                               }
                                                                           };
                                                                           )
                                         return new Tuple2(e, singleEventRDD);
                                     }
                                 };
                                 );

        // convert the eventRDD to a list
        List<String, JavaPairRDD<String, String>> eventRDDs = eventRDD.collect();           

        JavaPairRDD<String, PropertyMap> fieldsRDD = PJavaEventStore.aggregateProperties(
                dsp.getAppName(),                           // app name
                OptionHelper.some("item"),                  // entity type
                OptionHelper.<~>none(),                     // channel name
                OptionHelper.<DateTime>none(),              // start time
                OptionHelper.<DateTime>none(),              // end time
                OptionHelper.<List<String>>none(),          // required entities
                sc                                          // spark context
        ).repartition(sc.defaultParallelism);

        return new TrainingData(eventRDDs, fieldsRDD);
    }

}
    
