package org.template.recommendation;
import com.google.common.collect.ImmutableMap;

import lombok.AllArgsConstructor;
import org.apache.predictionio.controller.EmptyParams;
import org.apache.predictionio.controller.PDataSource;
import org.apache.predictionio.core.EventWindow;
import org.apache.predictionio.core.SelfCleaningDataSource;
import org.apache.predictionio.core.SelfCleaningDataSource$class;
import org.apache.predictionio.data.storage.*;
import org.apache.predictionio.data.store.java.OptionHelper;
import org.apache.predictionio.data.store.java.PJavaEventStore;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.joda.time.DateTime;
import grizzled.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterable;
import scala.collection.Seq;
import scala.collection.immutable.*;
import scala.collection.immutable.Set;


import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

public class DataSource extends PDataSource<TrainingData, EmptyParams, Query, Set<String>>
        implements SelfCleaningDataSource {

    Logger logger = new Logger(LoggerFactory.getLogger(SelfCleaningDataSource.class));
    transient PEvents pEventsDb = Storage.getPEvents();
    transient LEvents lEventsDb = Storage.getLEvents(false);

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

        ArrayList<String> eventNames = dsp.getEventNames(); // get event names

        // find events associated with the particular app name and eventNames
        JavaRDD<Event> eventsRDD = PJavaEventStore.find(
                dsp.getAppName(),                       // app name
                OptionHelper.<String>none(),                 // channel name
                OptionHelper.<DateTime>none(),          // start time
                OptionHelper.<DateTime>none(),          // end time
                OptionHelper.some("user"),              // entity type
                OptionHelper.<String>none(),                 // entity id
                OptionHelper.some(dsp.getEventNames()),                    // event names
                OptionHelper.some(OptionHelper.some("item")),              // target entity type
                OptionHelper.<Option<String>>none(),    // target entity id
                sc                                      // spark context
        ).repartition(sc.defaultParallelism());

        // Now separate events by event name
        List<Tuple2<String, JavaPairRDD<String,String>>> actionRDDs =
                eventNames.stream()
                .map(eventName -> {
                    JavaRDD<Tuple2<String, String>> actionRDD =
                            eventsRDD.filter(event -> { return !event.entityId().isEmpty()
                            && !event.targetEntityId().get().isEmpty()
                            && eventName.equals(event.event()); })
                                    .map(event -> {
                                        return new Tuple2<String, String>(
                                                event.entityId(),
                                                event.targetEntityId().get());
                                    });
                    return new Tuple2<>(eventName, JavaPairRDD.fromJavaRDD(actionRDD));
                })
                .filter( pair -> !pair._2().isEmpty())
                .collect(Collectors.toList());

        // String eventNamesLogger = actionRDDs.stream()
         //       .map(i -> i._1()).collect(Collectors.joining(", "));

        // logger.debug(String.format("Received actions for events %s", eventNamesLogger));

        JavaRDD<Tuple2<String, PropertyMap>> fieldsRDD = PJavaEventStore.aggregateProperties(
                dsp.getAppName(),                           // app name
                "item",                  // entity type
                OptionHelper.<String>none(),                     // channel name
                OptionHelper.<DateTime>none(),              // start time
                OptionHelper.<DateTime>none(),              // end time
                OptionHelper.<List<String>>none(),          // required entities
                sc                                          // spark context
        ).repartition(sc.defaultParallelism());

        JavaPairRDD fieldsJavaPairRDD = JavaPairRDD.fromJavaRDD(fieldsRDD);

        return new TrainingData(actionRDDs, fieldsJavaPairRDD );
    }

    private boolean isSetEvent(Event e) {
        return e.event() == "$set" || e.event() == "$unset";
    }

    public Logger logger() {
      return this.logger;
    }

    @Override
    public String appName() {
        return dsp.getAppName();
    }

    @Override
    public Option<EventWindow> eventWindow() {
        return SelfCleaningDataSource$class.eventWindow(this);
    }

    @Override
    public RDD<Event> getCleanedPEvents(RDD<Event> pEvents) {
        return SelfCleaningDataSource$class.getCleanedPEvents(this, pEvents);
    }

    @Override
    public Iterable<Event> getCleanedLEvents(Iterable<Event> lEvents) {
        return SelfCleaningDataSource$class.getCleanedLEvents(this, lEvents);
    }

    @Override
    public Event recreateEvent(Event x, Option<String> eventId, DateTime creationTime) {
        return SelfCleaningDataSource$class.recreateEvent(this, x, eventId, creationTime);
    }

    @Override
    public Iterable<Event> removeLDuplicates(Iterable<Event> ls) {
        return SelfCleaningDataSource$class.removeLDuplicates(this, ls);
    }

    @Override
    public void removeEvents(Set<String> eventsToRemove, int appId) {
        SelfCleaningDataSource$class.removeEvents(this, eventsToRemove, appId);
    }

    @Override
    public RDD<Event> compressPProperties(SparkContext sc, RDD<Event> rdd) {
        return SelfCleaningDataSource$class.compressPProperties(this, sc, rdd);
    }

    @Override
    public Iterable<Event> compressLProperties(Iterable<Event> events) {
        return SelfCleaningDataSource$class.compressLProperties(this, events);
    }

    @Override
    public RDD<Event> removePDuplicates(SparkContext sc, RDD<Event> rdd) {
        return SelfCleaningDataSource$class.removePDuplicates(this, sc, rdd);
    }

    @Override
    public void cleanPersistedPEvents(SparkContext sc) {
        SelfCleaningDataSource$class.cleanPersistedPEvents(this, sc);
    }

    @Override
    public void wipePEvents(RDD<Event> newEvents, RDD<String> eventsToRemove, SparkContext sc) {
        SelfCleaningDataSource$class.wipePEvents(this, newEvents, eventsToRemove, sc);
    }

    @Override
    public void removePEvents(RDD<String> eventsToRemove, int appId, SparkContext sc) {
        SelfCleaningDataSource$class.removePEvents(this, eventsToRemove, appId, sc);
    }

    @Override
    public void wipe(Set<Event> newEvents, Set<String> eventsToRemove) {
        SelfCleaningDataSource$class.wipe(this, newEvents, eventsToRemove);
    }

    @Override
    public RDD<Event> cleanPEvents(SparkContext sc) {
        return SelfCleaningDataSource$class.cleanPEvents(this, sc);
    }

    @Override
    public void cleanPersistedLEvents() {
        SelfCleaningDataSource$class.cleanPersistedLEvents(this);
    }

    @Override
    public scala.collection.Iterable<Event> cleanLEvents() {
        return SelfCleaningDataSource$class.cleanLEvents(this);
    }

    public LEvents org$apache$predictionio$core$SelfCleaningDataSource$$lEventsDb() {
        return this.lEventsDb;
    }
    public PEvents org$apache$predictionio$core$SelfCleaningDataSource$$pEventsDb() {
        return this.pEventsDb;
    }
    public org.apache.predictionio.core.SelfCleaningDataSource.DateTimeOrdering$ DateTimeOrdering() {
        //TO-DO
        return null;
    }
}
    
