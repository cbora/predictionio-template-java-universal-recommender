package org.template.recommendation;

import org.apache.predictionio.core.SelfCleaningDataSource$class;
import org.apache.predictionio.data.storage.*;
import org.apache.predictionio.core.SelfCleaningDataSource;
import org.apache.predictionio.data.store.java.OptionHelper;
import org.apache.predictionio.controller.EmptyParams;
import org.apache.predictionio.core.EventWindow;
import org.apache.predictionio.controller.PDataSource;
import org.apache.predictionio.data.store.java.PJavaEventStore;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;

import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Set;
import scala.collection.Iterable;

import java.util.List;


public class DataSource extends PDataSource<TrainingData, EmptyParams, Query, Set<String>> implements SelfCleaningDataSource {

    private DataSourceParams dsp;                                                               // Data source param object
    private transient static final Logger logger = LoggerFactory.getLogger(DataSource.class);
    transient PEvents pEventsDb = Storage.getPEvents();
    transient private LEvents lEventsDb = Storage.getLEvents(false);

    /* Constructor
     *
     * @param datasourceparams: parameters of the datasource 
     */
    public DataSource(DataSourceParams dsp) {
        super();
        this.dsp = dsp;

        /***
         // Draw info
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
    @Override
    public String appName() {
        return dsp.getAppName();
    }

    /* Getter
     * @return EventWindow
     */
    public EventWindow getEventWindow() {
        return dsp.getEventWindow();
    }

    /* Getter
     * @return datasourceparams
     */
    public DataSourceParams getDataSourceParams() {
        return dsp;
    }

    /* Getter
     * @param SparkContext object
     * @retrun new TrainingData object
     */
    public TrainingData readTraining(SparkContext sc) {

        List<String> eventNames = (dsp.getEventNames()); // get event names

        // find events associated with the particular app name and eventNames
        JavaRDD<Event> eventsRDD = PJavaEventStore.find(
                dsp.getAppName(),                                               // app name
                OptionHelper.<String>none(),                                    // channel name
                OptionHelper.<DateTime>none(),                                  // start time
                OptionHelper.<DateTime>none(),                                  // end time
                OptionHelper.some("user"),                                // entity type
                OptionHelper.<String>none(),                                    // entity id
                OptionHelper.some(eventNames),                                  // event names
                OptionHelper.some(OptionHelper.some("item")),             // target entity type
                OptionHelper.<Option<String>>none(),                            // target entity id
                sc                                                              // spark context
        ).repartition(sc.defaultParallelism());


        JavaSparkContext jsc = new JavaSparkContext();
        JavaRDD<String> eNames = jsc.parallelize(eventNames);
        JavaRDD<Tuple2<String, JavaRDD<Tuple2<String, String>>>> eventRDD =
                eNames.map(
                        eventName -> {
                            JavaRDD<Tuple2<String, String>> singleEventRDD =
                                    eventsRDD.filter(
                                            event -> {
                                                return !event.eventId().isEmpty() && !event.targetEntityId().get().isEmpty() && eventName.equals(event.event());
                                            }).map(
                                            event -> {
                                                return new Tuple2<String, String>(event.entityId(),
                                                        event.targetEntityId().get());
                                            });
                            return new Tuple2<String, JavaRDD<Tuple2<String, String>>>(eventName, singleEventRDD);
                        }).filter(pair -> !pair._2().isEmpty());

        // convert the eventRDD to a list
        List<Tuple2<String, JavaRDD<Tuple2<String, String>>>> eventRDDs = eventRDD.collect();

        JavaRDD<Tuple2<String, PropertyMap>> fieldsRDD = PJavaEventStore.aggregateProperties(
                dsp.getAppName(),                                  // app name
                "item",                                  // entity type
                OptionHelper.<String>none(),                       // channel name
                OptionHelper.<DateTime>none(),                     // start time
                OptionHelper.<DateTime>none(),                     // end time
                OptionHelper.<List<String>>none(),                 // required entities
                sc                                                 // spark context
        ).repartition(sc.defaultParallelism());

        //JavaPairRDD<String, PropertyMap> fieldsRDD = JavaPairRDD.fromJavaRDD(tmpFieldsRDD);
        logger.info("Received event: " + eventRDD.map(e -> e._1()));
        logger.info("Number of events: " + eventRDDs.size());

        return new TrainingData(eventRDDs, fieldsRDD);
    }

    public LEvents org$apache$predictionio$core$SelfCleaningDataSource$$lEventsDb() {
        return lEventsDb;
    }

    public PEvents org$apache$predictionio$core$SelfCleaningDataSource$$pEventsDb() {
        return pEventsDb;
    }

    public org.apache.predictionio.core.SelfCleaningDataSource.DateTimeOrdering$ DateTimeOrdering() {
        //TO-DO
        return null;
    }

    @Override
    public grizzled.slf4j.Logger logger() {
        return this.logger();
    }

    private boolean isSetEvent(Event e) {
        return e.event() == "$set" || e.event() == "$unset";
    }


    @Override
    public Option<EventWindow> eventWindow() {
        // TO-DO
        //return new OptionHelper.some(dsp.getEventWindow());
        return null;
    }

    @Override
    public RDD<Event> getCleanedPEvents(RDD<Event> pEvents) {
        return SelfCleaningDataSource$class.getCleanedPEvents(this, pEvents);
    }

    public Iterable<Event> getCleanedLEvents(Iterable<Event> lEvents) {
        return SelfCleaningDataSource$class.getCleanedLEvents(this, lEvents);
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
    public Iterable<Event> removeLDuplicates(Iterable<Event> ls) {
        return SelfCleaningDataSource$class.removeLDuplicates(this, ls);
    }

    @Override
    public Event recreateEvent(Event x, Option<String> eventId, DateTime creationTime) {
        return SelfCleaningDataSource$class.recreateEvent(this, x, eventId, creationTime);
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
    public void removeEvents(Set<String> eventsToRemove, int appId) {
        SelfCleaningDataSource$class.removeEvents(this, eventsToRemove, appId);
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

    public void cleanPersistedLEvents() {
        SelfCleaningDataSource$class.cleanPersistedLEvents(this);
    }

    @Override
    public scala.collection.Iterable<Event> cleanLEvents() {
        return SelfCleaningDataSource$class.cleanLEvents(this);
    }
}