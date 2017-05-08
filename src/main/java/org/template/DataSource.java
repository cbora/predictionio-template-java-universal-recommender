package org.template;

import org.slf4j.Logger;
import org.apache.predictionio.controller.EmptyParams;
import org.apache.predictionio.controller.java.PJavaDataSource;
import org.apache.predictionio.core.EventWindow;
import org.apache.predictionio.core.SelfCleaningDataSource;
import org.apache.predictionio.core.SelfCleaningDataSource$class;
import org.apache.predictionio.data.storage.*;
import org.apache.predictionio.data.store.java.OptionHelper;
import org.apache.predictionio.data.store.java.PJavaEventStore;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterable;
import scala.collection.immutable.Set;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DataSource extends PJavaDataSource<TrainingData, EmptyParams, Query, Set<String>>
        implements SelfCleaningDataSource {

    private final Logger logger = LoggerFactory.getLogger(SelfCleaningDataSource.class);
    private transient PEvents pEventsDb = Storage.getPEvents();
    private transient LEvents lEventsDb = Storage.getLEvents(false);

    public final DataSourceParams dsp; // Data source param object

    /**
     * Data Source reads data from an input source and transforms it into a desired format
     * @param dsp
     */
    public DataSource(DataSourceParams dsp) {
        this.dsp = dsp;

        // Draw info
        List<Tuple2<String, Object>> t = new ArrayList<Tuple2<String, Object>>();
        t.add(new Tuple2("===================", "==================="));
        t.add(new Tuple2("App name", dsp.getAppName()));
        t.add(new Tuple2("Event window", dsp.getEventWindow()));
        t.add(new Tuple2("Event names", dsp.getEventNames()));
        Conversions.drawInfo("Init DataSource", t, this.logger);
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

    /**
     *  Separate events by event name
     *  @return actionRdds
     * */
    public List<Tuple2<String, JavaPairRDD<String, String>>> separateEvents(JavaRDD<Event> eventsRDD) {
        ArrayList<String> eventNames = dsp.getEventNames(); // get event names

        // Now separate events by event names
        List<Tuple2<String, JavaPairRDD<String,String>>> actionRDDs =
                eventNames.stream()
                        .map(eventName -> {
                            JavaRDD<Tuple2<String, String>> actionRDD =
                                    eventsRDD.filter(event -> !event.entityId().isEmpty()
                                            && !event.targetEntityId().get().isEmpty()
                                            && eventName.equals(event.event()))
                                            .map(event -> new Tuple2<String, String>(
                                                    event.entityId(),
                                                    event.targetEntityId().get()));
                            return new Tuple2<>(eventName, JavaPairRDD.fromJavaRDD(actionRDD));
                        })
                        .filter( pair -> !pair._2().isEmpty())
                        .collect(Collectors.toList());

        return actionRDDs;

    }

    /* Getter
     * @retrun new TrainingData object
     */
    public TrainingData readTraining(SparkContext sc) {
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

        List<Tuple2<String, JavaPairRDD<String, String>>> actionRDDs = separateEvents(eventsRDD);
        String eventNamesLogger = actionRDDs.stream().map(i -> i._1()).collect(Collectors.joining(", "));

        logger.debug(String.format("Received actions for events %s", eventNamesLogger));

        JavaRDD<Tuple2<String, PropertyMap>> fieldsRDD = PJavaEventStore.aggregateProperties(
                dsp.getAppName(),                           // app name
                "item",                  // entity type
                OptionHelper.<String>none(),                     // channel name
                OptionHelper.<DateTime>none(),              // start time
                OptionHelper.<DateTime>none(),              // end time
                OptionHelper.<List<String>>none(),          // required entities
                sc                                          // spark context
        ).repartition(sc.defaultParallelism());

        JavaPairRDD<String, PropertyMap> fieldsJavaPairRDD = JavaPairRDD.fromJavaRDD(fieldsRDD);

        return new TrainingData(actionRDDs, fieldsJavaPairRDD );
    }

    private boolean isSetEvent(Event e) {
        return e.event().equals("$set") || e.event().equals("$unset");
    }

    public grizzled.slf4j.Logger logger() {
      return (grizzled.slf4j.Logger)this.logger;
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
    
