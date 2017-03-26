package org.template.recommendation;


import org.apache.predictionio.data.storage.Event;
import org.apache.predictionio.data.store.java.OptionHelper;
import org.apache.predictionio.data.store.java.PJavaEventStore;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import scala.Option;

import java.util.List;

public class EventStore implements IEventStore  {
    private final String appName;

    /**
     * Constructor
     * @param appName name of app whose events we want to look at
     */
    public EventStore(String appName) {
        this.appName = appName;
    }

    @Override
    public JavaRDD<Event> eventsRDD(SparkContext sc, List<String> eventNames, Interval interval) {
        return PJavaEventStore.find(
                appName, // appName
                OptionHelper.<String>none(), // channelName
                OptionHelper.<DateTime>some(interval.getStart()), // startTime
                OptionHelper.<DateTime>some(interval.getEnd()), // untilTime
                OptionHelper.<String>none(), // entityType
                OptionHelper.<String>none(), //entityID
                eventNames != null ? OptionHelper.<List<String>>some(eventNames) : OptionHelper.<List<String>>none(), // eventNames
                OptionHelper.<Option<String>>none(), // targetEntityType
                OptionHelper.<Option<String>>none(), // targetEntityID
                sc // sparkContext
        ).repartition(sc.defaultParallelism());
    }

    @Override
    public JavaRDD<Event> eventsRDD(SparkContext sc, Interval interval) {
        return eventsRDD(sc, null, interval);
    }
}
