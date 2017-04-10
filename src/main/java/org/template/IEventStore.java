package org.template;


import org.apache.predictionio.data.storage.Event;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.Interval;

import java.util.List;

/**
 * Event Store that supports retrieving events within an interval
 */
public interface IEventStore {
    /**
     * Retrieve events within provided interval
     * @param sc SparkContext we are using
     * @param eventNames names of events we want to look at (retrieve all events if null)
     * @param interval look at events within [interval.state, interval.end)
     * @return JavaRDD &lt Event &gt
     */
    public JavaRDD<Event> eventsRDD(SparkContext sc, List<String> eventNames, Interval interval);

    /**
     * Retrieve events within provided interval
     * @param sc SparkContext we are using
     * @param interval look at events within [interval.state, interval.end)
     * @return JavaRDD &lt Event &gt
     */
    public JavaRDD<Event> eventsRDD(SparkContext sc, Interval interval);
}
