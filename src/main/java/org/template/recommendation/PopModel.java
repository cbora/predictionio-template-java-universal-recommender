package org.template.recommendation;

import org.apache.predictionio.data.store.java.OptionHelper;
import org.apache.predictionio.data.storage.Event;
import org.apache.predictionio.data.store.java.PJavaEventStore;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.format.ISODateTimeFormat;
import org.json4s.JsonAST;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import scala.Option;
import scala.Tuple2;
import scala.reflect.ClassTag$;
import scala.util.Random;
import scala.reflect.ClassTag;


import java.util.*;

public class PopModel {

    private transient static final Logger logger = LoggerFactory.getLogger(PopModel.class);
    private final JavaPairRDD<String, Map<String, JsonAST.JValue>> fieldsRDD;  // ItemID -> ItemProps
    private final SparkContext sc;

    /**
     * Constructor
     * @param fieldsRDD rdd of (ItemID, ItemProp) pairs (ItemProp is a String -> JSonAST.JValue map)
     * @param sc spark context
     */
    public PopModel(JavaPairRDD<String, Map<String, JsonAST.JValue>> fieldsRDD, SparkContext sc) {
        this.fieldsRDD = fieldsRDD;
        this.sc = sc;
    }

    /**
     * Create random rank for all items
     * @param modelName name of model
     * @param eventNames names of events we want to look at
     * @param appName name of app whose events we want to look at
     * @param duration length of time we want to look at
     * @param offsetDate look at events within [offsetDate - duration, offsetDate). Defaults to now() if empty string or invalid syntax.
     * @return JavaPairRDD &lt ItemID, Double &gt
     */
    public JavaPairRDD<String, Double> calc(String modelName, List<String> eventNames, String appName, Integer duration, String offsetDate) {
        // end should always be now except in unusual instances like testing
        DateTime end;
        if (offsetDate.isEmpty()) {
            end = DateTime.now();
        }
        else {
            try {
                end = ISODateTimeFormat.dateTimeParser().parseDateTime(offsetDate);
            } catch (IllegalArgumentException e) {
                logger.warn("Bad end for popModel: " + offsetDate + " using 'now'");
                end = DateTime.now();
            }
        }

        Interval interval = new Interval(end.minusSeconds(duration), end);

        // based on type of popularity model return a set of (item-id, ranking-number) for all items
        logger.info("PopModel " + modelName + " using end: " + end + ", and duration: " + duration + ", interval: " + interval);

        switch (modelName) {
            case RankingType.Popular:
                return calcPopular(appName, eventNames, interval);
            case RankingType.Trending:
                return calcTrending(appName, eventNames, interval);
            case RankingType.Hot:
                return calcHot(appName, eventNames, interval);
            case RankingType.Random:
                return calcRandom(appName, interval);
            case RankingType.UserDefined:
                return getEmptyRDD();
            default:
                logger.warn( "" +
                        "|Bad rankings param type=[$unknownRankingType] in engine definition params, possibly a bad json value.\n" +
                        "|Use one of the available parameter values ($RankingType).");
                return getEmptyRDD();
        }
    }

    /**
     * Create random rank for all items
     * @param appName name of app whose events we want to look at
     * @param interval look at events within this interval
     * @return JavaPairRDD &lt ItemID, Double &rt
     */
    public JavaPairRDD<String, Double> calcRandom(String appName, Interval interval) {
        final JavaRDD<Event> events = eventsRDD(appName, null, interval);
        final JavaRDD<String> actionsRDD = events
                .map(Event::targetEntityId)
                .filter(Option::isDefined)
                .map(Option::get).distinct();
        final JavaRDD<String> itemsRDD = fieldsRDD.map(Tuple2::_1);

        Random rand = new Random();
        return actionsRDD.union(itemsRDD).distinct().mapToPair(itemID -> new Tuple2<String, Double>(itemID, rand.nextDouble()));
    }

    /**
     * Creates rank from the number of named events per item for the duration
     * @param appName name of app whose events we want to look at
     * @param eventNames names of events we want to look at
     * @param interval look at events within this interval
     * @return JavaPairRDD &lt ItemID, Double &gt
     */
    public JavaPairRDD<String, Double> calcPopular(String appName, List<String> eventNames, Interval interval) {
        final JavaRDD<Event> events = eventsRDD(appName, eventNames, interval);
        return events.mapToPair(e -> new Tuple2<String, Integer>(e.targetEntityId().get(), 1))
                .reduceByKey((a,b) -> a + b)
                .mapToPair(t -> new Tuple2<String, Double>(t._1(), (double) t._2()));
    }

    /**
     * Creates a rank for each item by dividing the duration in two and counting named events in both buckets
     * then dividing most recent by less recent. This ranks by change in popularity or velocity of populatiy change.
     * Interval(start, end) end instant is always greater than or equal to the start instant.
     * @param appName name of app whose events we want to look at
     * @param eventNames names of events we want to look at
     * @param interval look at events within this interval
     * @return JavaPairRDD &lt ItemID, Double &gt
     */
    public JavaPairRDD<String, Double> calcTrending(String appName, List<String> eventNames, Interval interval) {
        logger.info("Current Interval: " + interval + ", " + interval.toDurationMillis());
        long halfInterval = interval.toDurationMillis() / 2;
        Interval olderInterval = new Interval(interval.getStart(), interval.getStart().plus(halfInterval));
        logger.info("Older Interval: " + olderInterval);
        Interval newerInterval = new Interval(interval.getStart().plus(halfInterval), interval.getEnd());
        logger.info("Newer Interval: " + newerInterval);

        JavaPairRDD<String, Double> olderPopRDD = calcPopular(appName, eventNames, olderInterval);
        if (!olderPopRDD.isEmpty()) {
            JavaPairRDD<String, Double> newerPopRDD = calcPopular(appName, eventNames, newerInterval);
            return newerPopRDD.join(olderPopRDD)
                    .mapToPair(t -> new Tuple2<String, Double>(t._1, t._2._1 - t._2._2));
        }
        else {
            return getEmptyRDD();
        }

        /* alt way
        final JavaRDD<Event> events = eventsRDD(appName, eventNames, interval);
        return events.mapToPair(e -> new Tuple2<String, Integer>(e.targetEntityId().get(), 1))
                .reduceByKey((a,b) -> a + b)
                .mapToPair(t -> new Tuple2<String, Double>(t._1(), (double) t._2()));
         */
    }

    /**
     * Creates a rank for each item by divding all events per item into three buckets and calculating the change in
     * velocity over time, in other words the acceleration of popularity change.
     * @param appName name of app whose events we want to look at
     * @param eventNames names of events we want to look at
     * @param interval look at events within this interval
     * @return RDD &lt ItemID, Double &gt
     */
    public JavaPairRDD<String, Double> calcHot(String appName, List<String> eventNames, Interval interval) {
        logger.info("Current Interval: " + interval + ", " + interval.toDurationMillis());
        Interval olderInterval = new Interval(interval.getStart(), interval.getStart().plus(interval.toDurationMillis() / 3));
        logger.info("Older Interval: " + olderInterval);
        Interval middleInterval = new Interval(olderInterval.getEnd(), olderInterval.getEnd().plus(olderInterval.toDurationMillis()));
        logger.info("Middle Interval: " + middleInterval);
        Interval newerInterval = new Interval(middleInterval.getEnd(), interval.getEnd());
        logger.info("Newer Interval: " + newerInterval);

        JavaPairRDD<String, Double> olderPopRDD = calcPopular(appName, eventNames, olderInterval);
        if (!olderPopRDD.isEmpty()) {
            JavaPairRDD<String, Double> middlePopRDD = calcPopular(appName, eventNames, middleInterval);
            if (!middlePopRDD.isEmpty()) {
                JavaPairRDD<String, Double> newerPopRDD = calcPopular(appName, eventNames, newerInterval);

                JavaPairRDD<String, Double> newerVelocity = newerPopRDD.join(middlePopRDD)
                        .mapToPair(t -> new Tuple2<String, Double>(t._1, t._2._1 - t._2._2));
                JavaPairRDD<String, Double> olderVelocity = middlePopRDD.join(olderPopRDD)
                        .mapToPair(t -> new Tuple2<String, Double>(t._1, t._2._1 - t._2._2));
                return newerVelocity.join(olderVelocity)
                        .mapToPair(t -> new Tuple2<String, Double>(t._1, t._2._1 - t._2._2));
            }
            else {
                return getEmptyRDD();
            }
        }
        else {
            return getEmptyRDD();
        }
    }

    /**
     * Read events from event store
     * @param appName name of app whose events we want to look at
     * @param eventNames names of events we want to look at
     * @param interval look at events within this interval
     * @return JavaRDD &lt Event &gt
     */
    public JavaRDD<Event> eventsRDD(String appName, List<String> eventNames, Interval interval) {
        logger.info("PopModel getting eventsRDD for startTime: " + interval.getStart() + " and endTime " + interval.getEnd());
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

    /**
     * Generate an empty JavaPairRDD
     * @param <K> type of key in key-value pair
     * @param <V> type of value in key-value pair
     * @return JavaPairRDD &lt K,V &gt
     */
    private <K,V> JavaPairRDD<K,V> getEmptyRDD() {
        ClassTag<Tuple2<K, V>> tag = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDD<Tuple2<K, V>> empty = sc.emptyRDD(tag).toJavaRDD();
        return JavaPairRDD.fromJavaRDD(empty);
    }

}
