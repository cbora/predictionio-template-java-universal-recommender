package org.template.recommendation;

import org.apache.predictionio.data.storage.DataMap;
import org.apache.predictionio.data.storage.Event;
import org.apache.predictionio.data.store.java.OptionHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class PopModelTest {
    private IEventStore eventStore;
    private SparkContext sc;
    private PopModel model;

    @Before
    public void setUp() {
        // create events
        Event e1 = makeEvent("e1", "a", new DateTime(100));
        Event e2 = makeEvent("e2", "a", new DateTime(100));
        Event e3 = makeEvent("e3", "b", new DateTime(120));
        Event e4 = makeEvent("e4", "b", new DateTime(80));
        Event e5 = makeEvent("e5", "b", new DateTime(100));
        Event e6 = makeEvent("e6", "c", new DateTime(50));
        Event e7 = makeEvent("e7", "d", new DateTime(150));
        List<Event> events = Arrays.asList(e1, e2, e3, e4, e5, e6, e7);
        eventStore = new MockEventStore(events);

        // create spark context
        SparkConf conf = new SparkConf().setAppName("myApp").setMaster("local");
        sc = new SparkContext(conf);

        // create Pop Model
        model = new PopModel(null, sc);
    }

    @Ignore
    public void calc() throws Exception {

    }

    @Ignore
    public void calcRandom() throws Exception {

    }

    @Test
    public void calcPopular() throws Exception {
        JavaPairRDD<String, Double> rdd;
        Map<String, Double> map;

        rdd = model.calcPopular(eventStore, null, new Interval(0,200));
        map = rdd.collectAsMap();
        assertTrue(map.get("a") == 2.0);
        assertTrue(map.get("b") == 3.0);
        assertTrue(map.get("c") == 1.0);
        assertTrue(map.get("d") == 1.0);

        rdd = model.calcPopular(eventStore, null, new Interval(50,120));
        map = rdd.collectAsMap();
        assertTrue(map.get("a") == 2.0);
        assertTrue(map.get("b") == 2.0);
        assertTrue(map.get("c") == 1.0);
        assertTrue(!map.containsKey("d"));

        rdd = model.calcPopular(eventStore, Arrays.asList("e1", "e2", "e3", "e4", "e6"), new Interval(0,200));
        map = rdd.collectAsMap();
        assertTrue(map.get("a") == 2.0);
        assertTrue(map.get("b") == 2.0);
        assertTrue(map.get("c") == 1.0);
        assertTrue(!map.containsKey("d"));
    }

    @Ignore
    public void calcTrending() throws Exception {

    }

    @Ignore
    public void calcHot() throws Exception {

    }

    @After
    public void tearDown() {
        sc.stop();
    }

    private Event makeEvent(String name, String targetEntityId, DateTime eventTime) {
        Event event = new Event(
                OptionHelper.<String>none(), // eventID
                name, // event
                "", // entityType
                "", // entityID
                OptionHelper.<String>none(),// targetEntityType
                OptionHelper.<String>some(targetEntityId), // targetEntityId
                null, // properties: DataMap = DataMap(), // default empty
                eventTime, // eventTime: DateTime = DateTime.now,
                null, // tags: Seq[String] = Nil,
                OptionHelper.<String>none(), // prId: Option[String] = None,
                DateTime.now()// creationTime: DateTime = DateTime.now
        );
        return event;
    }

    private class MockEventStore implements IEventStore {
        private final List<Event> events;

        public MockEventStore(List<Event> events) {
            this.events = events;
        }

        @Override
        public JavaRDD<Event> eventsRDD(SparkContext sc, List<String> eventNames, Interval interval) {
            List<Event> eventsSubset = new LinkedList<>();

            for (Event event : events) {
                boolean cond = eventNames == null ? true : eventNames.contains(event.event());
                if (cond && event.eventTime().getMillis() >= interval.getStartMillis()
                        && event.eventTime().getMillis() < interval.getEndMillis()) {
                    eventsSubset.add(event);
                }
            }

            Seq<Event> seqEvents = JavaConversions.asScalaBuffer(eventsSubset).toSeq();
            final ClassTag<Event> tag = ClassTag$.MODULE$.apply(Event.class);
            return sc.parallelize(seqEvents, sc.defaultParallelism(), tag).toJavaRDD();
        }

        @Override
        public JavaRDD<Event> eventsRDD(SparkContext sc, Interval interval) {
            return eventsRDD(sc, null, interval);
        }
    }

}