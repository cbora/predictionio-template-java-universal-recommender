package org.template;

import org.apache.predictionio.data.storage.Event;
import org.apache.predictionio.data.store.java.OptionHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.json4s.JsonAST;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.*;

import static org.junit.Assert.assertTrue;

public class PopModelTest {
    private IEventStore eventStore;
    private SparkContext sc;
    private PopModel model;

    @Before
    public void setUp() {
        // create events
        Event e1 = makeEvent("e1", "a", new DateTime(90));
        Event e2 = makeEvent("e2", "a", new DateTime(110));
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

        // create fieldsRDD
        List<Tuple2<String, HashMap<String, JsonAST.JValue>>> fieldsList = Arrays.asList(
                new Tuple2<String, HashMap<String, JsonAST.JValue>>("a", null),
                new Tuple2<String, HashMap<String, JsonAST.JValue>>("e", null)
        );
        Seq<Tuple2<String, HashMap<String, JsonAST.JValue>>> fieldsSeq = JavaConversions.asScalaBuffer(fieldsList).toSeq();
        ClassTag<Tuple2<String, HashMap<String, JsonAST.JValue>>> tag = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDD<Tuple2<String, HashMap<String, JsonAST.JValue>>> _fieldsRDD = sc.parallelize(fieldsSeq, sc.defaultParallelism(), tag).toJavaRDD();
        JavaPairRDD<String, HashMap<String, JsonAST.JValue>> fieldsRDD = JavaPairRDD.fromJavaRDD(_fieldsRDD);

        // create Pop Model
        model = new PopModel(fieldsRDD, sc);
    }

    @Test
    public void calc() throws Exception {
        JavaPairRDD<String, Double> rdd;

        model.calc(RankingType.Random, Arrays.asList("e1", "e2", "e3", "e4", "e6"), eventStore, 200, "");
        model.calc(RankingType.Popular, Arrays.asList("e1", "e2", "e3", "e4", "e6"), eventStore, 200, "");
        model.calc(RankingType.Trending, Arrays.asList("e1", "e2", "e3", "e4", "e6"), eventStore, 200, "");
        model.calc(RankingType.Hot, Arrays.asList("e1", "e2", "e3", "e4", "e6"), eventStore, 200, "");

        rdd = model.calc(RankingType.UserDefined, Arrays.asList("e1", "e2", "e3", "e4", "e6"), eventStore, 200, "");
        assertTrue(rdd.isEmpty());

        rdd = model.calc("Hello, world", Arrays.asList("e1", "e2", "e3", "e4", "e6"), eventStore, 200, "");
        assertTrue(rdd.isEmpty());
    }

    @Test
    public void calcRandom() throws Exception {
        JavaPairRDD<String, Double> rdd;
        Map<String, Double> map;

        rdd = model.calcRandom(eventStore, new Interval(60, 200));
        map = rdd.collectAsMap();
        assertTrue(map.containsKey("a"));
        System.out.println("************ calc rand: a -> " + map.get("a") + "************");
        assertTrue(map.containsKey("b"));
        System.out.println("************ calc rand: b -> " + map.get("b") + "************");
        assertTrue(!map.containsKey("c"));
        assertTrue(map.containsKey("d"));
        System.out.println("************ calc rand: d -> " + map.get("d") + "************");
        assertTrue(map.containsKey("e"));
        System.out.println("************ calc rand: e -> " + map.get("e") + "************");
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

    @Test
    public void calcTrending() throws Exception {
        JavaPairRDD<String, Double> rdd;
        Map<String, Double> map;

        rdd = model.calcTrending(eventStore, null, new Interval(0,200));
        map = rdd.collectAsMap();
        assertTrue(map.get("a") == 0.0);
        assertTrue(map.get("b") == 1.0);
        assertTrue(!map.containsKey("c"));
        assertTrue(!map.containsKey("d"));

        rdd = model.calcTrending(eventStore, null, new Interval(0, 80));
        assertTrue(rdd.isEmpty());

        rdd = model.calcTrending(eventStore, null, new Interval(100, 300));
        assertTrue(rdd.isEmpty());
    }

    @Test
    public void calcHot() throws Exception {
        JavaPairRDD<String, Double> rdd;
        Map<String, Double> map;

        rdd = model.calcHot(eventStore, null, new Interval(80,140));
        map = rdd.collectAsMap();
        assertTrue(!map.containsKey("a"));
        assertTrue(map.get("b") == 0.0);
        assertTrue(!map.containsKey("c"));
        assertTrue(!map.containsKey("d"));

        rdd = model.calcHot(eventStore, null, new Interval(0, 120));
        assertTrue(rdd.isEmpty());

        rdd = model.calcHot(eventStore, null, new Interval(100, 400));
        assertTrue(rdd.isEmpty());

        rdd = model.calcHot(eventStore, null, new Interval(0, 300));
        assertTrue(rdd.isEmpty());
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
            ClassTag<Event> tag = ClassTag$.MODULE$.apply(Event.class);
            return sc.parallelize(seqEvents, sc.defaultParallelism(), tag).toJavaRDD();
        }

        @Override
        public JavaRDD<Event> eventsRDD(SparkContext sc, Interval interval) {
            return eventsRDD(sc, null, interval);
        }
    }

}