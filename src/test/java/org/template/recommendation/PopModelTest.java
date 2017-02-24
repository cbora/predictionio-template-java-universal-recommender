package org.template.recommendation;

import org.apache.predictionio.data.storage.Event;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.Interval;
import org.junit.Test;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

public class PopModelTest {
    @Test
    public void calc() throws Exception {

    }

    @Test
    public void calcRandom() throws Exception {

    }

    @Test
    public void calcPopular() throws Exception {

    }

    @Test
    public void calcTrending() throws Exception {

    }

    @Test
    public void calcHot() throws Exception {

    }

    @Test
    public void eventsRDD() throws Exception {

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