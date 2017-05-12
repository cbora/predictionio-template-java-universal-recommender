package org.template;



import static org.junit.Assert.*;


import org.apache.predictionio.core.EventWindow;
import org.apache.predictionio.data.storage.Event;
import org.apache.predictionio.data.store.java.OptionHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.joda.time.DateTime;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * Created by cbora on 3/24/17.
 */
public class DataSourceTest {

    private DataSource source;
    private DataSourceParams params;
    private SparkContext sc;
    private JavaRDD<Event> events;
    private String appName;

    /**
     * Helper function to set environment variables
     * @param key
     * @param value
     */
    public void setEnv(String key, String value) {
        try {
            Map<String, String> env = System.getenv();
            Class<?> cl = env.getClass();
            java.lang.reflect.Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> writableEnv = (Map<String, String>) field.get(env);
            writableEnv.put(key, value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set environment variable", e);
        }
    }

    /**
     * Function to set environment variables
     */
    public void setEnvironmentVariables() {

        // Function to set different environment variables
        setEnv("SPARK_HOME", "$PIO_HOME/vendors/spark-1.5.1-bin-hadoop2.6");
        setEnv("POSTGRES_JDBC_DRIVER", "$PIO_HOME/lib/postgresql-9.4-1204.jdbc41.jar");
        setEnv("MYSQL_JDBC_DRIVER", "$PIO_HOME/lib/mysql-connector-java-5.1.37.jar");
        setEnv("PIO_FS_BASEDIR", "$HOME/.pio_store");
        setEnv("PIO_FS_ENGINESDIR", "$PIO_FS_BASEDIR/engines");
        setEnv("PIO_FS_TMPDIR", "$PIO_FS_BASEDIR/tmp");
        setEnv("PIO_STORAGE_REPOSITORIES_METADATA_NAME", "pio_meta");
        //setEnv("PIO_STORAGE_REPOSITORIES_METADATA_SOURCE", "elasticsearch");
        setEnv("PIO_STORAGE_REPOSITORIES_METADATA_SOURCE", "PGSQL");
        setEnv("PIO_STORAGE_REPOSITORIES_EVENTDATA_NAME", "pio_event");
        setEnv("PIO_STORAGE_REPOSITORIES_EVENTDATA_SOURCE", "PGSQL");
        setEnv("PIO_STORAGE_REPOSITORIES_MODELDATA_NAME", "pio_model");
        setEnv("PIO_STORAGE_REPOSITORIES_MODELDATA_SOURCE", "PGSQL");
        setEnv("PIO_STORAGE_SOURCES_PGSQL_TYPE", "jdbc");
        setEnv("PIO_STORAGE_SOURCES_PGSQL_URL", "jdbc:postgresql://localhost/pio");
        setEnv("PIO_STORAGE_SOURCES_PGSQL_USERNAME", "pio");
        setEnv("PIO_STORAGE_SOURCES_PGSQL_PASSWORD", "pio");
        setEnv("PIO_STORAGE_SOURCES_ELASTICSEARCH_TYPE", "elasticsearch");
        setEnv("PIO_STORAGE_SOURCES_ELASTICSEARCH_CLUSTERNAME", "elasticsearch");
        setEnv("PIO_STORAGE_SOURCES_ELASTICSEARCH_HOSTS", "localhost");
        setEnv("PIO_STORAGE_SOURCES_ELASTICSEARCH_PORTS", "9300");
        setEnv("PIO_STORAGE_SOURCES_ELASTICSEARCH_HOME", "$PIO_HOME/vendors/elasticsearch-1.4.4");
        setEnv("PIO_STORAGE_SOURCES_HBASE_TYPE", "hbase");
        setEnv("PIO_STORAGE_SOURCES_HBASE_HOME", "$PIO_HOME/vendors/hbase-1.0.0");
    }

    /**
     * Helper function to create a new Event object
     * @param name
     * @param targetEntityId
     * @param eventTime
     * @return
     */
    private Event makeEvent(String name, String targetEntityId, DateTime eventTime){
        Event e = new Event(
                OptionHelper.<String>none(),
                name,
                "a",
                "a",
                OptionHelper.<String>none(),
                OptionHelper.some("b"),
                null,
                eventTime,
                null,
                OptionHelper.<String>none(),
                DateTime.now()
        );
        return e;
    }


    @Before
    public void setUp() {
        setEnvironmentVariables();

        // spark context object
        appName = "myApp";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local");
        sc = new SparkContext(conf);

        // create some events
        Event e1 = makeEvent("event1", "a", new DateTime(2));
        Event e2 = makeEvent("event2", "a", new DateTime(10));
        Event e3 = makeEvent("event3", "b", new DateTime(30));
        Event e4 = makeEvent("event4", "c", new DateTime(40));
        Event e5 = makeEvent("event5", "d", new DateTime(50));

        List<Event> eventsList = Arrays.asList(e1, e2, e3, e4, e5);
        Seq<Event> seqEvents = JavaConversions.asScalaBuffer(eventsList).toSeq();
        ClassTag<Event> tag = ClassTag$.MODULE$.apply(Event.class);
        events = sc.parallelize(seqEvents, sc.defaultParallelism(), tag).toJavaRDD();
    }

    @Test
    public void matchAllEvents() throws Exception {
        // Construct DataSourceParams object
        ArrayList<String> eventNames = new ArrayList<String>();
        eventNames.add("event1");
        eventNames.add("event2");
        eventNames.add("event3");
        eventNames.add("event4");
        eventNames.add("event5");
        EventWindow eventWindow = null;
        params = new DataSourceParams(appName, eventNames, eventWindow);
        source = new DataSource(params);
        List<Tuple2<String, JavaPairRDD<String, String>>> results = source.separateEvents(events);
        assertTrue(results.size() == eventNames.size());
    }

    @Test
    public void matchNoEvent() throws Exception {
        ArrayList<String> eventNames = new ArrayList<String>();
        eventNames.add("event100");
        eventNames.add("event101");
        eventNames.add("event102");
        eventNames.add("event103");
        EventWindow eventWindow = null;
        params = new DataSourceParams(appName, eventNames, eventWindow);
        source = new DataSource(params);
        List<Tuple2<String, JavaPairRDD<String, String>>> results = source.separateEvents(events);
        assertTrue(results.size() == 0);
    }

    @Test
    public void matchWithEmptyEventNames() throws Exception {
        ArrayList<String> eventNames = new ArrayList<String>();
        EventWindow eventWindow = null;
        params = new DataSourceParams(appName, eventNames, eventWindow);
        source  = new DataSource(params);
        List<Tuple2<String, JavaPairRDD<String, String>>> results = source.separateEvents(events);
        assertTrue(results.size() == 0);
        //System.out.println()
    }

    @Test
    public void getAppName() throws Exception {
        ArrayList<String> eventNames = new ArrayList<String>();
        EventWindow eventWindow = null;
        params = new DataSourceParams(appName, eventNames, eventWindow);
        source = new DataSource(params);
        String appName = source.getAppName();
        assertTrue(appName.equals(params.getAppName()));
    }

    @Test
    public void getEventWindow() throws Exception {
        ArrayList<String> eventNames = new ArrayList<String>();
        EventWindow eventWindow = null;
        params = new DataSourceParams(appName, eventNames, eventWindow);
        source = new DataSource(params);
        List<Tuple2<String, JavaPairRDD<String, String>>> results = source.separateEvents(events);
        assertNull(source.getEventWindow());
    }

    @After
    public void tearDown() {
        sc.stop();
    }
}