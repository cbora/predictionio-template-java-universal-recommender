package org.template;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for AlgorithmParams class
 */

//    JSON being tested against -
//
// {
//    "appName": "handmade",
//    "indexName": "urindex",
//    "typeName": "items",
//    "recsModel": null,
//    "eventNames": ["purchase", "view"],
//    "blacklistEvents": ["delete"],
//    "maxQueryEvents": "4",
//    "maxEventsPerEventType": "4",
//    "maxCorrelaatorsPerEeventType": "4",
//    "num": "4",
//    "userBias": "5",
//    "itemBias": "5",
//    "returnSelf": "false",
//    "fields": [{
//    "name": "item1",
//    "values": ["val1", "val2"],
//    "bias": 2.0
//    }],
//    "rankings": [{
//    "name": "nameValue",
//    "backFill": "",
//    "eventNames": ["purchase", "view"],
//    "offsetDate": "2017-08-15T11:28:45.114-07:00",
//    "endDate": "2017-08-15T11:28:45.114-07:00",
//    "duration": ""
//    }],
//    "availableDateName": "available",
//    "expireDateName": "expires",
//    "dateName": "date",
//    "indicators": [{
//    "name": "nameValue",
//    "maxItemsPerUser": 4,
//    "maxCorrelatorsPerItem": 4,
//    "minLLR": 4.3
//    }],
//    "seed": "4"
// }

public class AlgorithmParamsTest {

  private Gson gson;
  String paramsJson = "{\n" +
      "\t\"appName\": \"handmade\",\n" +
      "\t\"indexName\": \"urindex\",\n" +
      "\t\"typeName\": \"items\",\n" +
      "\t\"recsModel\": \"\",\n" +
      "\t\"eventNames\": [\"purchase\", \"view\"],\n" +
      "\t\"blacklistEvents\": [\"delete\"],\n" +
      "\t\"maxQueryEvents\": \"4\",\n" +
      "\t\"maxEventsPerEventType\": \"4\",\n" +
      "\t\"maxCorrelaatorsPerEeventType\": \"4\",\n" +
      "\t\"num\": \"4\",\n" +
      "\t\"userBias\": \"5\",\n" +
      "\t\"itemBias\": \"5\",\n" +
      "\t\"returnSelf\": \"false\",\n" +
      "\"fields\": [{\n" +
      "\t\t\"name\": \"item1\",\n" +
      "\"values\": [\"val1\", \"val2\"]," +
      "\t\t\"bias\": 2.0\n" +
      "\t}]," +
      "\"rankings\": [{\n" +
      "\t\t\"name\": \"nameValue\",\n" +
      "\t\t\"backFill\": \"\",\n" +
      "\"eventNames\": [\"purchase\", \"view\"]," +
      "\"offsetDate\": \"2017-08-15T11:28:45.114-07:00\",\n" +
      "\t\t\"endDate\": \"2017-08-15T11:28:45.114-07:00\"," +
      "\t\t\"duration\": \"\"\n" +
      "\t}]," +
      "\t\"availableDateName\": \"available\",\n" +
      "\t\"expireDateName\": \"expires\",\n" +
      "\t\"dateName\": \"date\",\n" +
      "\t\"indicators\": [{\n" +
      "\t\t\"name\": \"nameValue\",\n" +
      "\t\t\"maxItemsPerUser\": 4,\n" +
      "\t\t\"maxCorrelatorsPerItem\": 4,\n" +
      "\t\t\"minLLR\": 4.3\n" +
      "\t}],\n" +
      "\t\"seed\": \"4\"\n" +
      "}";

  @Before
  public void init() {
    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapterFactory(new AlgorithmParamsTypeAdapterFactory());
    this.gson = builder.create();
  }

  @Test
  public void testBasicGetterSetters() {
    AlgorithmParams param = gson.fromJson(paramsJson, AlgorithmParams.class);
    assertEquals("handmade", param.getAppName());
  }

  @Test
  public void testBasics() {
    AlgorithmParams param = gson.fromJson(paramsJson, AlgorithmParams.class);
    assertEquals("default", param.getRecsModelOrElse("default"));
  }

  @Test
  public void testBlacklistEvents() {
    AlgorithmParams param = gson.fromJson(paramsJson, AlgorithmParams.class);
    assertEquals("delete", param.getBlacklistEvents().get(0));
  }


  @Test
  public void testGetReturnSelfOrElse() {
    AlgorithmParams param = gson.fromJson(paramsJson, AlgorithmParams.class);
    assertEquals(false, param.getReturnSelfOrElse(true));
  }

  @Test
  public void testGetFields() {
    AlgorithmParams param = gson.fromJson(paramsJson, AlgorithmParams.class);
    assertEquals(1, param.getFields().size());
    assertEquals(new Float(2), param.getFields().get(0).getBias());
    assertEquals(2, param.getFields().get(0).getValues().size());
    assertEquals("item1", param.getFields().get(0).getName());
  }

  @Test
  public void testGetRankings() {
    AlgorithmParams param = gson.fromJson(paramsJson, AlgorithmParams.class);
    assertEquals(null, param.getRankings().get(0).getBackfillType());
    assertEquals("2017-08-15T11:28:45.114-07:00", param.getRankings().get(0).getEndDate());
    assertEquals("2017-08-15T11:28:45.114-07:00", param.getRankings().get(0).getOffsetDate());
    assertEquals(2, param.getRankings().get(0).getEventNames().size());
  }

  @Test
  public void testGetModelEventNames() {
    AlgorithmParams param = gson.fromJson(paramsJson, AlgorithmParams.class);
    assertEquals(false, param.getReturnSelf());
  }

  @Test
  public void testGetUserBiasOrElse() {
    AlgorithmParams param = gson.fromJson(paramsJson, AlgorithmParams.class);
    assertEquals(new Float(5), param.getUserBiasOrElse(4.0f));
  }

  @Test
  public void testGetNumOrElse() {
    AlgorithmParams param = gson.fromJson(paramsJson, AlgorithmParams.class);
    assertEquals(new Integer(4), param.getNumOrElse(3));
  }

  @Test
  public void testGetItemBiasOrElse() {
    AlgorithmParams param = gson.fromJson(paramsJson, AlgorithmParams.class);
    assertEquals(new Float(5), param.getItemBiasOrElse(4.0f));
  }

  @Test
  public void testGetReturnSelf() {
    AlgorithmParams param = gson.fromJson(paramsJson, AlgorithmParams.class);
    assertEquals("nameValue", param.getModelEventNames().get(0));
  }

  @Test
  public void testGetIndicatorParams() {
    AlgorithmParams param = gson.fromJson(paramsJson, AlgorithmParams.class);
    assertEquals(new Integer(4), param.getIndicators().get(0).getMaxCorrelatorsPerItem());
    assertEquals(new Double(4.3), param.getIndicators().get(0).getMinLLR());
    assertEquals(new Integer(4), param.getIndicators().get(0).getMaxItemsPerUser());
    assertEquals("nameValue", param.getIndicators().get(0).getName());
  }


  @Test
  public void testGetSeed() {
    AlgorithmParams param = gson.fromJson(paramsJson, AlgorithmParams.class);
    assertEquals(new Long(4L), param.getSeedOrElse(0L));
  }

  @Test
  public void toStringTest() {
    AlgorithmParams param = gson.fromJson(paramsJson, AlgorithmParams.class);
    assertEquals("AlgorithmParams{appName: handmadeindexName: urindextypeName: itemsrecsModel: eventNames: [purchase, view]blacklistEvents: [delete]maxQueryEvents: 4maxEventsPerEventType: 4maxCorrelatorsPerEventType: 4num: 4userBias: 5.0itemBias: 5.0returnSelf: falsefields: [Field{, name= item1, values= [val1, val2], bias= 2.0}]rankings: [RankingParams{name: nameValuetype: nulleventNames: [purchase, view]offsetDate: 2017-08-15T11:28:45.114-07:00endDate: 2017-08-15T11:28:45.114-07:00duration: }]availableDateName: availableexpireDateName: expiresdateName: dateindicators: [IndicatorParams{name: nameValuemaxItemsPerUser: 4maxCorrelatorsPerItem: 4minLLR: 4.3}]seed: 4}", param.toString());
  }

  @Test
  public void toConstructorTest() {
    String appName = "handle";
    String indexName = "index";
    String typeName = "typeName";
    String recsModel = "recsModel";

    List<String> eventNames = new ArrayList<>();
    eventNames.add("event1");

    List<String> blacklistEvents = new ArrayList<>();
    blacklistEvents.add("blackListEvent1");

    Integer maxQueryEvents = 10;
    Integer maxEventsPerEventType = 10;
    Integer maxCorrelatorsPerEventType = 10;
    Integer num = 10;
    Float userBias = 10.10f;
    Float itemBias = 10.21f;
    Boolean returnSelf = false;
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("name", new ArrayList<>(), 10.0f));

    List<RankingParams> rankings = new ArrayList<>();
    rankings.add(new RankingParams());

    String availableDateName = "availableDateName";
    String expireDateName = "expireDateName";
    String dateName = "dateName";

    List<IndicatorParams> indicators = new ArrayList<>();
    indicators.add(new IndicatorParams("name", 12, 12, 10.0));

    Long seed = Long.valueOf(100000);

    AlgorithmParams params = new AlgorithmParams(appName,
        indexName,
        typeName,
        recsModel,
        eventNames,
        blacklistEvents,
        maxQueryEvents,
        maxEventsPerEventType,
        maxCorrelatorsPerEventType,
        num,
        userBias,
        itemBias,
        returnSelf,
        fields,
        rankings,
        availableDateName,
        expireDateName,
        dateName,
        indicators,
        seed);

    //Test the constructor
    assertEquals("handle", params.getAppName());
    assertEquals("index", params.getIndexName());
    assertEquals("typeName", params.getTypeName());
    assertEquals("recsModel", params.getRecsModel());
    assertEquals("event1", params.getEventNames().get(0));
    assertEquals("blackListEvent1", params.getBlacklistEvents().get(0));
    assertEquals(new Integer(10), params.getMaxQueryEvents());
    assertEquals(new Integer(10), params.getMaxEventsPerEventType());
    assertEquals(new Integer(10), params.getMaxCorrelatorsPerEventType());
    assertEquals(new Integer(10), params.getNum());
    assertEquals(new Float(10.10), params.getUserBias());
    assertEquals(new Float(10.21), params.getItemBias());
    assertEquals(false, params.getReturnSelf());
    assertEquals("dateName", params.getDateName());
    assertEquals("expireDateName", params.getExpireDateName());
    assertEquals("availableDateName", params.getAvailableDateName());
    assertEquals("name", params.getFields().get(0).getName());
    assertEquals("name", params.getIndicators().get(0).getName());
    assertEquals(new Long(100000), params.getSeed());

  }


  /**
   * {}
   */
  //This is a compulsory field
  @Test(expected = IllegalArgumentException.class)
  public void emptyJsonParams() {
    AlgorithmParams params = gson.fromJson("{}", AlgorithmParams.class);
  }


  /**
   * {
   * "indexName": "urindex",
   * "typeName": "items",
   * "eventNames": ["purchase", "view"],
   * "recsModel": "backfill",
   * "rankings": [{
   * "name": "trendRank",
   * "type": "trending",
   * "eventNames": ["purchase", "view"],
   * "duration": 259200
   * }]
   * }
   */
  //This is a compulsory field  appName
  @Test(expected = IllegalArgumentException.class)
  public void appNameRequiredNotPresent() {
    String minParamsJson = "{\n" +
        "        \"indexName\": \"urindex\",\n" +
        "        \"typeName\": \"items\",\n" +
        "        \"eventNames\": [\"purchase\", \"view\"],\n" +
        "        \"recsModel\": \"backfill\",\n" +
        "        \"rankings\": [{\n" +
        "          \"name\": \"trendRank\",\n" +
        "          \"type\": \"trending\",\n" +
        "          \"eventNames\": [\"purchase\", \"view\"],\n" +
        "          \"duration\": 259200\n" +
        "        }]\n" +
        "      }";
    AlgorithmParams params = gson.fromJson(minParamsJson, AlgorithmParams.class);
  }


  /**
   * {
   * "appName": "handmade",
   * "typeName": "items",
   * "eventNames": ["purchase", "view"],
   * "recsModel": "backfill",
   * "rankings": [{
   * "name": "trendRank",
   * "type": "trending",
   * "eventNames": ["purchase", "view"],
   * "duration": 259200
   * }]
   * }
   */
  //This is a compulsory field  indexName
  @Test(expected = IllegalArgumentException.class)
  public void indexNameRequiredNotPresent() {
    String missingIndexNameParamsJson = "{\n" +
        "        \"appName\": \"handmade\",\n" +
        "        \"typeName\": \"items\",\n" +
        "        \"eventNames\": [\"purchase\", \"view\"],\n" +
        "        \"recsModel\": \"backfill\",\n" +
        "        \"rankings\": [{\n" +
        "          \"name\": \"trendRank\",\n" +
        "          \"type\": \"trending\",\n" +
        "          \"eventNames\": [\"purchase\", \"view\"],\n" +
        "          \"duration\": 259200\n" +
        "        }]\n" +
        "      }";
    AlgorithmParams params = gson.fromJson(missingIndexNameParamsJson, AlgorithmParams.class);
  }

  /**
   * {
   * "appName": "handmade",
   * "eventNames": ["purchase", "view"],
   * "recsModel": "backfill",
   * "rankings": [{
   * "name": "trendRank",
   * "type": "trending",
   * "eventNames": ["purchase", "view"],
   * "duration": 259200
   * }]
   * }
   */
  //This is a compulsory field  indexName
  @Test(expected = IllegalArgumentException.class)
  public void typeNameRequiredNotPresent() {
    String missingIndexNameParamsJson = "{\n" +
        "        \"appName\": \"handmade\",\n" +
        "        \"eventNames\": [\"purchase\", \"view\"],\n" +
        "        \"recsModel\": \"backfill\",\n" +
        "        \"rankings\": [{\n" +
        "          \"name\": \"trendRank\",\n" +
        "          \"type\": \"trending\",\n" +
        "          \"eventNames\": [\"purchase\", \"view\"],\n" +
        "          \"duration\": 259200\n" +
        "        }]\n" +
        "      }";
    AlgorithmParams params = gson.fromJson(missingIndexNameParamsJson, AlgorithmParams.class);
  }


  //This is a compulsory field
  @Test(expected = IllegalArgumentException.class)
  public void eventNamesOrIndicatorsRequiredNotPresent() {
    String minParamsJson = "{\n" +
        "        \"appName\": \"handmade\",\n" +
        "        \"indexName\": \"urindex\",\n" +
        "        \"typeName\": \"items\",\n" +
        "        \"recsModel\": \"backfill\",\n" +
        "        \"rankings\": [{\n" +
        "          \"name\": \"trendRank\",\n" +
        "          \"type\": \"trending\",\n" +
        "          \"eventNames\": [\"purchase\", \"view\"],\n" +
        "          \"duration\": 259200\n" +
        "        }]\n" +
        "      }";
    AlgorithmParams params = gson.fromJson(minParamsJson, AlgorithmParams.class);
  }


}
