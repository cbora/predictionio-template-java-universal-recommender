package org.template;

import com.google.common.base.Optional;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.apache.mahout.math.cf.DownsamplableCrossOccurrenceDataset;
import org.apache.mahout.math.cf.ParOpts;
import org.apache.mahout.math.indexeddataset.IndexedDataset;
import org.apache.predictionio.controller.java.P2LJavaAlgorithm;
import org.apache.predictionio.data.storage.Event;
import org.apache.predictionio.data.storage.NullModel;
import org.apache.predictionio.data.store.java.LJavaEventStore;
import org.apache.predictionio.data.store.java.OptionHelper;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.joda.time.DateTime;
import org.json4s.JsonAST;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.template.indexeddataset.IndexedDatasetJava;
import org.template.similarity.SimilarityAnalysisJava;
import scala.Option;
import scala.Tuple2;
import scala.concurrent.duration.Duration;

import java.util.*;

import static java.util.stream.Collectors.toList;

public class Algorithm extends P2LJavaAlgorithm<PreparedData, NullModel, Query, PredictedResult> {


  private static final Logger logger = LoggerFactory.getLogger(Algorithm.class);
  private final AlgorithmParams ap;

  private final String appName;
  private final String recsModel;
  private final Float userBias;
  private final Float itemBias;
  private final Integer maxQueryEvents;
  private final Integer limit;
  private final List<String> blackListEvents;
  private final Boolean returnSelf;
  private final List<Field> fields;
  private final Integer randomSeed;
  private final Integer maxCorrelatorsPerEventType;
  private final Integer maxEventsPerEventType;
  private final List<String> modelEventNames = new ArrayList<String>();
  private final List<RankingParams> rankingParams;
  private final List<String> rankingFieldNames;
  private final List<String> dateNames;
  private final String esIndex;
  private final String esType;

  //used in getBiasedRecentUserActions
  private List<String> queryEventNames = new ArrayList<>();

  /**
   * Creates cooccurrence, cross-cooccurrence and eventually content correlators with
   * [[org.apache.mahout.math.cf.SimilarityAnalysis]] The analysis part of the recommender is
   * done here but the algorithm can predict only when the coocurrence data is indexed in a
   * search engine like Elasticsearch. This is done in URModel.save.
   *
   * @param ap taken from engine.json to describe limits and event types
   */
  public Algorithm(AlgorithmParams ap) {
    this.ap = ap;
    this.appName = ap.getAppName();
    this.recsModel = ap.getRecsModelOrElse(DefaultURAlgorithmParams.DefaultRecsModel);
    this.userBias = ap.getUserBiasOrElse(1f);
    this.itemBias = ap.getItemBiasOrElse(1f);
    this.maxQueryEvents = ap.getMaxQueryEventsOrElse(
            DefaultURAlgorithmParams.DefaultMaxQueryEvents);
    this.limit = ap.getNumOrElse(DefaultURAlgorithmParams.DefaultNum);
    this.blackListEvents = ap.getBlacklistEvents();
    this.returnSelf = ap.getReturnSelfOrElse(DefaultURAlgorithmParams.DefaultReturnSelf);
    this.fields = ap.getFields();
    this.randomSeed = ap.getSeedOrElse(System.currentTimeMillis()).intValue();
    this.maxCorrelatorsPerEventType = ap.getMaxCorrelatorsPerEventTypeOrElse(
            DefaultURAlgorithmParams.DefaultMaxCorrelatorsPerEventType
    );
    this.maxEventsPerEventType = ap.getMaxEventsPerEventTypeOrElse(
            DefaultURAlgorithmParams.DefaultMaxEventsPerEventType
    );
    if (ap.getIndicators() == null) {
        if (ap.getEventNames() != null) {
          throw new IllegalArgumentException("No eventNames or indicators in engine.json and one of these is required");
        }
        else {
          this.modelEventNames.addAll(ap.getEventNames());
        }
    }
    else {
       for (IndicatorParams ip : ap.getIndicators()){
            this.modelEventNames.add(ip.getName());
        }
    }

    List<RankingParams> defaultRankingParams = new ArrayList<>(Arrays.asList(
        new RankingParams(
            DefaultURAlgorithmParams.DefaultBackfillFieldName,
            DefaultURAlgorithmParams.DefaultBackfillType,
            this.modelEventNames.subList(0, 1),
            null,
            null,
            DefaultURAlgorithmParams.DefaultBackfillDuration
        )
    ));
    this.rankingParams = ap.getRankingsOrElse(defaultRankingParams);
    Collections.sort(this.rankingParams, new RankingParamsComparatorByGroup());
    this.rankingFieldNames = this.rankingParams.stream().map(
        rankingParams -> {
          String rankingType = rankingParams.getBackfillTypeOrElse(
              DefaultURAlgorithmParams.DefaultBackfillType
          );
          return rankingParams.getNameOrElse(
              PopModel.nameByType.get(rankingType)
          );
        }
    ).collect(toList());
    this.dateNames = new ArrayList<>(Arrays.asList(
        ap.getDateName(),
        ap.getAvailableDateName(),
        ap.getExpireDateName()
    )).stream().distinct().collect(toList());
    this.esIndex = ap.getIndexName();
    this.esType = ap.getTypeName();

    // TODO: refer to drawInfo
  }


  class RankingParamsComparatorByGroup implements Comparator<RankingParams> {
    @Override
    public int compare(RankingParams r1, RankingParams r2) {
      int groupComparison = r1.getBackfillType()
          .compareTo(r2.getBackfillType());
      return groupComparison == 0
          ? r1.getName().compareTo(r2.getName())
          : groupComparison;
    }
  }

  private static class BoostableCorrelators {
    public final String actionName;
    public final List<String> itemIDs; // itemIDs
    public final Float boost;

    public BoostableCorrelators(String actionName, List<String> itemIDs,
                                Float boost) {
      this.actionName = actionName;
      this.itemIDs = itemIDs;
      this.boost = boost;
    }

    /**
     * Overrides these two functions to use Hashset in getBoostedMetadata()
     */
    @Override
    public boolean equals(Object obj) {
      BoostableCorrelators other = (BoostableCorrelators) obj;
      return actionName.equals(other.actionName) &&
          itemIDs.equals(other.itemIDs) &&
          boost.equals(other.boost);
    }

    @Override
    public int hashCode() {
      return actionName.hashCode() + itemIDs.hashCode() + boost.hashCode();
    }

    public FilterCorrelators toFilterCorrelators() {
      return new FilterCorrelators(this.actionName, this.itemIDs);
    }
  }

  private static class FilterCorrelators {
    public final String actionName;
    public final List<String> itemIDs;

    public FilterCorrelators(String actionName, List<String> itemIDs) {
      this.actionName = actionName;
      this.itemIDs = itemIDs;
    }
  }

  public NullModel calcAll(SparkContext sc, PreparedData preparedData,
                           Boolean calcPopular) {

    // if data is empty then throw an exception
    if (preparedData.getActions().size() == 0 ||
        preparedData.getActions().get(0)._2().getRowIds().size() == 0) {
      throw new RuntimeException("|There are no users with the primary / conversion event and this is not allowed" +
          "|Check to see that your dataset contains the primary event.");
    }

    logger.info("Actions read now creating correlators");
    List<IndexedDataset> cooccurrenceIDS = new ArrayList<IndexedDataset>();
    List<IndexedDatasetJava> iDs = new ArrayList<>();
    for (Tuple2<String, IndexedDatasetJava> p : preparedData.getActions()) {
      iDs.add(p._2());
    }


    if (ap.getIndicators().size() == 0) {
      cooccurrenceIDS = SimilarityAnalysisJava.cooccurrencesIDSs(
          iDs.toArray(new IndexedDataset[iDs.size()]),
          //random seed
          ap.getSeed() == null ? (int) System.currentTimeMillis() : ap.getSeed().intValue(),
          //maxInterestingItemsPerThing
          ap.getMaxCorrelatorsPerEventType() == null ? DefaultURAlgorithmParams.DefaultMaxCorrelatorsPerEventType
              : ap.getMaxCorrelatorsPerEventType(),
          // maxNumInteractions
          ap.getMaxEventsPerEventType() == null ? DefaultURAlgorithmParams.DefaultMaxEventsPerEventType
              : ap.getMaxEventsPerEventType(),
          defaultParOpts()
      );
    } else {
      // using params per matrix pair, these take the place of eventNames, maxCorrelatorsPerEventType,
      // and maxEventsPerEventType!
      List<IndicatorParams> indicators = ap.getIndicators();
      List<DownsamplableCrossOccurrenceDataset> datasets = new ArrayList<DownsamplableCrossOccurrenceDataset>();
      for (int i = 0; i < iDs.size(); i++) {
        datasets.add(
            new DownsamplableCrossOccurrenceDataset(
                iDs.get(i),
                indicators.get(i).getMaxItemsPerUser() == null ? DefaultURAlgorithmParams.DefaultMaxEventsPerEventType
                    : indicators.get(i).getMaxItemsPerUser(),
                indicators.get(i).getMaxCorrelatorsPerItem() == null ? DefaultURAlgorithmParams.DefaultMaxCorrelatorsPerEventType
                    : indicators.get(i).getMaxCorrelatorsPerItem(),
                OptionHelper.<Object>some(indicators.get(i).getMinLLR()),
                OptionHelper.<ParOpts>some(defaultParOpts())

            )
        );
      }
      cooccurrenceIDS = SimilarityAnalysisJava.crossOccurrenceDownsampled(
          datasets,
          ap.getSeed() == null ? (int) System.currentTimeMillis() : ap.getSeed().intValue());

    }

    List<Tuple2<String, IndexedDataset>> cooccurrenceCorrelators =
        new ArrayList<>();

    for (int i = 0; i < cooccurrenceIDS.size(); i++) {
      cooccurrenceCorrelators.add(new Tuple2<>(
          preparedData.getActions().get(i)._1(),
          cooccurrenceIDS.get(i)
      ));
    }

        JavaPairRDD<String,HashMap<String, JsonAST.JValue>> propertiesRDD;
        if (calcPopular) {
            JavaPairRDD<String, HashMap<String, JsonAST.JValue>> ranksRdd = getRanksRDD(preparedData.getFieldsRDD(), sc);
            propertiesRDD = preparedData.getFieldsRDD().fullOuterJoin(ranksRdd).mapToPair(new CalcAllFunction());
        } else {
            propertiesRDD = RDDUtils.getEmptyPairRDD(sc);
        }

    logger.info("Correlators created now putting into URModel");

    // singleton list for propertiesRdd
    ArrayList<JavaPairRDD<String, HashMap<String, JsonAST.JValue>>> pList = new ArrayList<>();
    pList.add(propertiesRDD);
    new URModel(
        cooccurrenceCorrelators,
        pList,
        getRankingMapping(),
        false,
        sc).save(dateNames, esIndex, esType);
    return new NullModel();
  }

  private Map<String, String> getRankingMapping() {
    HashMap<String, String> out = new HashMap<>();
    for (String r : rankingFieldNames) {
      out.put(r, "float");
    }
    return out;
  }

  /**
   * Lambda expression class for calcPopular in calcAll()
   */
  private static class CalcAllFunction implements
      PairFunction<
          Tuple2<String, Tuple2<Optional<HashMap<String, JsonAST.JValue>>, Optional<HashMap<String, JsonAST.JValue>>>>,
          String,
          HashMap<String, JsonAST.JValue>
          > {
    public Tuple2<String, HashMap<String, JsonAST.JValue>> call(
        Tuple2<String, Tuple2<Optional<HashMap<String, JsonAST.JValue>>,
                Optional<HashMap<String, JsonAST.JValue>>>> t) {

      String item = t._1();
      Optional<HashMap<String, JsonAST.JValue>> oFieldsPropMap = t._2()._1();
      Optional<HashMap<String, JsonAST.JValue>> oRankPropMap = t._2()._2();

      if (oFieldsPropMap.isPresent() && oRankPropMap.isPresent()) {
        HashMap<String, JsonAST.JValue> fieldPropMap = oFieldsPropMap.get();
        HashMap<String, JsonAST.JValue> rankPropMap = oRankPropMap.get();
        HashMap<String, JsonAST.JValue> newMap = new HashMap<>(fieldPropMap);
        newMap.putAll(rankPropMap);
        return new Tuple2<>(item, newMap);
      } else if (oFieldsPropMap.isPresent()) {
        return new Tuple2<>(item, oFieldsPropMap.get());
      } else if (oRankPropMap.isPresent()) {
        return new Tuple2<>(item, oRankPropMap.get());
      } else {
        return new Tuple2<>(item, new HashMap<String, JsonAST.JValue>());
      }
    }
  }

  private ParOpts defaultParOpts() {
    return new ParOpts(-1, -1, true);
  }

  /**
   * Lambda expression class for getRankRDDs()
   */
  private static class RankFunction implements
      PairFunction<
          Tuple2<String, Tuple2<Optional<HashMap<String, JsonAST.JValue>>, Optional<Double>>>,
          String,
          HashMap<String, JsonAST.JValue>
          > {

    private String fieldName;

    public RankFunction(String fieldName) {
      this.fieldName = fieldName;
    }


    public Tuple2<String, HashMap<String, JsonAST.JValue>> call(
        Tuple2<String, Tuple2<Optional<HashMap<String, JsonAST.JValue>>, Optional<Double>>> t) {

      String itemID = t._1();
      Optional<HashMap<String, JsonAST.JValue>> oPropMap = t._2()._1();
      Optional<Double> oRank = t._2()._2();

      if (oPropMap.isPresent() && oRank.isPresent()) {
        HashMap<String, JsonAST.JValue> propMap = oPropMap.get();
        HashMap<String, JsonAST.JValue> newMap = new HashMap<>(propMap);
        newMap.put(fieldName, new JsonAST.JDouble(oRank.get()));
        return new Tuple2<>(itemID, newMap);
      } else if (oPropMap.isPresent()) {
        return new Tuple2<>(itemID, oPropMap.get());
      } else if (oRank.isPresent()) {
        HashMap<String, JsonAST.JValue> newMap = new HashMap<>();
        newMap.put(fieldName, new JsonAST.JDouble(oRank.get()));
        return new Tuple2<>(itemID, newMap);
      } else {
        return new Tuple2<>(itemID, new HashMap<String, JsonAST.JValue>());
      }
    }
  }

  /**
   * Calculate all fields and items needed for ranking.
   *
   * @param fieldsRdd all items with their fields
   * @param sc        the current Spark context
   * @return
   */
  private JavaPairRDD<String, HashMap<String, JsonAST.JValue>> getRanksRDD(
      JavaPairRDD<String, HashMap<String, JsonAST.JValue>> fieldsRdd,
      SparkContext sc
  ) {
    PopModel popModel = new PopModel(fieldsRdd, sc);
    List<Tuple2<String, JavaPairRDD<String, Double>>> rankRDDs = new ArrayList();
    for (RankingParams rp : rankingParams) {
      String rankingType = rp.getBackfillType() == null ? DefaultURAlgorithmParams.DefaultBackfillType
          : rp.getBackfillType();
      String rankingFieldName = rp.getName() == null ? PopModel.nameByType.get(rankingType)
          : rp.getName();
      String durationAsString = rp.getDuration() == null ? DefaultURAlgorithmParams.DefaultBackfillDuration
          : rp.getDuration();
      Integer duration = (int) Duration.apply(durationAsString).toSeconds();
      List<String> backfillEvents = rp.getEventNames() == null ? modelEventNames.subList(0, 1)
          : rp.getEventNames();
      String offsetDate = rp.getOffsetDate();
      JavaPairRDD<String, Double> rankRdd =
          popModel.calc(rankingType, backfillEvents, new EventStore(appName), duration, offsetDate);
      rankRDDs.add(new Tuple2<>(rankingFieldName, rankRdd));
    }

    JavaPairRDD<String, HashMap<String, JsonAST.JValue>> acc = RDDUtils.getEmptyPairRDD(sc);
    // TODO: Is functional [fold & map] more efficient than looping?
    for (Tuple2<String, JavaPairRDD<String, Double>> t : rankRDDs) {
      String fieldName = t._1();
      JavaPairRDD<String, Double> rightRdd = t._2();
      JavaPairRDD joined = acc.fullOuterJoin(rightRdd);
      acc = joined.mapToPair(new RankFunction(fieldName));
    }
    return acc;
  }

  public NullModel calcAll(SparkContext sc, PreparedData preparedData) {
    return calcAll(sc, preparedData, true);
  }

  @Override
  public NullModel train(SparkContext sc, PreparedData preparedData) {
    if (this.recsModel.equals(RecsModel.All))
      return this.calcAll(sc, preparedData);
    else if (this.recsModel.equals(RecsModel.BF))
      return this.calcPop(sc, preparedData);
    else if (this.recsModel.equals(RecsModel.CF)) {
      return this.calcAll(sc, preparedData, false);
    } else {
      throw new IllegalArgumentException(
          String.format("| Bad algorithm param recsModel=[%s] in engine definition params, possibly a bad json value.  |Use one of the available parameter values (%s).",
              this.recsModel, new RecsModel().toString())
      );
    }
  }

  public NullModel calcPop(SparkContext sc, PreparedData data) {
    JavaPairRDD<String, HashMap<String, JsonAST.JValue>> fieldsRDD = data.getFieldsRDD();
    JavaPairRDD<String, HashMap<String, JsonAST.JValue>> ranksRDD = getRanksRDD(fieldsRDD, sc);

    JavaPairRDD<String, HashMap<String, JsonAST.JValue>> currentMetadataRDD =
            EsClient.getInstance().getRDD(esIndex, esType, sc);
    JavaPairRDD<String, HashMap<String, JsonAST.JValue>> propertiesRDD =
            currentMetadataRDD.fullOuterJoin(ranksRDD).mapToPair(new CalcAllFunction());

    // singleton list for propertiesRdd
    ArrayList<JavaPairRDD<String, HashMap<String, JsonAST.JValue>>> pList = new ArrayList<>();
    pList.add(fieldsRDD.cache());
    pList.add(propertiesRDD.cache());
    new URModel(
        new ArrayList<Tuple2<String, IndexedDataset>>(),
        pList,
        getRankingMapping(),
        false,
        sc).save(dateNames, esIndex, esType);
    return new NullModel();
  }


  @Override
  public PredictedResult predict(NullModel model, Query query) {
    logger.info("Query received, user id: ${query.user}, item id: ${query.item}");

    this.queryEventNames = query.getEventNamesOrElse(modelEventNames); // updating the global variable

    Tuple2<String, List<Event>> builtQuery = buildQuery(query);

    SearchHits searchHits = EsClient.getInstance().search(builtQuery._1(), esIndex);
    Boolean withRanks = query.getRankingsOrElse(false);
    List<ItemScore> recs = new ArrayList<>();

    if (searchHits.totalHits() > 0) {
      SearchHit[] hits = searchHits.getHits();
      for (SearchHit hit : hits) {
        if (withRanks) {
          Map<String, Object> source = hit.getSource();
          Map<String, Double> ranks = new HashMap<>();
          for (RankingParams rankingParams : rankingParams) {
            String backFillType = rankingParams.getBackfillTypeOrElse(DefaultURAlgorithmParams.DefaultBackfillType);
            String backfillFieldName = rankingParams.getNameOrElse(backFillType);
            ranks.put(backFillType, Double.parseDouble(source.get(backfillFieldName).toString()));
          }
          if (ranks.size() == 0)
            ranks = null;
          recs.add(new ItemScore(hit.getId(), ((double) hit.getScore()), ranks));

        } else {
          recs.add(new ItemScore(hit.getId(), ((double) hit.getScore()), null));
        }

      }
      logger.info("Results: ${searchHits.getHits.length} retrieved of a possible ${searchHits.totalHits()}");
      return new PredictedResult(recs);
    } else {
      logger.info("No results for query " + query.toString());
      return new PredictedResult(null);
    }
  }

  /**
   * Get recent events of the user on items to create the recommendations query from
   */
  private Tuple2<List<BoostableCorrelators>, List<Event>> getBiasedRecentUserActions(Query query) {
    List<Event> recentEvents = new ArrayList<>();
    try {
      recentEvents =
          LJavaEventStore.findByEntity(
              this.appName,
              "user",
              query.getUser(),
                  OptionHelper.<String>none(),
              Option.apply(queryEventNames),
                  (OptionHelper.<Option<String>>none()),
                  (OptionHelper.<Option<String>> none()),
              OptionHelper.<DateTime>none(),
                  OptionHelper.<DateTime>none(),
              OptionHelper.<Integer>none(),
              true,
              Duration.create(200, "millis")
          );
    } catch (NoSuchElementException ex) {
      logger.info("No user id for recs, returning similar items for the item specified");
    } catch (Exception ex) {
      logger.error("Error when read recent events: \n");
      throw ex;
    }

    Float userEventBias = query.getUserBias();
    Float userEventsBoost;
    if (userEventBias > 0 && userEventBias != 1) {
      userEventsBoost = userEventBias;
    } else
      userEventsBoost = null;

    List<BoostableCorrelators> boostableCorrelators = new ArrayList<>();

    for (String action : queryEventNames) {
      Set<String> items = new HashSet<>();

      for (Event e : recentEvents) {
        if (e.event().equals(action) && items.size() < maxQueryEvents) {
          items.add(e.targetEntityId().toString()); // converting Option<String> to JAVA string, since Event is a native pio scala class
        }
      }
      List<String> stringList = new ArrayList<>(items); // Boostable correlators needs a unique list, .distinct in scala
      boostableCorrelators.add(new BoostableCorrelators(action, stringList, userEventsBoost));
    }

    return new Tuple2<>(boostableCorrelators, recentEvents);
  }

  private List<JsonElement> buildQueryMust(Query query, List<BoostableCorrelators> boostable) {

    List<FilterCorrelators> recentUserHistoryFilter = new ArrayList<>();
    if (userBias < 0.0f) {
      recentUserHistoryFilter = boostable.stream().map(
          correlator -> {
            return correlator.toFilterCorrelators();
          }
      ).collect(toList()).subList(0, maxQueryEvents - 1);
    }

    List<FilterCorrelators> similarItemsFilter = new ArrayList<>();
    if (userBias < 0.0f) {
      similarItemsFilter = getBiasedSimilarItems(query).stream()
          .map(correlator -> correlator.toFilterCorrelators())
          .collect(toList())
          .subList(0, maxQueryEvents - 1);
    }

    List<FilterCorrelators> filteringMetadata = getFilteringMetadata(query);
    List<JsonElement> filteringDateRange = getFilteringDateRange(query);

    List<FilterCorrelators> allFilteringCorrelators = new ArrayList<>(recentUserHistoryFilter);
    allFilteringCorrelators.addAll(similarItemsFilter);
    allFilteringCorrelators.addAll(filteringMetadata);

    GsonBuilder builder = new GsonBuilder();
    Gson gson = builder.create();
    List<JsonElement> mustFields = allFilteringCorrelators.stream().map(
        filterCorrelator -> {
          String actionName = filterCorrelator.actionName;
          List<String> itemIDs = filterCorrelator.itemIDs;

          JsonObject obj = new JsonObject();
          JsonObject innerObj = new JsonObject();

          JsonElement itemIDsObj = gson.toJsonTree(itemIDs, new TypeToken<List<String>>() {
          }.getType());

          innerObj.add(actionName, itemIDsObj);
          obj.add("terms", innerObj);
          obj.addProperty("boost", 0);

          return obj;
        }
    ).collect(toList());

    mustFields.addAll(filteringDateRange);
    return mustFields;
  }

  private List<FilterCorrelators> getFilteringMetadata(Query query) {
    List<Field> paramsFilterFields = this.fields.stream()
        .filter(field -> field.getBias() >= 0.0f)
        .collect(toList());
    List<Field> queryFilterFields = query.getFields().stream()
        .filter(field -> field.getBias() < 0.0f)
        .collect(toList());

    paramsFilterFields.addAll(queryFilterFields);
    List<FilterCorrelators> toReturn = paramsFilterFields.stream()
        .map(field -> new FilterCorrelators(field.getName(), field.getValues()))
        .distinct()
        .collect(toList());
    return toReturn;
  }

  private List<JsonElement> getFilteringDateRange(Query query) {
    DateTime currentDateDate = query.getCurrentDate();
    if (currentDateDate == null)
      currentDateDate = DateTime.now().toDateTimeISO();
    String currentDate = currentDateDate.toString();

    List<JsonElement> json = new ArrayList<>();
    DateRange dr = query.getDateRange();

    if (dr != null && (dr.getAfter() != null || dr.getBefore() != null)) {
      String name = dr.getName();

      DateTime beforeDate = dr.getBefore();
      DateTime afterDate = dr.getAfter();

      String before = beforeDate == null ? "" : beforeDate.toString();
      String after = afterDate == null ? "" : afterDate.toString();

      StringBuilder rangeStart = new StringBuilder()
          .append("\n{\n  \"constant_score\": {\n    \"filter\": {\n")
          .append("      \"range\": {\n        \"")
          .append(name)
          .append("\": {\n");

      String rangeAfter = new StringBuilder()
          .append("\n          \"gt\": \"")
          .append(after)
          .append("\"\n")
          .toString();

      String rangeBefore = new StringBuilder()
          .append("\n          \"lt\": \"")
          .append(before)
          .append("\"\n")
          .toString();

      String rangeEnd = new StringBuilder()
          .append("\n        }\n      }\n    },\n    \"boost\": 0\n  }\n}\n")
          .toString();

      StringBuilder range = rangeStart;
      if (!after.isEmpty()) {
        range.append(rangeAfter);

        if (!before.isEmpty())
          range.append(",");
      }
      if (!before.isEmpty())
        range.append(rangeBefore);

      range.append(rangeEnd);
      JsonElement el = new JsonParser().parse(range.toString());
      json.add(el);
    } else if (ap.getAvailableDateName() != null && ap.getExpireDateName() != null) {
      String availableDate = ap.getAvailableDateName();
      String expireDate = ap.getExpireDateName();

      String available = new StringBuilder()
          .append("\n{\n  \"constant_score\": {\n    \"filter\": {\n")
          .append("      \"range\": {\n        \"")
          .append(availableDate)
          .append("\": {\n          \"lte\": \"")
          .append(currentDate)
          .append("\"\n        }\n      }\n    },\n    \"boost\": 0\n  }\n}\n")
          .toString();

      String expire = new StringBuilder()
          .append("\n{\n  \"constant_score\": {\n    \"filter\": {\n")
          .append("      \"range\": {\n        \"")
          .append(expireDate)
          .append("\": {\n          \"gt\": \"")
          .append(currentDate)
          .append("\"\n        }\n      }\n    },\n    \"boost\": 0\n  }\n}\n")
          .toString();

      JsonElement avEl = new JsonParser().parse(available);
      JsonElement exEl = new JsonParser().parse(expire);
      json.add(avEl);
      json.add(exEl);
    } else {
      logger.info("\nMisconfigured date information, either your engine.json date settings " +
          "or your query's dateRange is incorrect.\n" +
          "Ignoring date information for this query.");
    }

    return json;
  }

  /**
   * Query strucutre :
   * {
   * "size": 0,
   * "query": {
   * "bool":{
   * "should": [{}, {}],
   * "must": [{}, {}],
   * "must_not": [{}, {}],
   * "minimum_should_match": 1
   * }
   * },
   * "sort": [{}, {}]
   * }
   *
   * @param query
   * @return query string , List of events
   */

  private Tuple2<String, List<Event>> buildQuery(Query query) {
    List<String> backfillFieldNames = this.rankingFieldNames; // created during the initialization
    // AlgorithmParams is ap

    try {
      Tuple2<List<BoostableCorrelators>, List<Event>> boostableEvents = getBiasedRecentUserActions(query);
      int numRecs = query.getNumOrElse(DefaultURAlgorithmParams.DefaultNum);
      List<JsonElement> should = buildQueryShould(query, boostableEvents._1());
      List<JsonElement> must = buildQueryMust(query, boostableEvents._1());
      JsonElement mustNot = buildQueryMustNot(query, boostableEvents._2());
      List<JsonElement> sort = buildQuerySort();

      JsonObject jsonQuery = new JsonObject(); // Outer most
      JsonObject innerObj = new JsonObject();
      JsonObject innerMostObject = new JsonObject();

      // add "size" to outer query
      jsonQuery.addProperty("size", numRecs);

      // add "query" to outer query
      jsonQuery.add("query", innerObj);
      innerObj.add("bool", innerMostObject);


      JsonArray shouldJsonArray = new JsonArray();
      if (should != null) should.forEach(shouldJsonArray::add);
      innerMostObject.add("should", shouldJsonArray);

      JsonArray mustJsonArray = new JsonArray();
      if (must != null) must.forEach(mustJsonArray::add);
      innerMostObject.add("must", mustJsonArray);

      innerMostObject.add("must_not", mustNot);

      innerMostObject.addProperty("minimum_should_match", 1);

      // add "sort" to outer query
      JsonArray sortJsonArray = new JsonArray();
      if (sort != null) sort.forEach(sortJsonArray::add);
      jsonQuery.add("sort", sortJsonArray);

      String queryStr = jsonQuery.toString();
      logger.info("Query : " + queryStr);

      return new Tuple2<>(queryStr, boostableEvents._2());
    } catch (IllegalArgumentException ex) {
      logger.debug("IllegalArgumentException" + ex.getMessage());
      return new Tuple2<>("", new ArrayList<>());
    }

  }

  /**
   * Get similar items for an item, these are already in the action correlators in ES
   */
  private List<BoostableCorrelators> getBiasedSimilarItems(Query query) {
    if (query.getItem() != null) {
      Map<String, Object> m = EsClient.getInstance().getSource(esIndex, esType, query.getItem());

      if (m != null) {
        Float itemEventBias = query.getItemBias() == null ? itemBias : query.getItemBias();
        Float itemEventsBoost = (itemEventBias > 0 && itemEventBias != 1) ? itemEventBias : null;

        ArrayList<BoostableCorrelators> out = new ArrayList<>();
        for (String action : modelEventNames) {
          ArrayList<String> items;
          try {
            if (m.containsKey(action) && m.get(action) != null) {
              items = (ArrayList<String>) m.get(action);
            } else {
              items = new ArrayList<>();
            }
          } catch (ClassCastException e) {
            logger.warn("Bad value in item [${query.item}] corresponding to key:" +
                "[$action] that was not a Seq[String] ignored.");
            items = new ArrayList<>();
          }
          List<String> rItems = (items.size() <= maxQueryEvents) ? items : items.subList(0, maxQueryEvents - 1);
          out.add(new BoostableCorrelators(action, rItems, itemEventsBoost));
        }
        return out;
      } else {
        return new ArrayList<>();
      }
    } else {
      return new ArrayList<>();
    }
  }

  /**
   * get all metadata fields that potentially have boosts (not filters)
   */
  private List<BoostableCorrelators> getBoostedMetadata(Query query) {
    ArrayList<Field> paramsBoostedFields = new ArrayList<>();
    for (Field f : fields) {
      if (f.getBias() < 0f) {
        paramsBoostedFields.add(f);
      }
    }

    ArrayList<Field> queryBoostedFields = new ArrayList<>();
    if (query.getFields() != null) {
      for (Field f : query.getFields()) {
        if (f.getBias() >= 0f) {
          queryBoostedFields.add(f);
        }
      }
    }

    Set<BoostableCorrelators> out = new HashSet<>();
    for (Field f : queryBoostedFields) {
      out.add(new BoostableCorrelators(f.getName(), f.getValues(), f.getBias()));
    }
    for (Field f : paramsBoostedFields) {
      out.add(new BoostableCorrelators(f.getName(), f.getValues(), f.getBias()));
    }
    return new ArrayList<>(out);
  }

  /**
   * Build should query part
   */
  private List<JsonElement> buildQueryShould(Query query, List<BoostableCorrelators> boostable) {
    // create a list of all boosted query correlators
    List<BoostableCorrelators> recentUserHistory;
    if (userBias >= 0f) {
      recentUserHistory = boostable.subList(0, maxQueryEvents - 1);
    } else {
      recentUserHistory = new ArrayList<>();
    }

    List<BoostableCorrelators> similarItems;
    if (itemBias >= 0f) {
      similarItems = getBiasedSimilarItems(query);
    } else {
      similarItems = new ArrayList<>();
    }

    List<BoostableCorrelators> boostedMetadata = getBoostedMetadata(query);
    recentUserHistory.addAll(similarItems);
    recentUserHistory.addAll(boostedMetadata);

    ArrayList<JsonElement> shouldFields = new ArrayList<>();
    Gson gson = new Gson();
    for (BoostableCorrelators bc : recentUserHistory) {
      JsonObject obj = new JsonObject();
      JsonObject innerObj = new JsonObject();
      //TODO: does render() in Json4S actually produce a String?
      innerObj.addProperty(bc.actionName, gson.toJson(bc.itemIDs));

      obj.add("terms", innerObj);
      obj.addProperty("boost", bc.boost);
      shouldFields.add(obj);
    }
      
      String shouldScore =
      "{\n" +
      "   \"constant_score\": {\n" +
      "  \"filter\": {\n" +
      "  \"match_all\": {}\n" +
      "},\n" +
      "  \"boost\": 0\n" +
      "  }\n" +
      "}";
      shouldFields.add(
                       new JsonParser().parse(shouldScore).getAsJsonObject()
                       );
      
      return shouldFields;
  }

    /** Build sort query part */
    private List<JsonElement> buildQuerySort(){
        if (recsModel == RecsModel.All || recsModel == RecsModel.BF){
            Gson gson = new Gson();
            List <JsonElement> sortByScore = new ArrayList <JsonElement>();
            List <JsonElement> sortByRanks = new ArrayList <JsonElement>();
            sortByScore.add(new JsonParser().parse("{\"_score\": {\"order\": \"desc\"}}"));
            for(String fieldName:rankingFieldNames){
                sortByRanks.add(new JsonParser().parse("{ \" "+ fieldName + "\": { \"unmapped_type\": \"double\", \"order\": \"desc\" } }"));
            }
            sortByScore.addAll(sortByRanks);
            return sortByScore;
        }
        else{

            return new ArrayList <JsonElement>();
        }
    }
    
    /** Build not must query part */
    private JsonElement buildQueryMustNot(Query query, List<Event> events){
        Gson gson = new Gson();
        JsonObject obj = new JsonObject();
        JsonObject innerObj = new JsonObject();
        
        innerObj.addProperty("values", gson.toJson(getExcludedItems(events,query)));
        obj.add("ids",innerObj);
        obj.addProperty("boost", 0);

        return obj;
    }
    
    /** Create a list of item ids that the user has interacted with or are not to be included in recommendations */
    private List<String> getExcludedItems(List<Event> events, Query query) {
        List<Event> blacklistedItems = new ArrayList<Event>();
        List<String> blacklistedStrings = new ArrayList<String>();
        // either a list or an empty list of filtering events so honor them
        for (Event event : events) {
            if (blackListEvents.isEmpty()) {
                if (event.equals(modelEventNames.get(0))) {
                    blacklistedItems.add(event);
                }
            } else if (blackListEvents.contains(event)) {
                blacklistedItems.add(event);
            }
        }
        for (Event event : blacklistedItems) {
            if (event.targetEntityId().get() != null) {
                blacklistedStrings.add(event.targetEntityId().get());
            } else {
                blacklistedStrings.add("");
            }
        }
        blacklistedStrings.addAll(query.getBlacklistItems().stream().distinct().collect(toList()));
        // Now conditionally add the query item itself
        Boolean includeSelf;
        if (query.getReturnSelf() != null) {
            includeSelf = query.getReturnSelf();
        } else {
            includeSelf = returnSelf;
        }

        if (!includeSelf && (query.getItem() != null)) {
          blacklistedStrings.add(query.getItem());
        }

        List<String> allExcludedStrings = new ArrayList<String>();
        allExcludedStrings.addAll(blacklistedStrings.stream().distinct().collect(toList()));
        return allExcludedStrings;
    }
}