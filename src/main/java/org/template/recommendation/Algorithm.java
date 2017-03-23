package org.template.recommendation;

import org.apache.mahout.math.indexeddataset.IndexedDataset;
import org.apache.predictionio.controller.java.P2LJavaAlgorithm;
import org.apache.predictionio.data.storage.NullModel;
import org.apache.predictionio.data.store.java.OptionHelper;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.json4s.JsonAST;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import static java.util.stream.Collectors.toList;
import org.apache.mahout.math.cf.DownsamplableCrossOccurrenceDataset;
import org.apache.mahout.math.cf.ParOpts;
import org.template.recommendation.indexeddataset.IndexedDatasetJava;
import org.template.recommendation.similarity.SimilarityAnalysisJava;
import scala.Tuple2;
import scala.concurrent.duration.Duration;
import com.google.common.base.Optional;
import org.apache.spark.api.java.function.PairFunction;

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
    private final List<String> modelEventNames;
    private final List<RankingParams> rankingParams;
    private final List<String> rankingFieldNames;
    private final List<String> dateNames;
    private final String esIndex;
    private final String esType;

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
        this.modelEventNames = ap.getModelEventNames();

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
        if(preparedData.getActions().size() == 0 ||
           preparedData.getActions().get(0)._2().getRowIds().size() == 0){
            throw new RuntimeException("|There are no users with the primary / conversion event and this is not allowed"+
                    "|Check to see that your dataset contains the primary event.");
        }

        logger.info("Actions read now creating correlators");
        List<IndexedDataset> cooccurrenceIDS = new ArrayList<IndexedDataset>();
        List<IndexedDatasetJava> iDs = new ArrayList<>();
        for (Tuple2<String, IndexedDatasetJava> p : preparedData.getActions()){
            iDs.add(p._2());
        }

        if (ap.getIndicators().size() == 0){
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
        }else{
            // using params per matrix pair, these take the place of eventNames, maxCorrelatorsPerEventType,
            // and maxEventsPerEventType!
            List<IndicatorParams> indicators = ap.getIndicators();
            List<DownsamplableCrossOccurrenceDataset> datasets=new ArrayList<DownsamplableCrossOccurrenceDataset>();
            for (int i=0;i<iDs.size();i++){
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
                    ap.getSeed() == null ?  (int)System.currentTimeMillis(): ap.getSeed().intValue());

        }

        List<Tuple2<String, IndexedDataset>> cooccurrenceCorrelators =
                new ArrayList<>();

        for(int i=0; i<cooccurrenceIDS.size(); i++){
            cooccurrenceCorrelators.add(new Tuple2<>(
                    preparedData.getActions().get(i)._1(),
                    cooccurrenceIDS.get(i)
            ));
        }

        JavaPairRDD<String, Map<String, JsonAST.JValue>> propertiesRDD;
        if (calcPopular) {
            JavaPairRDD<String, Map<String, JsonAST.JValue>> ranksRdd = getRanksRDD(preparedData.getFieldsRDD(), sc);
            propertiesRDD = preparedData.getFieldsRDD().fullOuterJoin(ranksRdd).mapToPair(new CalcAllFunction());
        } else {
            propertiesRDD = RDDUtils.getEmptyPairRDD(sc);
        }

        logger.info("Correlators created now putting into URModel");

        // singleton list for propertiesRdd
        ArrayList<JavaPairRDD<String, Map<String, JsonAST.JValue>>> pList = new ArrayList<>();
        pList.add(propertiesRDD);
        new URModel(
//                cooccurrenceCorrelators,
                null,
                pList,
                getRankingMapping(),
                false,
                sc).save(dateNames, esIndex, esType);
        return new NullModel();
    }

    private Map<String,String> getRankingMapping(){
        HashMap<String, String> out = new HashMap<>();
        for (String r: rankingFieldNames){
            out.put(r, "float");
        }
        return out;
    }

    /**
     * Lambda expression class for calcPopular in calcAll()
     */
    private static class CalcAllFunction implements
            PairFunction<
                    Tuple2<String,Tuple2<Optional<Map<String,JsonAST.JValue>>,Optional<Map<String, JsonAST.JValue>>>>,
                    String,
                    Map<String,JsonAST.JValue>
                    >{
        public Tuple2<String,Map<String, JsonAST.JValue>> call(
                Tuple2<String,Tuple2<Optional<Map<String,JsonAST.JValue>>,Optional<Map<String, JsonAST.JValue>>>> t){

            String item = t._1();
            Optional<Map<String, JsonAST.JValue>> oFieldsPropMap = t._2()._1();
            Optional<Map<String, JsonAST.JValue>> oRankPropMap = t._2()._2();

            if (oFieldsPropMap.isPresent() && oRankPropMap.isPresent()){
                Map<String, JsonAST.JValue> fieldPropMap = oFieldsPropMap.get();
                Map<String, JsonAST.JValue> rankPropMap = oRankPropMap.get();
                HashMap<String, JsonAST.JValue> newMap = new HashMap<>(fieldPropMap);
                newMap.putAll(rankPropMap);
                return new Tuple2<>(item, newMap);
            } else if (oFieldsPropMap.isPresent()){
                return new Tuple2<>(item,  oFieldsPropMap.get());
            } else if (oRankPropMap.isPresent()){
                return new Tuple2<>(item,  oRankPropMap.get());
            } else{
                return new Tuple2<>(item, new HashMap<String, JsonAST.JValue>());
            }
        }
    }

    private ParOpts defaultParOpts(){
        return new ParOpts(-1, -1, true);
    }

    /**
     * Lambda expression class for getRankRDDs()
     */
    private static class RankFunction implements
            PairFunction<
                    Tuple2<String,Tuple2<Optional<Map<String, JsonAST.JValue>>,Optional<Double>>>,
                    String,
                    Map<String, JsonAST.JValue>
                            >{

        private String fieldName;
        public RankFunction(String fieldName){
            this.fieldName = fieldName;
        }


        public Tuple2<String,Map<String, JsonAST.JValue>> call(
                Tuple2<String,Tuple2<Optional<Map<String, JsonAST.JValue>>,Optional<Double>>> t){

            String itemID = t._1();
            Optional<Map<String, JsonAST.JValue>> oPropMap = t._2()._1();
            Optional<Double> oRank = t._2()._2();

            if (oPropMap.isPresent() && oRank.isPresent()){
                Map<String, JsonAST.JValue> propMap = oPropMap.get();
                HashMap<String, JsonAST.JValue> newMap = new HashMap<>(propMap);
                newMap.put(fieldName, new JsonAST.JDouble(oRank.get()) );
                return new Tuple2<>(itemID, newMap);
            } else if (oPropMap.isPresent()){
                return new Tuple2<>(itemID, oPropMap.get());
            } else if (oRank.isPresent()){
                HashMap<String, JsonAST.JValue> newMap = new HashMap<>();
                newMap.put(fieldName, new JsonAST.JDouble(oRank.get()));
                return new Tuple2<>(itemID, newMap);
            } else{
                return new Tuple2<>(itemID, new HashMap<String, JsonAST.JValue>());
            }
        }
    }

    /** Calculate all fields and items needed for ranking.
     *
     *  @param fieldsRDD all items with their fields
     *  @param sc the current Spark context
     *  @return
     */
    private JavaPairRDD<String, Map<String, JsonAST.JValue>> getRanksRDD(
            JavaPairRDD<String, Map<String, JsonAST.JValue>> fieldsRdd,
            SparkContext sc
            ){
        PopModel popModel = new PopModel(fieldsRdd, sc);
        List<Tuple2<String, JavaPairRDD<String, Double>>> rankRDDs = new ArrayList();
        for (RankingParams rp : rankingParams){
            String rankingType = rp.getBackfillType() == null ? DefaultURAlgorithmParams.DefaultBackfillType
                                : rp.getBackfillType();
            String rankingFieldName = rp.getName() == null ? PopModel.nameByType.get(rankingType)
                                      : rp.getName();
            String durationAsString = rp.getDuration() == null ? DefaultURAlgorithmParams.DefaultBackfillDuration
                                      : rp.getDuration();
            Integer duration =  (int) Duration.apply(durationAsString).toSeconds();
            List<String> backfillEvents = rp.getEventNames() == null ? modelEventNames.subList(0,1)
                                          : rp.getEventNames();
            String offsetDate = rp.getOffsetDate();
            JavaPairRDD<String, Double> rankRdd =
                    popModel.calc(rankingType, backfillEvents, new EventStore(appName), duration, offsetDate);
            rankRDDs.add(new Tuple2<>(rankingFieldName, rankRdd));
        }

        JavaPairRDD<String, Map<String, JsonAST.JValue>> acc = RDDUtils.getEmptyPairRDD(sc);
        // TODO: Is functional [fold & map] more efficient than looping?
        for (Tuple2<String, JavaPairRDD<String, Double>> t : rankRDDs){
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
        if (this.recsModel.equals(RecsModel.All) ||
                this.recsModel.equals(RecsModel.BF)) {
            return this.calcAll(sc, preparedData);
        } else if (this.recsModel.equals(RecsModel.CF)) {
            return this.calcAll(sc, preparedData, false);
        } else {
            throw new IllegalArgumentException(
                    String.format("| Bad algorithm param recsModel=[%s] in engine definition params, possibly a bad json value.  |Use one of the available parameter values (%s).",
                    this.recsModel, new RecsModel().toString())
            );
        }
    }

    public NullModel calcPop(SparkContext sc, PreparedData data) {
        throw new RuntimeException("Not yet implemented; waiting on engine" +
                " team to modify PreparedData");

    }

    @Override
    public PredictedResult predict(NullModel model, Query query) {
        List<String> queryEventNames = query.getEventNamesOrElse(modelEventNames);
        throw new RuntimeException("Not yet implemented");
    }
}
