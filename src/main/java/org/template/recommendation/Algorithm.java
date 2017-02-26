package org.template.recommendation;

import org.apache.predictionio.controller.java.P2LJavaAlgorithm;
import org.apache.predictionio.data.storage.NullModel;
import org.apache.spark.SparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        logger.info("Actions read now creating correlators");
        throw new RuntimeException("Not yet implemented; waiting on algo team");

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
