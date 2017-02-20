package org.template.recommendation;

import org.apache.predictionio.controller.Params;

import java.util.Collections;
import java.util.List;
import static java.util.stream.Collectors.toList;

public class AlgorithmParams implements Params {
    private final String appName; // filled in from engine.json
    private final String indexName; // can optionally be used to specify the elasticsearch index name
    private final String typeName; // can optionally be used to specify the elasticsearch type name
    private final String recsModel;  // "all", "collabFiltering", "backfill"
    private final List<String> eventNames; // names used to ID all user actions
    private final List<String> blacklistEvents;// None means use the primary event, empty array means no filter
    // number of events in user-based recs query
    private final Integer maxQueryEvents;
    private final Integer maxEventsPerEventType;
    private final Integer maxCorrelatorsPerEventType;
    private final Integer num; // default max # of recs requested
    private final Float userBias; // will cause the default search engine boost of 1.0
    private final Float itemBias; // will cause the default search engine boost of 1.0
    private final Boolean returnSelf; // query building logic defaults this to false
    private final List<Field> fields; //defaults to no fields
    // leave out for default or popular
    private final List<RankingParams> rankings;
    // name of date property field for when the item is available
    private final String availableDateName;
    // name of date property field for when an item is no longer available
    private final String expireDateName;
    // used as the subject of a dateRange in queries, specifies the name of the item property
    private final String dateName;
    private final List<IndicatorParams> indicators; // control params per matrix pair
    private final Long seed;

    public AlgorithmParams(String appName, String indexName,
                           String typeName, String recsModel,
                           List<String> eventNames,
                           List<String> blacklistEvents,
                           Integer maxQueryEvents,
                           Integer maxEventsPerEventType,
                           Integer maxCorrelatorsPerEventType,
                           Integer num, Float userBias,
                           Float itemBias,
                           Boolean returnSelf,
                           List<Field> fields,
                           List<RankingParams> rankings,
                           String availableDateName,
                           String expireDateName,
                           String dateName,
                           List<IndicatorParams> indicators,
                           Long seed) {
        this.appName = appName;
        this.indexName = indexName;
        this.typeName = typeName;
        this.recsModel = recsModel;
        this.eventNames = eventNames;
        this.blacklistEvents = blacklistEvents;
        this.maxQueryEvents = maxQueryEvents;
        this.maxEventsPerEventType = maxEventsPerEventType;
        this.maxCorrelatorsPerEventType = maxCorrelatorsPerEventType;
        this.num = num;
        this.userBias = userBias;
        this.itemBias = itemBias;
        this.returnSelf = returnSelf;
        this.fields = fields;
        this.rankings = rankings;
        this.availableDateName = availableDateName;
        this.expireDateName = expireDateName;
        this.dateName = dateName;
        this.indicators = indicators;
        this.seed = seed;
    }

    public String getAppName() {
        return this.appName;
    }

    public String getIndexName() {
        return this.indexName;
    }

    public String getTypeName() {
        return this.typeName;
    }

    public String getRecsModelOrElse(String defaultValue) {
        return this.recsModel == null
                ? defaultValue
                : this.getRecsModel();
    }

    public String getRecsModel() {
        return this.recsModel;
    }

    public List<String> getEventNames() {
        return this.eventNames == null
                ? Collections.<String>emptyList()
                : this.eventNames;
    }

    public List<String> getBlacklistEvents() {
        return this.blacklistEvents == null
                ? Collections.<String>emptyList()
                : this.blacklistEvents;
    }

    public Integer getMaxQueryEventsOrElse(Integer defaultValue) {
        return this.maxQueryEvents == null
                ? defaultValue
                : this.getMaxQueryEvents();
    }

    public Integer getMaxQueryEvents() {
        return this.maxQueryEvents;
    }

    public Integer getMaxEventsPerEventTypeOrElse(Integer defaultValue) {
        return this.maxEventsPerEventType == null
                ? defaultValue
                : this.getMaxEventsPerEventType();
    }
    public Integer getMaxEventsPerEventType() {
        return this.maxEventsPerEventType;
    }

    public Integer getMaxCorrelatorsPerEventTypeOrElse(Integer defaultValue) {
        return this.maxCorrelatorsPerEventType == null
                ? defaultValue
                : this.getMaxCorrelatorsPerEventType();
    }
    public Integer getMaxCorrelatorsPerEventType() {
        return this.maxCorrelatorsPerEventType;
    }

    public Integer getNumOrElse(Integer defaultValue) {
        return this.num == null ? defaultValue : this.getNum();
    }

    public Integer getNum() {
        return this.num;
    }

    public Float getUserBiasOrElse(Float defaultValue) {
        return this.userBias == null
                ? defaultValue
                : this.getUserBias();
    }

    public Float getUserBias() {
        return this.userBias;
    }

    public Float getItemBiasOrElse(Float defaultValue) {
        return this.itemBias == null
                ? defaultValue
                : this.getItemBias();
    }

    public Float getItemBias() {
        return this.itemBias;
    }

    public Boolean getReturnSelfOrElse(Boolean defaultValue) {
        return this.returnSelf == null
                ? defaultValue
                : this.getReturnSelf();
    }

    public Boolean getReturnSelf() {
        return this.returnSelf;
    }

    public List<Field> getFields() {
        return this.fields == null
                ? Collections.<Field>emptyList()
                : this.fields;
    }

    public List<RankingParams> getRankingsOrElse(List<RankingParams> defaultValue) {
        return this.rankings == null
                ? defaultValue
                : this.getRankings();
    }

    public List<RankingParams> getRankings() {
        return this.rankings == null
                ? Collections.<RankingParams>emptyList()
                : this.rankings;
    }

    public String getAvailableDateName() {
        return this.availableDateName;
    }

    public String getExpireDateName() {
        return this.expireDateName;
    }

    public String getDateName() {
        return this.dateName;
    }

    public List<String> getModelEventNames() {
        boolean indicatorsIsEmpty = this.getIndicators().isEmpty();
        if (indicatorsIsEmpty && this.getEventNames().isEmpty()) {
            throw new IllegalArgumentException(
                    "No eventNames or indicators in engine.json " +
                            "and one of these is required");
        } else if (indicatorsIsEmpty) {
            return this.getEventNames();
        } else {
            this.getIndicators().stream()
                    .map(indicatorParams -> indicatorParams.getName())
                    .collect(toList());
        }
    }

    public List<IndicatorParams> getIndicators() {
        return this.indicators == null
                ? Collections.<IndicatorParams>emptyList()
                : this.indicators;
    }

    public Long getSeedOrElse(Long defaultValue) {
        return this.seed == null
                ? defaultValue
                : this.getSeed();
    }

    public Long getSeed() {
        return this.seed;
    }

    @Override
    public String toString() {
        return "AlgorithmParams{" +
                "appName: " + this.appName +
                "indexName: " + this.indexName +
                "typeName: " + this.typeName +
                "recsModel: " + this.recsModel +
                "eventNames: " + this.eventNames +
                "blacklistEvents: " + this.blacklistEvents +
                "maxQueryEvents: " + this.maxQueryEvents +
                "maxEventsPerEventType: " + this.maxEventsPerEventType +
                "maxCorrelaatorsPerEeventType: " + this.maxCorrelatorsPerEventType +
                "num: " + this.num +
                "userBias: " + this.userBias +
                "itemBias: " + this.itemBias +
                "returnSelf: " + this.returnSelf +
                "fields: " + this.fields +
                "rankings: " + this.rankings +
                "availableDateName: " + this.availableDateName +
                "expireDateName: " + this.expireDateName +
                "dateName: " + this.dateName +
                "indicators: " + this.indicators +
                "seed: " + this.seed +
                '}';
    }
}
