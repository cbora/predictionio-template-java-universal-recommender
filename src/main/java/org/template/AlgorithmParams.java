package org.template;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.predictionio.controller.Params;

import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;

@AllArgsConstructor
public class AlgorithmParams implements Params {
    @Getter private final String appName; // filled in from engine.json
    @Getter private final String indexName; // can optionally be used to specify the elasticsearch index name
    @Getter private final String typeName; // can optionally be used to specify the elasticsearch type name
    @Getter private final String recsModel;  // "all", "collabFiltering", "backfill"
    private final List<String> eventNames; // names used to ID all user actions
    private final List<String> blacklistEvents;// None means use the primary event, empty array means no filter
    // number of events in user-based recs query
    @Getter private final Integer maxQueryEvents;
    @Getter private final Integer maxEventsPerEventType;
    @Getter private final Integer maxCorrelatorsPerEventType;
    @Getter private final Integer num; // default max # of recs requested
    @Getter private final Float userBias; // will cause the default search engine boost of 1.0
    @Getter private final Float itemBias; // will cause the default search engine boost of 1.0
    @Getter private final Boolean returnSelf; // query building logic defaults this to false
    private final List<Field> fields; //defaults to no fields
    // leave out for default or popular
    private final List<RankingParams> rankings;
    // name of date property field for when the item is available
    @Getter private final String availableDateName;
    // name of date property field for when an item is no longer available
    @Getter private final String expireDateName;
    // used as the subject of a dateRange in queries, specifies the name of the item property
    @Getter private final String dateName;
    private final List<IndicatorParams> indicators; // control params per matrix pair
    @Getter private final Long seed;

    public String getRecsModelOrElse(String defaultValue) {
        return this.recsModel == null
                ? defaultValue
                : this.getRecsModel();
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

    public Integer getMaxEventsPerEventTypeOrElse(Integer defaultValue) {
        return this.maxEventsPerEventType == null
                ? defaultValue
                : this.getMaxEventsPerEventType();
    }

    public Integer getMaxCorrelatorsPerEventTypeOrElse(Integer defaultValue) {
        return this.maxCorrelatorsPerEventType == null
                ? defaultValue
                : this.getMaxCorrelatorsPerEventType();
    }

    public Integer getNumOrElse(Integer defaultValue) {
        return this.num == null ? defaultValue : this.getNum();
    }

    public Float getUserBiasOrElse(Float defaultValue) {
        return this.userBias == null
                ? defaultValue
                : this.getUserBias();
    }

    public Float getItemBiasOrElse(Float defaultValue) {
        return this.itemBias == null
                ? defaultValue
                : this.getItemBias();
    }

    public Boolean getReturnSelfOrElse(Boolean defaultValue) {
        return this.returnSelf == null
                ? defaultValue
                : this.getReturnSelf();
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

    public List<String> getModelEventNames() {
        boolean indicatorsIsEmpty = this.getIndicators().isEmpty();
        if (indicatorsIsEmpty && this.getEventNames().isEmpty()) {
            throw new IllegalArgumentException(
                    "No eventNames or indicators in engine.json " +
                            "and one of these is required");
        } else if (indicatorsIsEmpty) {
            return this.getEventNames();
        } else {
            return this.getIndicators().stream()
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
                "maxCorrelatorsPerEventType: " + this.maxCorrelatorsPerEventType +
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
