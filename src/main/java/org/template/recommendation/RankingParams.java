package org.template.recommendation;

import org.apache.predictionio.controller.Params;

import java.util.Collections;
import java.util.List;

public class RankingParams implements Params {
    private final String name;
    private final String backfillType; // See [[org.template.BackfillType]]
    private final List<String> eventNames; // None means use the algo eventNames
    // list, otherwise a list of events
    private final String offsetDate; // used only for tests,
    // specifies the offset date to start the duration so the most
    // recent date for events going back by from the more recent offsetDate - duration
    private final String endDate;
    private final String duration; // duration worth of events to use
    // in calculation of backfill

    public RankingParams(String name, String backfillType,
                         List<String> eventNames, String offsetDate,
                         String endDate, String duration) {
        this.name = name;
        this.backfillType = backfillType;
        this.eventNames = eventNames;
        this.offsetDate = offsetDate;
        this.endDate = endDate;
        this.duration = duration;
    }

    public RankingParams() {
        this(
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    public String getNameOrElse(String defaultValue) {
        return this.name == null
                ? defaultValue
                : this.getName();
    }
    public String getName() {
        return this.name;
    }

    public String getBackfillTypeOrElse(String defaultValue) {
        return this.backfillType == null
                ? defaultValue
                : this.getBackfillType();
    }

    public String getBackfillType() {
        return this.backfillType;
    }

    public List<String> getEventNames() {
        if (this.eventNames == null) {
            return Collections.<String>emptyList();
        } else {
            return this.eventNames;
        }
    }

    public String getOffsetDate() {
        return this.offsetDate;
    }

    public String getEndDate() {
        return this.endDate;
    }

    public String getDuration() {
        return this.duration;
    }

    @Override
    public String toString() {
        return "RankingParams{" +
                "name: " + this.name +
                "type: " + this.backfillType +
                "eventNames: " + this.eventNames +
                "offsetDate: " + this.offsetDate +
                "endDate: " + this.endDate +
                "duration: " + this.duration
                + "}";
    }
}
