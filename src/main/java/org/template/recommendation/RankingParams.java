package org.template.recommendation;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.predictionio.controller.Params;

import java.util.Collections;
import java.util.List;

@AllArgsConstructor
public class RankingParams implements Params {
    @Getter private final String name;
    @Getter private final String backfillType; // See [[org.template.BackfillType]]
    private final List<String> eventNames; // None means use the algo eventNames
    // list, otherwise a list of events
    @Getter private final String offsetDate; // used only for tests,
    // specifies the offset date to start the duration so the most
    // recent date for events going back by from the more recent offsetDate - duration
    @Getter private final String endDate;
    @Getter private final String duration; // duration worth of events to use
    // in calculation of backfill

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

    public String getBackfillTypeOrElse(String defaultValue) {
        return this.backfillType == null
                ? defaultValue
                : this.getBackfillType();
    }

    public List<String> getEventNames() {
        if (this.eventNames == null) {
            return Collections.<String>emptyList();
        } else {
            return this.eventNames;
        }
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
