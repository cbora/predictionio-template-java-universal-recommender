package org.template;

/**
 * Created by jihunkim on 2/12/17.
 */
public class DefaultURAlgorithmParams {
    public static final int DefaultMaxEventsPerEventType = 500;
    public static final int DefaultNum = 20;
    public static final int DefaultMaxCorrelatorsPerEventType = 50;

    // default number of user history events to use in recs query
    public static final int DefaultMaxQueryEvents = 100;

    // default name for the expire date property of an item
    public static final String DefaultExpireDateName = "expireDate";

    // default name for and item's available after date
    public static final String DefaultAvailableDateName = "availableDate";

    // when using a date range in the query this is the name of the item's date
    public static final String DefaultDateName = "date";

    // use CF + backfill
    public static final String DefaultRecsModel = RecsModel.All;

    public static final RankingParams DefaultRankingParams = new RankingParams();
    public static final String DefaultBackfillFieldName = RankingFieldName.PopRank;
    public static final String DefaultBackfillType = RankingType.Popular;

    // for all time
    public static String DefaultBackfillDuration = "3650 days";

    public static Boolean DefaultReturnSelf = false;

}
