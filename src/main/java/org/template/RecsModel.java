package org.template;

/**
 * Created by jihunkim on 2/12/17.
 */
public class RecsModel {
    public static final String All = "all";
    public static final String CF = "collabFiltering";
    public static final String BF = "backfill";

    @Override
    public String toString() {
        return All + " | " + CF + " | " + BF;
    }
}
