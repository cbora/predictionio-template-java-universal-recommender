package org.template.recommendation;


import java.util.ArrayList;
import java.util.List;

import org.apache.predictionio.controller.Params;
import org.apache.predictionio.core.EventWindow;

public class DataSourceParams implements Params{

    private final String appName;  // appName
    private final List<String> eventNames; // List of event names
    private final EventWindow eventWindow;  // event window

    public DataSourceParams(String appName, List<String> eventNames, EventWindow eventWindow){
        this.appName = appName;
        this.eventNames = eventNames;
        this.eventWindow = eventWindow;
    }

    /*
        @return app name
     */
    public String getAppName() {
        return appName;
    }

    /*
       @return event names
     */
    public List<String> getEventNames() {
        return eventNames;
    }

    /*
       @return event window
     */
    public EventWindow getEventWindow() {
        return eventWindow;
    }

}
