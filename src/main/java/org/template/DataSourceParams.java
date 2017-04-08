package org.template;


import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.predictionio.controller.Params;
import org.apache.predictionio.core.EventWindow;

import java.util.ArrayList;

@AllArgsConstructor
public class DataSourceParams implements Params{
    @Getter private final String appName;  // appName
    @Getter private final ArrayList<String> eventNames; // List of event names
    @Getter private final EventWindow eventWindow;  // event window
}
