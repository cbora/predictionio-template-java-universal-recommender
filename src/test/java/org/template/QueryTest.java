package org.template;

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class QueryTest {

    @Test
    public void constructorTest() throws Exception {
      String user = "user"; // must be a user or item id
      Float userBias = 0.0f; // default: whatever is in algorithm params or 1
      String item = "item"; // must be a user or item id
      Float itemBias = 0.0f; // default: whatever is in algorithm params or 1
      List<Field> fields = new LinkedList<>(); // default: whatever is in algorithm params or None
      String currentDate = "20170327"; // if used will override dateRange filter, currentDate must lie between the item's
      // expireDateName value and availableDateName value, all are ISO 8601 dates
      DateRange dateRange; // optional before and after filter applied to a date field
      List<String> blacklistItems = new LinkedList<>(); // default: whatever is in algorithm params or None
      Boolean returnSelf; // means for an item query should the item itself be returned, defaults
      // to what is in the algorithm params or false
      Integer num; // default: whatever is in algorithm params, which itself has a default--probably 20
      List<String> eventNames; // names used to ID all user actions
      Boolean withRanks; // Add to ItemScore rank fields values, default false


    }

    @Test
    public void getUser() throws Exception {


    }

    @Test
    public void getUserBias() throws Exception {

    }

    @Test
    public void getItem() throws Exception {

    }

    @Test
    public void getItemBias() throws Exception {

    }

    @Test
    public void getFields() throws Exception {

    }

    @Test
    public void getCurrentDate() throws Exception {

    }

    @Test
    public void getDateRange() throws Exception {

    }

    @Test
    public void getBlacklistItems() throws Exception {

    }

    @Test
    public void isReturnSelf() throws Exception {

    }

    @Test
    public void getNum() throws Exception {

    }

    @Test
    public void getEventNames() throws Exception {

    }

    @Test
    public void isWithRanks() throws Exception {

    }

}