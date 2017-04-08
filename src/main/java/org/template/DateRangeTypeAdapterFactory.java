package org.template;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DateRangeTypeAdapterFactory implements TypeAdapterFactory {
  public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> typeToken) {
    if (typeToken.getRawType() != DateRange.class) {
      return null;
    } else {
      return (TypeAdapter<T>) new DateRangeTypeAdapter();
    }
  }

  private class DateRangeTypeAdapter extends TypeAdapter<DateRange> {
    @Override
    public DateRange read(final JsonReader in) throws IOException {
      Map<String, String> fields = new HashMap<>();
      fields.put("name", null);
      fields.put("beforeDate", null);
      fields.put("afterDate", null);

      in.beginObject();
      while(in.hasNext()) {
        fields.put(in.nextName(), in.nextString());
      }
      in.endObject();

      return new DateRange(fields.get("name"), fields.get("beforeDate"),
              fields.get("afterDate"));
    }

    @Override
    public void write(JsonWriter out, DateRange dateRange) throws IOException {
      out.beginObject();
      out.name("name").value(dateRange.getName());

      if (dateRange.getBeforeDate() != null) {
        out.name("beforeDate").value(dateRange.getBeforeDate().toString());
      }

      if (dateRange.getAfterDate() != null) {
        out.name("afterDate").value(dateRange.getAfterDate().toString());
      }
      out.endObject();
    }

  }
}
