package org.wbs;

import com.google.gson.Gson;
import spark.ResponseTransformer;
/**
 * Created by cristu on 29/08/16.
 */


public class JsonTransformer implements ResponseTransformer {

    private Gson gson = new Gson();

    @Override
    public String render(Object model) {
        return gson.toJson(model);
    }

}