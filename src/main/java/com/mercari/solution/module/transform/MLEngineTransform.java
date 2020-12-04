package com.mercari.solution.module.transform;

import com.google.gson.Gson;
import com.squareup.okhttp.*;

import java.io.IOException;
import java.util.List;

public class MLEngineTransform {

    private Gson gson = new Gson();
    private List<Object> buffer;
    private OkHttpClient client;

    private class Prediction {

    }

    private Prediction sendRequest() throws IOException {
        String json = this.gson.toJson(this.buffer);
        Response response = this.client
                .newCall(new Request.Builder()
                        .post(RequestBody.create(
                                MediaType.parse("application/json"),
                                json))
                        .build()
                ).execute();
        return this.gson.fromJson(response.body().charStream(), Prediction.class);
    }

}
