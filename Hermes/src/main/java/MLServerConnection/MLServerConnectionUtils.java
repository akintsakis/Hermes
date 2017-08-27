/*
 * Copyright 2017 thanos.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package MLServerConnection;

import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;

/**
 * @author thanos
 */
public class MLServerConnectionUtils {

    public static void launchServer() {

    }

    public static void buildModel(Model m) {
        Sample s = new Sample();
        s.modelId = m.id;
        s.predictionType = m.predictionType;
        s.fileUrl = m.dataSetPath.replaceAll("/", ";");
        submitJsonObject(s, "build_model");
    }

    public static double classifyInstance(String modelId, String featureVectorCsv) {
        Sample s = new Sample();
        s.modelId = modelId;
        s.featuresVectorCsv = featureVectorCsv;
        String result = submitJsonObject(s, "classify_sample");
        return Double.parseDouble(result);
    }

    public static String submitJsonObject(Sample s, String endPoint) {
        // public static String excutePost(String targetURL, String urlParameters)
        //URL url;
        HttpURLConnection connection = null;

        ///First, all the GSON/JSon stuff up front
        //Gson gson = new Gson();
        //convert java object to JSON format
        //String json = gson.toJson(proj);
        //Then credentials and send string
        //String send_string = usr.getUserEmail()+"+++++"+usr.getUserHash();
//        Feature f1 = new Feature("length", "10");
//        Feature f2 = new Feature("size", "5");
//        Sample s = new Sample();
//        s.features.add(f1);
//        s.features.add(f2);
//        s.target = "setia";
        Gson g = new Gson();
        StringBuilder result = new StringBuilder();
        try {
            //Create connection

            //System.out.println(jsonObject);

            URL url = new URL("http://localhost:5000/" + endPoint + "/" + g.toJson(s));
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
            rd.close();

            //System.out.println(result.toString());
        } catch (Exception e) {
            e.printStackTrace();
            //return null;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
        return result.toString();
    }

}
