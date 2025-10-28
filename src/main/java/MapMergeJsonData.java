

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

public class MapMergeJsonData extends RichMapFunction<JSONObject,JSONObject>{
    @Override
    public JSONObject map(JSONObject data) throws Exception {
        JSONObject resultJson = new JSONObject();

        if(data.containsKey("after") && data.getJSONObject("after") !=null)
        {
            JSONObject after = data.getJSONObject("after");
            JSONObject source = data.getJSONObject("source");
            String db = source.getString("db");
            String schema = source.getString("schema");
            String table = source.getString("table");

            String tableName = "";
            tableName = db + "." + schema + "." + table;

            String op = data.getString("op");

            after.put("table_name",tableName);
            after.put("op",op);

            return after;
        }
        return null;
    }
}
