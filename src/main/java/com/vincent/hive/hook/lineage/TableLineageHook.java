package com.vincent.hive.hook.lineage;

import com.google.gson.stream.JsonWriter;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.LineageLogger;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.tools.LineageInfo;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author created by chenjun at 2020-08-03 18:07
 */
public class TableLineageHook implements ExecuteWithHookContext {
    private static final Logger LOG = LoggerFactory.getLogger(LineageLogger.class);

    private static ObjectMapper objectMapper;
    static {
        objectMapper = new ObjectMapper();
    }
    public void run(HookContext hookContext) throws Exception {
        LOG.info("print hookContext info: [{}] ",writeToJsonStr(hookContext));
        try {
            LineageInfo lep = new LineageInfo();
            QueryPlan plan = hookContext.getQueryPlan();
            String queryStr = plan.getQueryStr().trim();
            lep.getLineageInfo(queryStr);
            StringBuilderWriter out = new StringBuilderWriter(1024);
            JsonWriter writer = new JsonWriter(out);
            writer.beginObject();
            Iterator var3 = lep.getInputTableList().iterator();
            writer.name("inputTables");
            writer.beginArray();
            String tab;
            while(var3.hasNext()) {
                tab = (String)var3.next();
                writer.value(tab);
            }
            writer.endArray();
            writer.name("outputTable");
            writer.beginArray();
            var3 = lep.getOutputTableList().iterator();

            while(var3.hasNext()) {
                tab = (String)var3.next();
                writer.value(tab);
            }
            writer.endArray();
            writer.endObject();
            writer.close();
            LOG.info("print table lineage[{}]",out.toString());
        } catch (Exception e) {
            LOG.info("bad news,we got problem",e);
        }
    }

    private String writeToJsonStr(Object obj) {
        String result = null;
        try {
            result = objectMapper.writeValueAsString(obj);
        } catch (IOException e) {
            LOG.info("convert to json string error",e);
        }
        return result;
    }
}
