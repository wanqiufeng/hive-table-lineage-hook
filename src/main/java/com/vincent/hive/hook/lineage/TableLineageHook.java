package com.vincent.hive.hook.lineage;

import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.tools.LineageInfo;

/**
 * @author created by chenjun at 2020-08-03 18:07
 */
public class TableLineageHook implements ExecuteWithHookContext {

    public void run(HookContext hookContext) throws Exception {
        LineageInfo lep = new LineageInfo();
        QueryPlan plan = hookContext.getQueryPlan();
        String queryStr = plan.getQueryStr().trim();
        lep.getLineageInfo(queryStr);
    }
}
