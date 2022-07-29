package com.holden;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import com.alibaba.fastjson.JSONObject;

/**
 * @ClassName spider_flink-MyCustomerDeserialization
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年7月10日23:39 - 周日
 * @Describe
 */
public class MyCustomerDeserialization implements DebeziumDeserializationSchema<String> {
    /*
     * 时间格式:
     * "database":"",
     * "tableName":"",
     * "type":"c u d",
     * "before":{"id":"","tm_name":""...}
     * "after":{"":"","":""...}
     * "ts":12412412
     *
     * 我们需要的格式：
     * {"database":"xxx","data":{"name":"flower","id":2},"type":"xxx","table":"xxx"}
     * */

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //Step-1 创建JSON对象用于存储最终数据
        JSONObject result = new JSONObject();


        //Step-2 获取主题信息，包含数据库和表名
        String topic = sourceRecord.topic();
        String[] arr = topic.split("\\.");
        String db = arr[1];//获取数据库名
        String tableName = arr[2];//获取表名


        //Step-5 获取after数据
        Struct value2 = (Struct) sourceRecord.value();
        Struct after = value2.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            for (Field field : after.schema().fields()) {
                afterJson.put(field.name(), after.get(field.name()));
            }
        }

        //Step-6 获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type) || "read".equals(type)) {
            type = "insert";
        }

        //Step-7 将字段写入JSON对象
        result.put("database", db);
        result.put("table", tableName);
        result.put("type", type);

        //如果是delete操作再读取数据,不用提早读取
        if ("delete".equals(type)) {
            Struct before = value2.getStruct("before");
            JSONObject beforeJson = new JSONObject();
            if (before != null) {
                for (Field field : before.schema().fields()) {
                    beforeJson.put(field.name(), before.get(field.name()));
                }
            }
            result.put("data", beforeJson);
        } else {
            result.put("data", afterJson);
        }

        //Step- 输出
        collector.collect(result.toString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
