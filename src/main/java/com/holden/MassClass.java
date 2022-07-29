package com.holden;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @ClassName spider_flink-MassClass
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年7月28日9:43 - 周四
 * @Describe
 */
public class MassClass {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //将状态保存在文件中
        //env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///T:\\ShangGuiGu\\FlinkDemo\\output\\statebackend"));
        DebeziumSourceFunction<String> mysql = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("spider_base")
                .tableList("spider_base.ods_stock_step_one")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyCustomerDeserialization())
                .build();

        DataStreamSource<String> mysqlDS = env.addSource(mysql);

        KeyedStream<OdsStock, String> keyedStream = mysqlDS.map(x -> {
            JSONObject jsonObject = JSONObject.parseObject(x);
            JSONObject data = jsonObject.getJSONObject("data");
            return OdsStock.builder()
                    .code(data.getString("code"))
                    .name(data.getString("name"))
                    .opening_price(data.getBigDecimal("opening_price"))
                    .closing_price(data.getBigDecimal("closing_price"))
                    .last_closing(data.getBigDecimal("last_closing"))
                    .ds(data.getString("ds"))
                    .deal_amount(data.getBigDecimal("deal_amount"))
                    .rk(data.getInteger("rk"))
                    .hhv(data.getBigDecimal("hhv"))
                    .llv(data.getBigDecimal("llv"))
                    .rsv(data.getBigDecimal("rsv"))
                    .highest(data.getBigDecimal("highest"))
                    .lowest(data.getBigDecimal("lowest"))
                    .sar_high(data.getBigDecimal("sar_high"))
                    .sar_low(data.getBigDecimal("sar_low"))
                    .tr(data.getBigDecimal("tr"))
                    .dmm(data.getBigDecimal("dmm"))
                    .dmp(data.getBigDecimal("dmp"))
                    .closing_diff(data.getBigDecimal("closing_diff"))
                    .ma3(data.getBigDecimal("ma3"))
                    .ma5(data.getBigDecimal("ma5"))
                    .ma10(data.getBigDecimal("ma10"))
                    .ma12(data.getBigDecimal("ma12"))
                    .ma20(data.getBigDecimal("ma20"))
                    .ma24(data.getBigDecimal("ma24"))
                    .ma50(data.getBigDecimal("ma50"))
                    .ma60(data.getBigDecimal("ma60"))
                    .bbi(data.getBigDecimal("bbi"))
                    .wr6(data.getBigDecimal("wr6"))
                    .wr10(data.getBigDecimal("wr10"))
                    .bias6(data.getBigDecimal("bias6"))
                    .bias12(data.getBigDecimal("bias12"))
                    .bias24(data.getBigDecimal("bias24"))
                    .bias36(data.getBigDecimal("bias36"))
                    .roc(data.getBigDecimal("roc"))
                    .maroc(data.getBigDecimal("maroc"))
                    .asi(data.getBigDecimal("asi"))
                    .ene(data.getBigDecimal("ene"))
                    .psy(data.getBigDecimal("psy"))
                    .psyma(data.getBigDecimal("psyma"))
                    .br(data.getBigDecimal("br"))
                    .ar(data.getBigDecimal("ar"))
                    .mass(data.getBigDecimal("mass"))
                    .atr(data.getBigDecimal("atr"))
                    .hhv10(data.getBigDecimal("hhv10"))
                    .llv10(data.getBigDecimal("llv10"))
                    .hlc(data.getBigDecimal("hlc"))
                    .upperl(data.getBigDecimal("upperl"))
                    .uppers(data.getBigDecimal("uppers"))
                    .lowers(data.getBigDecimal("lowers"))
                    .lowerl(data.getBigDecimal("lowerl"))
                    .emv(data.getBigDecimal("emv"))
                    .highest9(data.getBigDecimal("highest9"))
                    .lowest9(data.getBigDecimal("lowest9"))
                    .build();
        }).keyBy(OdsStock::getCode);

        keyedStream.map(new RichMapFunction<OdsStock, Object>() {

            private ValueState<BigDecimal> lwr1_state;
            private ValueState<BigDecimal> lwr2_state;

            @Override
            public void open(Configuration parameters) throws Exception {
                lwr1_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("lwr1_state", BigDecimal.class));
                lwr2_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("lwr2_state", BigDecimal.class));
            }

            @Override
            public void close() throws Exception {
                lwr1_state.clear();
                lwr2_state.clear();
            }

            @Override
            public Object map(OdsStock value) throws Exception {
                BigDecimal lwr1 = BigDecimal.ZERO;
                BigDecimal lwr2 = BigDecimal.ZERO;
                if (value.getRk() == 1) {
                    lwr1 = value.getHighest9().subtract(value.getClosing_price()).multiply(BigDecimal.valueOf(100)).divide(value.getHighest9().subtract(value.getLowest9()), 6, RoundingMode.HALF_UP);
                    lwr2 = lwr1;
                } else {
                    BigDecimal rsv = value.getHighest9().subtract(value.getClosing_price()).multiply(BigDecimal.valueOf(100)).divide(value.getHighest9().subtract(value.getLowest9()), 6, RoundingMode.HALF_UP);
                    lwr1 = rsv.add(lwr1_state.value().multiply(BigDecimal.valueOf(2))).divide(BigDecimal.valueOf(3), 6, RoundingMode.HALF_UP);
                    lwr2 = lwr1.add(lwr2_state.value().multiply(BigDecimal.valueOf(2))).divide(BigDecimal.valueOf(3), 6, RoundingMode.HALF_UP);
                }
                lwr1_state.update(lwr1);
                lwr2_state.update(lwr2);
                System.out.println(lwr1 + "|||" + lwr2);

                return null;
            }
        });


        env.execute();
    }
}
