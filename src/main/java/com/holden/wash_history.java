package com.holden;

import com.alibaba.fastjson.JSONObject;
import com.mysql.cj.jdbc.Driver;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.holden.ConnConfig.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;


/**
 * @ClassName spider_flink-test
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年7月10日22:59 - 周日
 * @Describe
 */
public class wash_history {
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

        //转为POJO类并且keyBy根据code分区
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
                    .dpo(data.getBigDecimal("dpo"))
                    .highest9(data.getBigDecimal("highest9"))
                    .lowest9(data.getBigDecimal("lowest9"))
                    .mtm(data.getBigDecimal("mtm"))
                    .lower_ene(data.getBigDecimal("lower_ene"))
                    .upper_ene(data.getBigDecimal("upper_ene"))
                    .build();
        }).keyBy(OdsStock::getCode);


        SingleOutputStreamOperator<StockMid> result = keyedStream.map(new RichMapFunction<OdsStock, StockMid>() {
            private MapState<String, StockMid> stockState;
            private ValueState<BigDecimal> sar_af;
            private ValueState<BigDecimal> sar_high;
            private ValueState<BigDecimal> sar_low;
            private ValueState<Boolean> sar_bull;
            private ValueState<BigDecimal> tr_sum;
            private ValueState<BigDecimal> dmp_sum;
            private ValueState<BigDecimal> dmm_sum;
            private ValueState<BigDecimal> tr_ex;
            private ValueState<BigDecimal> dmp_ex;
            private ValueState<BigDecimal> dmm_ex;
            private ValueState<BigDecimal> adx;
            //MIKE
            private ValueState<BigDecimal> hv_state;
            private ValueState<BigDecimal> lv_state;
            private ValueState<BigDecimal> stor_state;
            private ValueState<BigDecimal> midr_state;
            private ValueState<BigDecimal> wekr_state;
            private ValueState<BigDecimal> weks_state;
            private ValueState<BigDecimal> mids_state;
            private ValueState<BigDecimal> stos_state;
            //TRIX
            private ValueState<BigDecimal> mtr_state;
            private ValueState<BigDecimal> ema1L_state;
            private ValueState<BigDecimal> ema2L_state;

            @Override
            public void open(Configuration parameters) throws Exception {
                stockState = getRuntimeContext().getMapState(new MapStateDescriptor<String, StockMid>("my-map", String.class, StockMid.class));
                sar_af = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("sar_state", BigDecimal.class));
                sar_high = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("sar_high_state", BigDecimal.class));
                sar_low = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("sar_low_state", BigDecimal.class));
                sar_bull = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("sar_bull_state", Boolean.class));
                tr_sum = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("tr_sum_state", BigDecimal.class));
                dmp_sum = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("dmp_sum_state", BigDecimal.class));
                dmm_sum = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("dmm_sum_state", BigDecimal.class));
                tr_ex = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("tr_ex_state", BigDecimal.class));
                dmp_ex = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("dmp_ex_state", BigDecimal.class));
                dmm_ex = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("dmm_ex_state", BigDecimal.class));
                adx = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("adx_state", BigDecimal.class));

                hv_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("hv_state", BigDecimal.class));
                lv_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("lv_high_state", BigDecimal.class));
                stor_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("stor_state", BigDecimal.class));
                midr_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("midr_state", BigDecimal.class));
                wekr_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("wekr_state", BigDecimal.class));
                weks_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("weks_state", BigDecimal.class));
                mids_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("mids_state", BigDecimal.class));
                stos_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("stos_state", BigDecimal.class));

                mtr_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("mtr_state", BigDecimal.class));
                ema1L_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("ema1L_state", BigDecimal.class));
                ema2L_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("ema2L_state", BigDecimal.class));

            }

            @Override
            public void close() throws Exception {
                stockState.clear();
                sar_af.clear();
                sar_high.clear();
                sar_low.clear();
                sar_bull.clear();
                tr_sum.clear();
                dmp_sum.clear();
                dmm_sum.clear();
                tr_ex.clear();
                dmp_ex.clear();
                dmm_ex.clear();
                adx.clear();
                hv_state.clear();
                lv_state.clear();
                stor_state.clear();
                midr_state.clear();
                wekr_state.clear();
                weks_state.clear();
                mids_state.clear();
                stos_state.clear();
                mtr_state.clear();
                ema1L_state.clear();
                ema2L_state.clear();
            }

            @Override
            public StockMid map(OdsStock value) throws Exception {
                StockMid stockMid = new StockMid();
                String key = value.getCode() + delimiter + value.getRk();
                BigDecimal hv = BigDecimal.ZERO;
                BigDecimal lv = BigDecimal.ZERO;
                BigDecimal stor = BigDecimal.ZERO;
                BigDecimal stos = BigDecimal.ZERO;
                BigDecimal midr = BigDecimal.ZERO;
                BigDecimal wekr = BigDecimal.ZERO;
                BigDecimal weks = BigDecimal.ZERO;
                BigDecimal mids = BigDecimal.ZERO;
                BigDecimal mtr = BigDecimal.ZERO;
                BigDecimal ema1L = BigDecimal.ZERO;
                BigDecimal ema2L = BigDecimal.ZERO;
                BigDecimal lwr1 = BigDecimal.ZERO;
                BigDecimal lwr2 = BigDecimal.ZERO;
                stockMid.setSar_bull(true);
                BigDecimal h9_l9 = value.getHighest9().subtract(value.getLowest9()).doubleValue() == 0 ? BigDecimal.ONE : value.getHighest9().subtract(value.getLowest9());
                //第一条数据
                if (value.getRk() == FIRST_DAY) {
                    BigDecimal highest = value.getHighest();
                    BigDecimal lowest = value.getLowest();
                    BigDecimal rsv = BigDecimal.ZERO;

                    BeanUtils.copyProperties(stockMid, value);
                    stockMid.setObv(INITIAL);
                    stockMid.setLast_closing(INITIAL);
                    stockMid.setDif(INITIAL);
                    stockMid.setEma12(value.getClosing_price());
                    stockMid.setEma26(value.getClosing_price());
                    stockMid.setClosing_diff(INITIAL);
                    hv = value.getHhv10();
                    lv = value.getLlv10();
                    stor = hv.multiply(BigDecimal.valueOf(2)).subtract(lv);
                    stos = lv.multiply(BigDecimal.valueOf(2)).subtract(hv);
                    ema1L = value.getClosing_price();
                    ema2L = ema1L;
                    mtr = ema2L;

                    if (!Objects.equals(highest, lowest)) {
                        rsv = (value.getClosing_price().subtract(lowest).multiply(BigDecimal.valueOf(100))).divide(highest.subtract(lowest).equals(BigDecimal.ZERO) ? BigDecimal.ONE : highest.subtract(lowest), SCALE, RoundingMode.HALF_UP);
                        stockMid.setRsv(rsv);
                    } else {
                        stockMid.setRsv(INITIAL);
                    }

                    lwr1 = value.getHighest9().subtract(value.getClosing_price()).multiply(BigDecimal.valueOf(100)).divide(h9_l9, 6, RoundingMode.HALF_UP)
                    ;
                    lwr2 = lwr1;
                    stockMid.setK(rsv);
                    stockMid.setD(rsv);
                    stockMid.setJ(rsv);
                    stockMid.setUp6(INITIAL);
                    stockMid.setUp12(INITIAL);
                    stockMid.setUp24(INITIAL);
                    stockMid.setDown6(INITIAL);
                    stockMid.setDown12(INITIAL);
                    stockMid.setDown24(INITIAL);
                    stockMid.setDea(INITIAL);
                    stockMid.setMacd(INITIAL);
                    stockMid.setRsi6(INITIAL);
                    stockMid.setRsi12(INITIAL);
                    stockMid.setRsi24(INITIAL);

                    stockState.put(key, stockMid);
                } else {
                    //除开第一条后的数据
                    if (!stockState.isEmpty()) {
                        String last_key = value.getCode() + "-" + (value.getRk() - 1);
                        //手动移除掉前面的状态，不然后面的读取速度会变慢
                        if (value.getRk() >= REMOVE_FLAG) {
                            stockState.remove(value.getCode() + "-" + (value.getRk() - 2));
                        }
                        //取上一条记录
                        StockMid last_Stock = stockState.get(last_key);
                        BeanUtils.copyProperties(stockMid, value);
                        BigDecimal closing_diff = value.getClosing_price().subtract(last_Stock.getClosing_price());
                        BigDecimal ema12 = (BigDecimal.valueOf(2).multiply(value.getClosing_price()).add(BigDecimal.valueOf(11).multiply(last_Stock.getEma12()))).divide(BigDecimal.valueOf(13), SCALE, RoundingMode.HALF_UP);
                        BigDecimal ema26 = (BigDecimal.valueOf(2).multiply(value.getClosing_price()).add(BigDecimal.valueOf(25).multiply(last_Stock.getEma26()))).divide(BigDecimal.valueOf(27), SCALE, RoundingMode.HALF_UP);
                        BigDecimal dif = ema12.subtract(ema26);

                        //MIKE
                        hv = hv_state.value().multiply(BigDecimal.valueOf(2)).add(value.getHhv10().multiply(EMA3ZI)).divide(EMA3MU, 6, RoundingMode.HALF_UP);
                        lv = lv_state.value().multiply(BigDecimal.valueOf(2)).add(value.getLlv10().multiply(EMA3ZI)).divide(EMA3MU, 6, RoundingMode.HALF_UP);
                        stor = hv.multiply(BigDecimal.valueOf(2)).subtract(lv).multiply(BigDecimal.valueOf(2)).add(EMA3ZI.multiply(stor_state.value())).divide(EMA3MU, 6, RoundingMode.HALF_UP);
                        stos = lv.multiply(BigDecimal.valueOf(2)).subtract(hv).multiply(BigDecimal.valueOf(2)).add(EMA3ZI.multiply(stos_state.value())).divide(EMA3MU, 6, RoundingMode.HALF_UP);
                        if (value.getRk() == MIKE_START_FLAG) {
                            midr = value.getHlc().add(hv).subtract(lv);
                            wekr = value.getHlc().multiply(BigDecimal.valueOf(2)).subtract(lv);
                            weks = value.getHlc().multiply(BigDecimal.valueOf(2)).subtract(hv);
                            mids = value.getHlc().subtract(hv).add(lv);
                        } else {
                            midr = midr_state.value().multiply(BigDecimal.valueOf(2)).add(value.getHlc().add(hv).subtract(lv).multiply(EMA3ZI)).divide(EMA3MU, 6, RoundingMode.HALF_UP);
                            wekr = wekr_state.value().multiply(BigDecimal.valueOf(2)).add(value.getHlc().multiply(BigDecimal.valueOf(2)).subtract(lv).multiply(EMA3ZI)).divide(EMA3MU, 6, RoundingMode.HALF_UP);
                            weks = weks_state.value().multiply(BigDecimal.valueOf(2)).add(value.getHlc().multiply(BigDecimal.valueOf(2)).subtract(hv).multiply(EMA3ZI)).divide(EMA3MU, 6, RoundingMode.HALF_UP);
                            mids = mids_state.value().multiply(BigDecimal.valueOf(2)).add(value.getHlc().subtract(hv).add(lv).multiply(EMA3ZI)).divide(EMA3MU, 6, RoundingMode.HALF_UP);
                        }
                        //TRIX
                        ema1L = value.getClosing_price().multiply(BigDecimal.valueOf(2)).add(ema1L_state.value().multiply(BigDecimal.valueOf(11))).divide(BigDecimal.valueOf(13), 6, RoundingMode.HALF_UP);
                        ema2L = ema1L.multiply(BigDecimal.valueOf(2)).add(ema2L_state.value().multiply(BigDecimal.valueOf(11))).divide(BigDecimal.valueOf(13), 6, RoundingMode.HALF_UP);
                        mtr = ema2L.multiply(BigDecimal.valueOf(2)).add(mtr_state.value().multiply(BigDecimal.valueOf(11))).divide(BigDecimal.valueOf(13), 6, RoundingMode.HALF_UP);
                        stockMid.setTrix(mtr.subtract(mtr_state.value()).divide(mtr_state.value().doubleValue() == 0 ? BigDecimal.ONE : mtr_state.value(), 6, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100)));
                        //LWR
                        BigDecimal rsv = value.getHighest9().subtract(value.getClosing_price()).multiply(BigDecimal.valueOf(100)).divide(h9_l9, 6, RoundingMode.HALF_UP);
                        lwr1 = rsv.add(last_Stock.getLwr1().multiply(BigDecimal.valueOf(2))).divide(BigDecimal.valueOf(3), 6, RoundingMode.HALF_UP);
                        lwr2 = lwr1.add(last_Stock.getLwr2().multiply(BigDecimal.valueOf(2))).divide(BigDecimal.valueOf(3), 6, RoundingMode.HALF_UP);


                        stockMid.setRsv(value.getRsv());
                        stockMid.setEma12(ema12);
                        stockMid.setEma26(ema26);
                        stockMid.setDif(dif);
                        stockMid.setClosing_diff(closing_diff);
                        stockMid.setLast_closing(last_Stock.getClosing_price());
                        BigDecimal up6;
                        BigDecimal up12;
                        BigDecimal up24;
                        BigDecimal down6;
                        BigDecimal down12;
                        BigDecimal down24;

                        if (closing_diff.compareTo(BigDecimal.valueOf(0)) > 0) {
                            stockMid.setObv(last_Stock.getObv().add(value.getDeal_amount()));
                            up6 = (closing_diff.add(last_Stock.getUp6().multiply(BigDecimal.valueOf(5)))).divide(BigDecimal.valueOf(6), SCALE, RoundingMode.HALF_UP);
                            up12 = (closing_diff.add(last_Stock.getUp12().multiply(BigDecimal.valueOf(11)))).divide(BigDecimal.valueOf(12), SCALE, RoundingMode.HALF_UP);
                            up24 = (closing_diff.add(last_Stock.getUp24().multiply(BigDecimal.valueOf(23)))).divide(BigDecimal.valueOf(24), SCALE, RoundingMode.HALF_UP);
                            down6 = last_Stock.getDown6().abs().multiply(BigDecimal.valueOf(5)).divide(BigDecimal.valueOf(6), SCALE, RoundingMode.HALF_UP);
                            down12 = last_Stock.getDown12().abs().multiply(BigDecimal.valueOf(11)).divide(BigDecimal.valueOf(12), SCALE, RoundingMode.HALF_UP);
                            down24 = last_Stock.getDown24().abs().multiply(BigDecimal.valueOf(23)).divide(BigDecimal.valueOf(24), SCALE, RoundingMode.HALF_UP);
                        } else if (closing_diff.compareTo(BigDecimal.valueOf(0)) < 0) {
                            stockMid.setObv(last_Stock.getObv().subtract(value.getDeal_amount()));
                            up6 = last_Stock.getUp6().multiply(BigDecimal.valueOf(5)).divide(BigDecimal.valueOf(6), SCALE, RoundingMode.HALF_UP);
                            down6 = (closing_diff.abs().add(last_Stock.getDown6().multiply(BigDecimal.valueOf(5)))).divide(BigDecimal.valueOf(6), SCALE, RoundingMode.HALF_UP);
                            down12 = (closing_diff.abs().add(last_Stock.getDown12().multiply(BigDecimal.valueOf(11)))).divide(BigDecimal.valueOf(12), SCALE, RoundingMode.HALF_UP);
                            up12 = last_Stock.getUp12().multiply(BigDecimal.valueOf(11)).divide(BigDecimal.valueOf(12), SCALE, RoundingMode.HALF_UP);
                            down24 = (closing_diff.abs().add(last_Stock.getDown24().multiply(BigDecimal.valueOf(23)))).divide(BigDecimal.valueOf(24), SCALE, RoundingMode.HALF_UP);
                            up24 = last_Stock.getUp24().multiply(BigDecimal.valueOf(23)).divide(BigDecimal.valueOf(24), SCALE, RoundingMode.HALF_UP);
                        } else {
                            stockMid.setObv(last_Stock.getObv());
                            down6 = last_Stock.getDown6().abs().multiply(BigDecimal.valueOf(5)).divide(BigDecimal.valueOf(6), SCALE, RoundingMode.HALF_UP);
                            up6 = last_Stock.getUp6().multiply(BigDecimal.valueOf(5)).divide(BigDecimal.valueOf(6), SCALE, RoundingMode.HALF_UP);
                            down12 = last_Stock.getDown12().abs().multiply(BigDecimal.valueOf(11)).divide(BigDecimal.valueOf(12), SCALE, RoundingMode.HALF_UP);
                            up12 = last_Stock.getUp12().multiply(BigDecimal.valueOf(11)).divide(BigDecimal.valueOf(12), SCALE, RoundingMode.HALF_UP);
                            down24 = last_Stock.getDown24().abs().multiply(BigDecimal.valueOf(23)).divide(BigDecimal.valueOf(24), SCALE, RoundingMode.HALF_UP);
                            up24 = last_Stock.getUp24().multiply(BigDecimal.valueOf(23)).divide(BigDecimal.valueOf(24), SCALE, RoundingMode.HALF_UP);
                        }
                        stockMid.setDown6(down6);
                        stockMid.setDown12(down12);
                        stockMid.setDown24(down24);
                        stockMid.setUp6(up6);
                        stockMid.setUp12(up12);
                        stockMid.setUp24(up24);


                        BigDecimal k = (value.getRsv().add(last_Stock.getK().multiply(BigDecimal.valueOf(2)))).divide(BigDecimal.valueOf(3), SCALE, RoundingMode.HALF_UP);
                        BigDecimal d = (k.add(last_Stock.getD().multiply(BigDecimal.valueOf(2)))).divide(BigDecimal.valueOf(3), SCALE, RoundingMode.HALF_UP);
                        stockMid.setK(k);
                        stockMid.setD(d);
                        stockMid.setJ(k.multiply(BigDecimal.valueOf(3)).subtract(d.multiply(BigDecimal.valueOf(2))));
                        BigDecimal dea = dif.multiply(BigDecimal.valueOf(2)).add(last_Stock.getDea().multiply(BigDecimal.valueOf(8))).divide(BigDecimal.valueOf(10), SCALE, RoundingMode.HALF_UP);
                        stockMid.setDea(dea);
                        stockMid.setMacd(dif.subtract(dea).multiply(BigDecimal.valueOf(2)));
                        /*
                         * Explain
                         *   这里不能使用.equals(Bigdecimal.ZERO),因为这里只比较0，而不能比较0.0
                         * */
                        stockMid.setRsi6(up6.multiply(BigDecimal.valueOf(100)).divide(up6.add(down6).doubleValue() == 0 ? BigDecimal.ONE : up6.add(down6), SCALE, RoundingMode.HALF_UP));
                        stockMid.setRsi12(up12.multiply(BigDecimal.valueOf(100)).divide(up12.add(down12).doubleValue() == 0 ? BigDecimal.ONE : up12.add(down12), SCALE, RoundingMode.HALF_UP));
                        stockMid.setRsi24(up24.multiply(BigDecimal.valueOf(100)).divide(up24.add(down24).doubleValue() == 0 ? BigDecimal.ONE : up24.add(down24), SCALE, RoundingMode.HALF_UP));

                        //Holden SAR指标
                        if (value.getRk() == SAR_START_FLAG || value.getRk() == (SAR_START_FLAG + 1)) {
                            stockMid.setSar(value.getSar_low());
                            sar_high.update(value.getSar_high());
                            sar_low.update(value.getSar_low());
                            sar_af.update(SAR_AF);
                            sar_bull.update(true);
                        } else if (value.getRk() > (SAR_START_FLAG + 1)) {
                            if (sar_bull.value()) {
                                BigDecimal tmp_sar = last_Stock.getSar().add(sar_af.value().multiply(sar_high.value().subtract(last_Stock.getSar())));
                                if (value.getHighest().compareTo(sar_high.value()) > 0) {
                                    //更新sar_high
                                    sar_high.update(value.getHighest());
                                    //更新sar_af
                                    sar_af.update(BigDecimal.valueOf(0.2).min((sar_af.value().add(SAR_AF))));
                                }
                                stockMid.setSar(tmp_sar);
                                if (tmp_sar.compareTo(value.getClosing_price()) > 0) {
                                    stockMid.setSar(value.getSar_high());
                                    sar_af.update(SAR_AF);
                                    sar_bull.update(false);
                                    sar_high.update(value.getSar_high());
                                    sar_low.update(value.getSar_low());
                                }
                            } else {
                                BigDecimal tmp_sar = last_Stock.getSar().add(sar_af.value().multiply(sar_low.value().subtract(last_Stock.getSar())));
                                if (value.getLowest().compareTo(sar_low.value()) < 0) {
                                    //更新sar_high
                                    sar_low.update(value.getLowest());
                                    //更新sar_af
                                    sar_af.update(BigDecimal.valueOf(0.2).min((sar_af.value().add(SAR_AF))));
                                }
                                stockMid.setSar(tmp_sar);
                                if (tmp_sar.compareTo(value.getClosing_price()) < 0) {
                                    stockMid.setSar(value.getSar_low());
                                    sar_af.update(SAR_AF);
                                    sar_bull.update(true);
                                    sar_high.update(value.getSar_high());
                                    sar_low.update(value.getSar_low());
                                }
                            }
                            stockMid.setSar_bull(sar_bull.value());
                        }
                        stockMid.setLwr1(lwr1);
                        stockMid.setLwr2(lwr2);
                        stockState.put(key, stockMid);
                    }
                }
                hv_state.update(hv);
                lv_state.update(lv);
                stor_state.update(stor);
                stos_state.update(stos);
                midr_state.update(midr);
                wekr_state.update(wekr);
                weks_state.update(weks);
                mids_state.update(mids);

                ema1L_state.update(ema1L);
                ema2L_state.update(ema2L);
                mtr_state.update(mtr);


                stockMid.setSar_af(sar_af.value());
                stockMid.setSar_high(sar_high.value());
                stockMid.setSar_low(sar_low.value());
                stockMid.setHv(hv);
                stockMid.setLv(lv);
                stockMid.setSTOR(stor);
                stockMid.setSTOS(stos);
                stockMid.setMIDR(midr);
                stockMid.setWEKR(wekr);
                stockMid.setWEKS(weks);
                stockMid.setMIDS(mids);

                stockMid.setEma1L(ema1L);
                stockMid.setMtr(mtr);
                stockMid.setLwr1(lwr1);
                stockMid.setLwr2(lwr2);


                //DMI
                BigDecimal trex = BigDecimal.ZERO;
                BigDecimal dmpex = BigDecimal.ZERO;
                BigDecimal dmmex = BigDecimal.ZERO;
                BigDecimal mpdi = BigDecimal.ZERO;
                if (value.getRk() < DMI_START_FLAG) {//13天含以前
                    tr_sum.update((tr_sum.value() == null ? BigDecimal.valueOf(0) : tr_sum.value()).add(value.getTr()));
                    dmp_sum.update((dmp_sum.value() == null ? BigDecimal.valueOf(0) : dmp_sum.value()).add(value.getDmp()));
                    dmm_sum.update((dmm_sum.value() == null ? BigDecimal.valueOf(0) : dmm_sum.value()).add(value.getDmm()));
                } else {//14天含以后
                    if (value.getRk() == DMI_START_FLAG) {
                        tr_sum.update((tr_sum.value() == null ? BigDecimal.valueOf(0) : tr_sum.value()).add(value.getTr()));
                        dmp_sum.update((dmp_sum.value() == null ? BigDecimal.valueOf(0) : dmp_sum.value()).add(value.getDmp()));
                        dmm_sum.update((dmm_sum.value() == null ? BigDecimal.valueOf(0) : dmm_sum.value()).add(value.getDmm()));

                        trex = tr_sum.value().divide(BigDecimal.valueOf(DMI_START_FLAG), SCALE, RoundingMode.HALF_UP);
                        dmmex = dmm_sum.value().divide(BigDecimal.valueOf(DMI_START_FLAG), SCALE, RoundingMode.HALF_UP);
                        dmpex = dmp_sum.value().divide(BigDecimal.valueOf(DMI_START_FLAG), SCALE, RoundingMode.HALF_UP);
                    } else {
                        trex = value.getTr().multiply(BigDecimal.valueOf(2)).add(tr_ex.value().multiply(BigDecimal.valueOf(13))).divide(BigDecimal.valueOf(15), SCALE, RoundingMode.HALF_UP);
                        dmpex = value.getDmp().multiply(BigDecimal.valueOf(2)).add(dmp_ex.value().multiply(BigDecimal.valueOf(13))).divide(BigDecimal.valueOf(15), SCALE, RoundingMode.HALF_UP);
                        dmmex = value.getDmm().multiply(BigDecimal.valueOf(2)).add(dmm_ex.value().multiply(BigDecimal.valueOf(13))).divide(BigDecimal.valueOf(15), SCALE, RoundingMode.HALF_UP);
                    }
                    tr_ex.update(trex);
                    dmp_ex.update(dmpex);
                    dmm_ex.update(dmmex);
                    stockMid.setTrex(trex);
                    stockMid.setDmmex(dmmex);
                    stockMid.setDmpex(dmpex);


                    if (trex.doubleValue() != 0) {
                        BigDecimal pdi = dmpex.multiply(BigDecimal.valueOf(100)).divide(trex, SCALE, RoundingMode.HALF_UP);
                        BigDecimal mdi = dmmex.multiply(BigDecimal.valueOf(100)).divide(trex, SCALE, RoundingMode.HALF_UP);
                        stockMid.setPdi(pdi);
                        stockMid.setMdi(mdi);
                        if (mdi.add(pdi).doubleValue() != 0) {
                            mpdi = mdi.subtract(pdi).abs().divide(pdi.add(mdi).doubleValue() == 0 ? BigDecimal.ONE : pdi.add(mdi), SCALE, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100));
                        }
                    }
                    if (value.getRk() <= ADX_START_FLAG) {
                        adx.update((adx.value() == null ? BigDecimal.valueOf(0) : adx.value()).add(mpdi));

                        if (value.getRk() == ADX_START_FLAG) {
                            BigDecimal adx_first = adx.value().divide(BigDecimal.valueOf(6), SCALE, RoundingMode.HALF_UP);
                            adx.update(adx_first);
                            stockMid.setAdx(adx_first);
                        }
                    } else {
                        BigDecimal last_adx = mpdi.multiply(BigDecimal.valueOf(2)).add(adx.value().multiply(BigDecimal.valueOf(5))).divide(BigDecimal.valueOf(7), SCALE, RoundingMode.HALF_UP);
                        adx.update(last_adx);
                        stockMid.setAdx(last_adx);
                    }
                }
                stockMid.setMpdi(mpdi);
                stockMid.setHighest(value.getHighest());
                stockMid.setLowest(value.getLowest());
                return stockMid;
            }
        });
        result.print();

        result.addSink(JdbcSink.sink(
                "INSERT INTO ods_a_stock_tech_index " +
                        "VALUES" +
                        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (ps, t) -> {
                    ps.setString(1, t.getCode());
                    ps.setString(2, t.getName());
                    ps.setBigDecimal(3, t.getOpening_price());
                    ps.setBigDecimal(4, t.getClosing_price());
                    ps.setBigDecimal(5, t.getLast_closing());
                    ps.setBigDecimal(6, t.getHighest());
                    ps.setBigDecimal(7, t.getLowest());
                    ps.setString(8, t.getDs());
                    ps.setBigDecimal(9, t.getDeal_amount());
                    ps.setBigDecimal(10, t.getClosing_diff());
                    ps.setInt(11, t.getRk());
                    ps.setBigDecimal(12, t.getEma12());
                    ps.setBigDecimal(13, t.getEma26());
                    ps.setBigDecimal(14, t.getDif());
                    ps.setBigDecimal(15, t.getObv());
                    ps.setBigDecimal(16, t.getRsv());
                    ps.setBigDecimal(17, t.getUp6());
                    ps.setBigDecimal(18, t.getDown6());
                    ps.setBigDecimal(19, t.getUp12());
                    ps.setBigDecimal(20, t.getDown12());
                    ps.setBigDecimal(21, t.getUp24());
                    ps.setBigDecimal(22, t.getDown24());
                    ps.setBigDecimal(23, t.getRsi6());
                    ps.setBigDecimal(24, t.getRsi12());
                    ps.setBigDecimal(25, t.getRsi24());
                    ps.setBigDecimal(26, t.getK());
                    ps.setBigDecimal(27, t.getD());
                    ps.setBigDecimal(28, t.getJ());
                    ps.setBigDecimal(29, t.getSar());
                    ps.setBigDecimal(30, t.getDea());
                    ps.setBigDecimal(31, t.getMacd());
                    ps.setBigDecimal(32, t.getPdi());
                    ps.setBigDecimal(33, t.getMdi());
                    ps.setBigDecimal(34, t.getAdx());
                    ps.setBigDecimal(35, t.getTrex());
                    ps.setBigDecimal(36, t.getDmpex());
                    ps.setBigDecimal(37, t.getDmmex());
                    ps.setBoolean(38, t.getSar_bull());
                    ps.setBigDecimal(39, t.getSar_low());
                    ps.setBigDecimal(40, t.getSar_high());
                    ps.setBigDecimal(41, t.getSar_af());
                    ps.setBigDecimal(42, t.getMpdi());
                    ps.setBigDecimal(43, t.getSTOR());
                    ps.setBigDecimal(44, t.getMIDR());
                    ps.setBigDecimal(45, t.getWEKR());
                    ps.setBigDecimal(46, t.getWEKS());
                    ps.setBigDecimal(47, t.getMIDS());
                    ps.setBigDecimal(48, t.getSTOS());
                    ps.setBigDecimal(49, t.getMa3());
                    ps.setBigDecimal(50, t.getMa5());
                    ps.setBigDecimal(51, t.getMa10());
                    ps.setBigDecimal(52, t.getMa12());
                    ps.setBigDecimal(53, t.getMa20());
                    ps.setBigDecimal(54, t.getMa24());
                    ps.setBigDecimal(55, t.getMa50());
                    ps.setBigDecimal(56, t.getMa60());
                    ps.setBigDecimal(57, t.getBbi());
                    ps.setBigDecimal(58, t.getWr6());
                    ps.setBigDecimal(59, t.getWr10());
                    ps.setBigDecimal(60, t.getBias6());
                    ps.setBigDecimal(61, t.getBias12());
                    ps.setBigDecimal(62, t.getBias24());
                    ps.setBigDecimal(63, t.getBias36());
                    ps.setBigDecimal(64, t.getRoc());
                    ps.setBigDecimal(65, t.getMaroc());
                    ps.setBigDecimal(66, t.getAsi());
                    ps.setBigDecimal(67, t.getEne());
                    ps.setBigDecimal(68, t.getPsy());
                    ps.setBigDecimal(69, t.getPsyma());
                    ps.setBigDecimal(70, t.getBr());
                    ps.setBigDecimal(71, t.getAr());
                    ps.setBigDecimal(72, t.getMass());
                    ps.setBigDecimal(73, t.getAtr());
                    ps.setBigDecimal(74, t.getHlc());
                    ps.setBigDecimal(75, t.getUpperl());
                    ps.setBigDecimal(76, t.getLowers());
                    ps.setBigDecimal(77, t.getUppers());
                    ps.setBigDecimal(78, t.getLowerl());
                    ps.setBigDecimal(79, t.getEmv());
                    ps.setBigDecimal(80, t.getHv());
                    ps.setBigDecimal(81, t.getLv());
                    ps.setBigDecimal(82, t.getEma1L());
                    ps.setBigDecimal(83, t.getMtr());
                    ps.setBigDecimal(84, t.getTrix());
                    ps.setBigDecimal(85, t.getDpo());
                    ps.setBigDecimal(86, t.getMtm());
                    ps.setBigDecimal(87, t.getLwr1());
                    ps.setBigDecimal(88, t.getLwr2());
                    ps.setBigDecimal(89, t.getUpper_ene());
                    ps.setBigDecimal(90, t.getLower_ene());
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(1).withMaxRetries(10) //这里批次大小来提交，这里最好写1次，因为我们处理的是历史数据
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://127.0.0.1:3306/spider_base?useSSL=false&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("root")
                        .withDriverName(Driver.class.getName())
                        .build()
        ));
        env.execute();
    }
}
