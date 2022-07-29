package com.holden;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @ClassName spider_flink-OdsStock
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年7月10日23:40 - 周日
 * @Describe ods_stock_step_one的映射表
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OdsStock {
    private String code;
    private String name;
    private BigDecimal opening_price;
    private BigDecimal closing_price;
    private BigDecimal last_closing;
    private String ds;
    private BigDecimal deal_amount;
    private int rk;
    private BigDecimal hhv;
    private BigDecimal llv;
    private BigDecimal rsv;
    private BigDecimal highest;
    private BigDecimal lowest;
    @Builder.Default
    private BigDecimal sar_high = BigDecimal.ZERO;
    @Builder.Default
    private BigDecimal sar_low = BigDecimal.ZERO;
    private BigDecimal tr;
    private BigDecimal dmp;
    private BigDecimal dmm;
    private BigDecimal closing_diff;
    private BigDecimal ma3;
    private BigDecimal ma5;
    private BigDecimal ma10;
    private BigDecimal ma12;
    private BigDecimal ma20;
    private BigDecimal ma24;
    private BigDecimal ma50;
    private BigDecimal ma60;
    private BigDecimal bbi;
    private BigDecimal wr6;
    private BigDecimal wr10;
    private BigDecimal bias6;
    private BigDecimal bias12;
    private BigDecimal bias24;
    private BigDecimal bias36;
    private BigDecimal roc;
    private BigDecimal maroc;
    private BigDecimal asi;
    private BigDecimal ene;
    private BigDecimal psy;
    private BigDecimal psyma;
    private BigDecimal br;
    private BigDecimal ar;
    private BigDecimal mass;
    private BigDecimal atr;
    private BigDecimal hhv10;
    private BigDecimal llv10;
    private BigDecimal hlc;
    private BigDecimal upperl;
    private BigDecimal uppers;
    private BigDecimal lowers;
    private BigDecimal lowerl;
    private BigDecimal emv;
    private BigDecimal dpo;
    private BigDecimal mtm;
    private BigDecimal highest9;
    private BigDecimal lowest9;
    private BigDecimal upper_ene;
    private BigDecimal lower_ene;
}
