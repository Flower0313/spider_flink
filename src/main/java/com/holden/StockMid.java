package com.holden;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @ClassName spider_flink-StockMid
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年7月11日9:44 - 周一
 * @Describe
 */


@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class StockMid {
    private String name;
    private int rk;
    private String code;
    private String ds;
    private BigDecimal deal_amount;
    private BigDecimal closing_price;
    private BigDecimal highest;
    private BigDecimal lowest;
    private BigDecimal ema12;
    private BigDecimal ema26;
    private BigDecimal dif;
    private BigDecimal opening_price;
    private BigDecimal closing_diff;
    private BigDecimal last_closing;
    private BigDecimal obv;
    private BigDecimal rsv;
    private BigDecimal up6;
    private BigDecimal down6;
    private BigDecimal up12;
    private BigDecimal down12;
    private BigDecimal up24;
    private BigDecimal down24;
    private BigDecimal rsi6;
    private BigDecimal rsi12;
    private BigDecimal rsi24;
    private BigDecimal k;
    private BigDecimal d;
    private BigDecimal j;
    @Builder.Default
    private BigDecimal sar = BigDecimal.ZERO;
    private BigDecimal dea;
    private BigDecimal macd;
    @Builder.Default
    private BigDecimal pdi = BigDecimal.ZERO;
    @Builder.Default
    private BigDecimal mdi = BigDecimal.ZERO;
    @Builder.Default
    private BigDecimal adx = BigDecimal.ZERO;
    @Builder.Default
    private BigDecimal trex = BigDecimal.ZERO;
    @Builder.Default
    private BigDecimal dmpex = BigDecimal.ZERO;
    @Builder.Default
    private BigDecimal dmmex = BigDecimal.ZERO;
    private Boolean sar_bull;
    @Builder.Default
    private BigDecimal sar_low = BigDecimal.ZERO;
    @Builder.Default
    private BigDecimal sar_high = BigDecimal.ZERO;
    @Builder.Default
    private BigDecimal sar_af = BigDecimal.valueOf(0.02);
    private BigDecimal mpdi;
    private BigDecimal STOR;
    @Builder.Default
    private BigDecimal MIDR = BigDecimal.ZERO;
    @Builder.Default
    private BigDecimal WEKR = BigDecimal.ZERO;
    @Builder.Default
    private BigDecimal WEKS = BigDecimal.ZERO;
    @Builder.Default
    private BigDecimal MIDS = BigDecimal.ZERO;
    private BigDecimal STOS;
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
    private BigDecimal hlc;
    private BigDecimal upperl;
    private BigDecimal uppers;
    private BigDecimal lowers;
    private BigDecimal lowerl;
    private BigDecimal emv;
    private BigDecimal hv;
    private BigDecimal lv;
    private BigDecimal ema1L; //给TRIX的mtr使用的
    private BigDecimal mtr; //给TRIX的mtr使用的
    @Builder.Default
    private BigDecimal trix = BigDecimal.ZERO;
    private BigDecimal dpo;
    private BigDecimal mtm;
    private BigDecimal lwr1;
    private BigDecimal lwr2;
    private BigDecimal upper_ene;
    private BigDecimal lower_ene;
}
