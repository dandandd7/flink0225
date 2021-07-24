package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yhm
 * @create 2021-07-16 19:07
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TxEvent {
    private String txId;
    private String payChannel;
    private Long eventTime;
}
