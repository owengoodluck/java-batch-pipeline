package com.example.batch;

/** 单条记录的处理结果状态。模板只关心"成功 / 失败"，不解读业务状态码的含义。 */
public enum OutcomeStatus {
    /** 成功 */
    SUCCESS,
    /** 失败（记录级：服务端有响应，但这条记录业务上没通过） */
    FAILED
}
