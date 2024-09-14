package com.ctrip.framework.apollo.use.cases.spring.boot.apollo;

import com.fasterxml.jackson.annotation.JsonGetter;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.annotate.JsonIgnore;
import org.slf4j.MDC;

import org.jetbrains.annotations.Nullable;

@Data
@Slf4j
public class JsonResult<T> {
    /**
     * 200
     */
    public static final String OK = "ok";
    /**
     * 400
     */
    public static final String BAD_REQUEST = "bad_request";
    /**
     * 401
     */
    public static final String UNAUTHORIZED = "unauthorized";

    /**
     * 403
     */
    public static final String FORBIDDEN = "forbidden";

    /**
     * 404
     */
    public static final String NOT_FOUND = "not_found";
    /**
     * 408
     */
    public static final String TIMEOUT = "timeout";
    /**
     * 409
     */
    public static final String DUPLICATE_REQUEST = "dup_request";
    /**
     * 500
     */
    public static final String INTERNAL_ERROR = "internal_error";
    /**
     * 503
     */
    public static final String SERVICE_UNAVAILABLE = "service_unavailable";

    private String status;

    private String msg;

    private T data;

    private Integer error_code;

    private String trace_id;

    public static <T> JsonResult<T> ok(T data) {
        JsonResult<T> result = new JsonResult<>();
        result.setStatus(OK);
        result.setMsg(OK);
        result.setData(data);
        return result;
    }

    public static <T> JsonResult<T> ok(T data, String msg) {
        JsonResult<T> result = new JsonResult<>();
        result.setStatus(OK);
        result.setMsg(msg);
        result.setData(data);
        return result;
    }

    public static <T> JsonResult<T> ok() {
        return ok(null);
    }

    public static JsonResult fail(String status, String msg) {
        return fail(status, msg, null);
    }

    public static JsonResult fail(String status, String msg, Object data) {
        return fail(status, msg, null, data);
    }

    public static JsonResult fail(String status, String msg, Integer errorCode, Object data) {
        if (status.equals(OK)) {
            throw new RuntimeException("ok is not fail");
        }
        JsonResult result = new JsonResult();
        result.setStatus(status);
        result.setMsg(msg);
        result.setError_code(errorCode);
        result.setData(data);
        result.setTrace_id(getTraceId());
        return result;
    }

    @Nullable
    private static String getTraceId() {
        try {
            return MDC.get("TRACE_ID");
        } catch (Exception e) {
            log.error("fail to get trace id", e);
            return null;
        }
    }

    public static JsonResult badRequest(String msg) {
        return fail(BAD_REQUEST, msg);
    }

    public static JsonResult unauthorized(String msg) {
        return fail(UNAUTHORIZED, msg);
    }

    public static JsonResult forbidden(String msg) {
        return fail(FORBIDDEN, msg);
    }

    public static JsonResult notFound(String msg) {
        return fail(NOT_FOUND, msg);
    }

    public static JsonResult timeout(String msg) {
        return fail(TIMEOUT, msg);
    }

    public static JsonResult duplicateRequest(String msg) {
        return fail(DUPLICATE_REQUEST, msg);
    }

    public static JsonResult internalError(String msg) {
        return fail(INTERNAL_ERROR, msg);
    }

    public static JsonResult serviceUnavailable(String msg) {
        return fail(SERVICE_UNAVAILABLE, msg);
    }

    @JsonIgnore
    public boolean isOk() {
        return OK.equals(status);
    }

    @JsonGetter
    public long getTimestamp() {
        return System.currentTimeMillis();
    }
}