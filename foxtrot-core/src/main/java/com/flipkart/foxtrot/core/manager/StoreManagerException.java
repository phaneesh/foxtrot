package com.flipkart.foxtrot.core.manager;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Created by rishabh.goyal on 09/12/15.
 */
public class StoreManagerException extends Exception {

    public enum ErrorCode {
        INITIALIZATION_EXCEPTION,
        METADATA_FETCH_EXCEPTION,
        DATA_CLEANUP_EXCEPTION
    }

    private final ErrorCode errorCode;

    public StoreManagerException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public StoreManagerException(ErrorCode errorCode, Throwable cause) {
        super(cause);
        this.errorCode = errorCode;
    }

    public StoreManagerException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("errorCode", errorCode)
                .toString();
    }

}
