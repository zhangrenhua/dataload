package com.hua.spark.dataload.common;

/**
 * Created by hua on 2016/4/9.
 */
public class BigDataLoadException extends Exception {

    public BigDataLoadException() {
        super();
    }

    public BigDataLoadException(String message) {
        super(message);
    }

    public BigDataLoadException(String message, Throwable cause) {
        super(message, cause);
    }

    public BigDataLoadException(Throwable cause) {
        super(cause);
    }

}
