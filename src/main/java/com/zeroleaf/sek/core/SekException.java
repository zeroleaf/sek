package com.zeroleaf.sek.core;

/**
 * @author zeroleaf
 */
public class SekException extends Exception {

    public SekException(String message) {
        super(message);
    }

    public SekException(String message, Throwable cause) {
        super(message, cause);
    }
}
