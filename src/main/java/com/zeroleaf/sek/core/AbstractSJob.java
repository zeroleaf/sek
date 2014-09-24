package com.zeroleaf.sek.core;

/**
 * @author zeroleaf
 */
public abstract class AbstractSJob implements SJob {

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }
}
