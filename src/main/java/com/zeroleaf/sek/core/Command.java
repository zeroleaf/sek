package com.zeroleaf.sek.core;

/**
 * @author zeroleaf
 */
public interface Command {

    /**
     * Get name of this command.
     *
     * @return name of this command.
     */
    String getName();

    /**
     * Execute this command with specified args.
     *
     * Where args include argument and options.
     *
     * @param args command args.
     */
    void execute(String... args) throws Exception;
}
