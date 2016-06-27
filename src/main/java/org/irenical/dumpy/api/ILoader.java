package org.irenical.dumpy.api;

import java.util.List;

public interface ILoader< TYPE > {

    enum Status {
        SUCCESS, ERROR
    }

    /**
     * Loads the given entities into the target system.
     *
     * @param entities  the list of extracted entities
     * @return  Status.SUCCESS if load is ok; Status.ERROR otherwise.
     */
    Status load(List< ? extends IExtractor.Entity< TYPE > > entities );

}
