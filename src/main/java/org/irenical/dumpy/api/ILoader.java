package org.irenical.dumpy.api;

import java.util.List;
import java.util.Map;

public interface ILoader< TYPE > {

    enum Status {
        SUCCESS, ERROR
    }

    /**
     * Loads the given entities into the target system.
     *
     * @param entities  the list of extracted entities
     * @return  for each entity, Status.SUCCESS if all good, Status.ERROR otherwise
     */
    Map< ? extends IExtractor.Entity< TYPE >, Status > load( List< ? extends IExtractor.Entity< TYPE > > entities );

}
