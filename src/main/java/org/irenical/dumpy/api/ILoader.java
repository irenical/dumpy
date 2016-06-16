package org.irenical.dumpy.api;

import java.util.List;

public interface ILoader< TYPE > {

    enum Status {
        SUCCESS, ERROR
    }

    Status load(List< ? extends IExtractor.Entity< TYPE > > entities );

}
