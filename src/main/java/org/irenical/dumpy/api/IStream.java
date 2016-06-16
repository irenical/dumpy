package org.irenical.dumpy.api;

public interface IStream< TYPE > {

    String getCode();

    IExtractor< TYPE > getExtractor();

//    TODO : should have a transformer from source entity to target entity
//    or this transformation should be done by the loader (it needs to know the source and the target)

    ILoader< TYPE > getLoader();

}
