package org.irenical.dumpy.impl.model;

import org.irenical.dumpy.api.IExtractor;
import org.irenical.dumpy.api.ILoader;
import org.irenical.dumpy.api.IStream;

public class StreamImpl< TYPE > implements IStream< TYPE > {

    private String code;

    private IExtractor< TYPE > iExtractor;

    private ILoader< TYPE > iLoader;

    public StreamImpl() {

    }

    public StreamImpl(String code, IExtractor< TYPE > iExtractor, ILoader< TYPE > iLoader) {
        this.code = code;
        this.iExtractor = iExtractor;
        this.iLoader = iLoader;
    }

    @Override
    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    @Override
    public IExtractor< TYPE > getExtractor() {
        return iExtractor;
    }

    public void setiExtractor(IExtractor< TYPE > iExtractor) {
        this.iExtractor = iExtractor;
    }

    @Override
    public ILoader< TYPE > getLoader() {
        return iLoader;
    }

    public void setiLoader(ILoader< TYPE > iLoader) {
        this.iLoader = iLoader;
    }
}
