package org.irenical.dumpy;

import org.irenical.dumpy.api.IExtractor;

public class Poopsy implements IExtractor.Entity< Integer > {

    private Integer size;

    public Poopsy(Integer size) {
        this.size = size;
    }

    @Override
    public String getId() {
        return String.valueOf(size);
    }

    @Override
    public Integer getValue() {
        return size;
    }

}
