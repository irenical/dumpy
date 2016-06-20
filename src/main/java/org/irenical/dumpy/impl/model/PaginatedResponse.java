package org.irenical.dumpy.impl.model;

import java.util.List;

public class PaginatedResponse< TYPE > {

    public List< TYPE > values;

    public String cursor;

    public boolean hasNext;


    public PaginatedResponse() {

    }

    public List< TYPE > getValues() {
        return values;
    }

    public void setValues(List< TYPE > values) {
        this.values = values;
    }

    public String getCursor() {
        return cursor;
    }

    public void setCursor(String cursor) {
        this.cursor = cursor;
    }

    public boolean hasNext() {
        return hasNext;
    }

    public void setHasNext(boolean hasNext) {
        this.hasNext = hasNext;
    }

}
