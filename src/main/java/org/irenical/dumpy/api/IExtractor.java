package org.irenical.dumpy.api;

import java.util.List;

public interface IExtractor< TYPE > {

    interface Entity< TYPE > {
        String getId();
        TYPE getValue();
    }


    class Response< TYPE > {

        private List< ? extends IExtractor.Entity< TYPE > > entities;

        private String cursor;

        private boolean hasNext;


        public Response() {

        }

        public List<? extends IExtractor.Entity< TYPE >> getEntities() {
            return entities;
        }

        public void setEntities(List<? extends IExtractor.Entity< TYPE >> entities) {
            this.entities = entities;
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


    Response< TYPE > get(String cursor);

    Response< TYPE > get(List< String > ids, String cursor);

}
