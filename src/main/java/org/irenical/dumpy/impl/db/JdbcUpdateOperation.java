package org.irenical.dumpy.impl.db;

import org.irenical.drowsy.query.Query;
import org.irenical.drowsy.transaction.JdbcOperation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JdbcUpdateOperation extends JdbcOperation< Boolean > {

    private final Query query;

    public JdbcUpdateOperation(Query query) {
        this.query = query;
    }

    @Override
    protected Boolean execute(Connection connection) throws SQLException {
        PreparedStatement statement = query.createPreparedStatement(connection);
        int update = statement.executeUpdate();
        return update > 0;
    }

}
