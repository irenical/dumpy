package org.irenical.dumpy.impl.db;

import org.irenical.drowsy.query.Query;
import org.irenical.drowsy.transaction.JdbcOperation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcInsertOperation extends JdbcOperation< Integer > {

    private final Query query;

    public JdbcInsertOperation(Query query) {
        this.query = query;
    }

    @Override
    protected Integer execute(Connection connection) throws SQLException {
        PreparedStatement statement = query.createPreparedStatement(connection);
        statement.executeUpdate();
        ResultSet generatedKeys = statement.getGeneratedKeys();
        return generatedKeys != null && generatedKeys.next() ? generatedKeys.getInt( 1 ) : null;
    }

}
