package database;

//import java.io.*;
import java.sql.*;
//import java.util.*;

public class JavaPostgres {

    public static void main(String[] args) {

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            String dbURL = "jdbc:postgresql://192.168.61.205:5432/license";
            String username = "postgres";
            String password = " ";

            Class.forName("com.mysql.jdbc.Driver");

            conn =
                DriverManager.getConnection(dbURL, username, password);

            stmt = conn.createStatement();

            if (stmt.execute("select * from users")) {
                rs = stmt.getResultSet();
            } else {
                System.err.println("select failed");
            }
            while (rs.next()) {
                String entry = rs.getString(4);

                System.out.println(entry);
            }

        } catch (ClassNotFoundException ex) {
            System.err.println("Failed to load mysql driver");
            System.err.println(ex);
        } catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) { /* ignore */ }
                rs = null;
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) { /* ignore */ }
                stmt = null;
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) { /* ignore */ }
                conn = null;
            }
        }
    }
}




