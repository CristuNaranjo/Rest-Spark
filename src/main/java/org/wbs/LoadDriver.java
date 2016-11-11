package org.wbs;

/**
 * Created by cristu on 30/08/16.
 */

import org.json.JSONObject;

import java.io.FileWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class LoadDriver {

    List<String> getFilenames() {
        return filenames;
    }

    public static List<String> filenames = new ArrayList<String>();

    public void main(List<String> dataDb){
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            Connection conn = null;
            try {
                conn = DriverManager.getConnection(dataDb.get(0), dataDb.get(1), dataDb.get(2));
                Statement stmt = null;
                ResultSet rs = null;
                ResultSet rsFilter = null;
                Statement statFilter = null;
                try {
                    stmt = conn.createStatement();
                    rs = stmt.executeQuery("SELECT DISTINCT user_id FROM social_reviews");
                    ResultSetMetaData rsMeta = rs.getMetaData();
                    int columnCnt = rsMeta.getColumnCount();
                    while(rs.next()) { // convert each object to an human readable JSON object
                        for(int i=1;i<=columnCnt;i++) {
                            String value = rs.getString(i);
                            String query = "SELECT * FROM social_reviews WHERE user_id=" + value;
                            statFilter = conn.createStatement();

                            rsFilter = statFilter.executeQuery(query);

                            List<JSONObject> result =  getFormattedResult(rsFilter);
                            String filename = String.valueOf(result);
                            filenames.add(filename);
                        }
                    }

                }
                catch (SQLException ex){
                    // handle any errors
                    System.out.println("SQLException: " + ex.getMessage());
                    System.out.println("SQLState: " + ex.getSQLState());
                    System.out.println("VendorError: " + ex.getErrorCode());
                }
                finally {
                    if (rs != null) {
                        try {
                            rs.close();
                        } catch (SQLException sqlEx) { } // ignore

                        rs = null;
                    }

                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException sqlEx) { } // ignore

                        stmt = null;
                    }
                    if (rsFilter != null) {
                        try {
                            rsFilter.close();
                        } catch (SQLException sqlEx) { } // ignore

                        rs = null;
                    }

                    if (statFilter != null) {
                        try {
                            statFilter.close();
                        } catch (SQLException sqlEx) { } // ignore

                        stmt = null;
                    }
                }

            } catch (SQLException ex) {
                // handle any errors
                System.out.println("SQLException: " + ex.getMessage());
                System.out.println("SQLState: " + ex.getSQLState());
                System.out.println("VendorError: " + ex.getErrorCode());
            }
        } catch (Exception ex) {
            // handle the error
        }
    }

    public static List<JSONObject> getFormattedResult(ResultSet rs) {
        List<JSONObject> resList = new ArrayList<JSONObject>();
        try {
            // get column names
            ResultSetMetaData rsMeta = rs.getMetaData();
            int columnCnt = rsMeta.getColumnCount();
            List<String> columnNames = new ArrayList<String>();
            for(int i=1;i<=columnCnt;i++) {
                columnNames.add(rsMeta.getColumnName(i).toUpperCase());
            }

            while(rs.next()) { // convert each object to an human readable JSON object
                JSONObject obj = new JSONObject();
                for(int i=1;i<=columnCnt;i++) {
                    String key = columnNames.get(i - 1);
                    String value = rs.getString(i);
                    obj.put(key, value);
                }
                resList.add(obj);
            }
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return resList;
    }

}