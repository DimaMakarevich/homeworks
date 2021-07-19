import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class MysqlConnector {
    public static void runQuery(String query) {
        Properties props = new Properties();
        props.setProperty("allowLoadLocalInfile", "true");
        props.setProperty("user", Config.username);
        props.setProperty("password", Config.password);
        try (Connection connection = DriverManager.getConnection(Config.url, props)) {
            Statement st = connection.createStatement();
            st.executeUpdate(query);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot connect the database!", e);
        }
    }

    public static ArrayList<ArrayList<String>> runSelectQuery(String query, boolean isReturnResult) {
        Properties props = new Properties();
        props.setProperty("allowLoadLocalInfile", "true");
        props.setProperty("user", Config.username);
        props.setProperty("password", Config.password);
        ArrayList<ArrayList<String>> returnFromSelectQuery = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(Config.url, props)) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            if (isReturnResult) {
                while (resultSet.next()) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int numberOfColumns = metaData.getColumnCount();
                    ArrayList<String> rowData = new ArrayList<>();
                    for (int columnCounter = 1; columnCounter <= numberOfColumns; columnCounter++) {
                        rowData.add(resultSet.getString(columnCounter));
                    }
                    returnFromSelectQuery.add(rowData);
                }
            }

        } catch (SQLException e) {
            throw new IllegalStateException("Cannot connect the database!", e);
        }
        return returnFromSelectQuery;
    }

    public static void db_setup() {
        String createAuditDb = "CREATE DATABASE IF NOT EXISTS java_audit_database";
        String createEtlDb = "CREATE DATABASE IF NOT EXISTS java_etl_database";
        String createJavaAuditTableSqlQuery = "CREATE TABLE IF NOT EXISTS java_audit_database.java_audit_file_load_table " +
                "(`checksum` VARCHAR(64), `start_timestamp` TIMESTAMP, `file_name` TEXT, `market` VARCHAR(10)" +
                ", `lines_inserted` BIGINT, `end_timestamp` TIMESTAMP)";
        String createStgTableSqlQuery = "CREATE TABLE IF NOT EXISTS java_etl_database.java_stg_table " +
                "(`year_month` VARCHAR(7), `open` DECIMAL(10, 3), `close` DECIMAL(10, 3), `open_time` TIMESTAMP, `close_time` TIMESTAMP,  UNIQUE KEY `year_month` (`year_month`))";
        String createAuditStgTableSqlQuery = "CREATE TABLE IF NOT EXISTS java_audit_database.java_audit_stg_table" +
                " (`year_month` VARCHAR(7), `open` DECIMAL(10, 3), `close` DECIMAL(10, 3), `open_time` TIMESTAMP, `close_time` TIMESTAMP, `load_timestamp` TIMESTAMP)";
        String createDestinationTableSqlQuery = "CREATE TABLE IF NOT EXISTS java_etl_database.java_destination_table " +
                " (`year` VARCHAR(4), `Jan` DECIMAL(10, 3), `Feb` DECIMAL(10, 3), `Mar` DECIMAL(10, 3),`Apr` DECIMAL(10, 3)" +
                ", `May` DECIMAL(10, 3), `Jun` DECIMAL(10, 3), `Jul` DECIMAL(10, 3), `Aug` DECIMAL(10, 3), `Sep` DECIMAL(10, 3)" +
                ", `Oct` DECIMAL(10, 3), `Nov` DECIMAL(10, 3), `Dec` DECIMAL(10, 3), `TOTAL` DECIMAL(10, 3))";
        String createAuditDestinationTableSqlQuery = "CREATE TABLE IF NOT EXISTS java_audit_database.java_audit_destination_table " +
                " (`year` VARCHAR(4), `Jan` DECIMAL(10, 3), `Feb` DECIMAL(10, 3), `Mar` DECIMAL(10, 3),`Apr` DECIMAL(10, 3)" +
                ", `May` DECIMAL(10, 3), `Jun` DECIMAL(10, 3), `Jul` DECIMAL(10, 3), `Aug` DECIMAL(10, 3), `Sep` DECIMAL(10, 3)" +
                ", `Oct` DECIMAL(10, 3), `Nov` DECIMAL(10, 3), `Dec` DECIMAL(10, 3), `TOTAL` DECIMAL(10, 3), `load_timestamp` TIMESTAMP(3))";
        runQuery(createAuditDb);
        runQuery(createEtlDb);
        runQuery(createJavaAuditTableSqlQuery);
        runQuery(createStgTableSqlQuery);
        runQuery(createAuditStgTableSqlQuery);
        runQuery(createDestinationTableSqlQuery);
        runQuery(createAuditDestinationTableSqlQuery);
    }

    public static ArrayList<ArrayList<String>> getIncrementalLoadStgTable(ArrayList<ArrayList<String>> openCloseMonthData) {
        for (ArrayList<String> monthValue : openCloseMonthData) {
            String yearMonth = monthValue.get(0);
            String open = monthValue.get(1);
            String close = monthValue.get(2);
            String currentOpenTime = monthValue.get(3);
            String currentCloseTime = monthValue.get(4);
            String loadTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
            String updateSqlQuery = String.format("INSERT INTO java_etl_database.java_stg_table" +
                            " SET `year_month` = '%s'," +
                            " `open` = %s," +
                            " `close` = %s," +
                            " `open_time` =  '%s'," +
                            " `close_time` = '%s'" +
                            " ON DUPLICATE KEY UPDATE" +
                            " `open` = IF(`open_time` > '%s', '%s', `open`)," +
                            " `close` = IF(`close_time` < '%s', '%s', `close`)," +
                            " `open_time` = IF(`open_time` > '%s', '%s', `open_time`)," +
                            " `close_time` = IF(`close_time` < '%s', '%s', `close_time`)"
                    , yearMonth
                    , open
                    , close
                    , currentOpenTime
                    , currentCloseTime
                    , currentOpenTime, open
                    , currentCloseTime, close
                    , currentOpenTime, currentOpenTime
                    , currentCloseTime, currentCloseTime
            );
            String insertIntoAuditStgTable = String.format("INSERT INTO java_audit_database.java_audit_stg_table" +
                            " VALUES ('%s', %s, %s, '%s', '%s', '%s')"
                    , yearMonth
                    , open
                    , close
                    , currentOpenTime
                    , currentCloseTime
                    , loadTimestamp);
            runQuery(updateSqlQuery);
            runQuery(insertIntoAuditStgTable);
        }
        String selectUpdatedStgDataSqlQuery = "SELECT * FROM  java_etl_database.java_stg_table";
        return runSelectQuery(selectUpdatedStgDataSqlQuery, true);
    }

    public static void updateDestinationTable(HashMap<String, ArrayList<Double>> data) {
        truncateDestinationTable();
        TreeMap<String, ArrayList<Double>> sortedData = new TreeMap<>(data);
        for (String keyYear : sortedData.keySet()) {
            double Jan = sortedData.get(keyYear).get(0);
            double Feb = sortedData.get(keyYear).get(1);
            double Mar = sortedData.get(keyYear).get(2);
            double Apr = sortedData.get(keyYear).get(3);
            double May = sortedData.get(keyYear).get(4);
            double Jun = sortedData.get(keyYear).get(5);
            double Jul = sortedData.get(keyYear).get(6);
            double Aug = sortedData.get(keyYear).get(7);
            double Sep = sortedData.get(keyYear).get(8);
            double Oct = sortedData.get(keyYear).get(9);
            double Nov = sortedData.get(keyYear).get(10);
            double Dec = sortedData.get(keyYear).get(11);
            double AVG = sortedData.get(keyYear).get(12);
            String loadTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
            String insertIntoSqlQuery = String.format("INSERT INTO java_etl_database.java_destination_table" +
                            " VALUES('%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    , keyYear
                    , Jan
                    , Feb
                    , Mar
                    , Apr
                    , May
                    , Jun
                    , Jul
                    , Aug
                    , Sep
                    , Oct
                    , Nov
                    , Dec
                    , AVG
            );
            String insertIntoAuditDestinationTable = String.format("INSERT INTO java_audit_database.java_audit_destination_table" +
                            " VALUES('%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '%s')"
                    , keyYear
                    , Jan
                    , Feb
                    , Mar
                    , Apr
                    , May
                    , Jun
                    , Jul
                    , Aug
                    , Sep
                    , Oct
                    , Nov
                    , Dec
                    , AVG
                    , loadTimestamp
            );
            runQuery(insertIntoSqlQuery);
            runQuery(insertIntoAuditDestinationTable);
        }
    }

    public static ArrayList<ArrayList<String>> getDestinationTable() {
        String selectFromDestinationSqlQuery = "SELECT * FROM java_etl_database.java_destination_table" +
                " ORDER BY `year`";
        return runSelectQuery(selectFromDestinationSqlQuery, true);
    }

    public static void cleanAllTables() {
        cleanEtlTables();
        cleanAllAuditTables();
    }

    private static void cleanEtlTables() {
        truncateDestinationTable();
        truncateStgTable();
    }

    private static void cleanAllAuditTables() {
        truncateAuditFileLoadTable();
        truncateAuditStgTable();
        truncateAuditDestinationTable();
    }

    private static void truncateDestinationTable() {
        String truncateSqlQuery = "TRUNCATE TABLE java_etl_database.java_destination_table";
        runQuery(truncateSqlQuery);
    }

    private static void truncateStgTable() {
        String truncateSqlQuery = "TRUNCATE TABLE java_etl_database.java_stg_table";
        runQuery(truncateSqlQuery);
    }

    private static void truncateAuditFileLoadTable() {
        String truncateSqlQuery = "TRUNCATE TABLE java_audit_database.java_audit_file_load_table";
        runQuery(truncateSqlQuery);
    }

    private static void truncateAuditStgTable() {
        String truncateSqlQuery = "TRUNCATE TABLE java_audit_database.java_audit_stg_table";
        runQuery(truncateSqlQuery);
    }

    private static void truncateAuditDestinationTable() {
        String truncateSqlQuery = "TRUNCATE TABLE java_audit_database.java_audit_destination_table";
        runQuery(truncateSqlQuery);
    }
}
