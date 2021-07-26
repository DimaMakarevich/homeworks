import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import au.com.bytecode.opencsv.CSVReader;

public class Loader {
    @SuppressWarnings("resource")
    public static ArrayList<ArrayList<String>> loadData(String dir_path) throws Exception {
        ArrayList<String> files_name = listFilesForFolder(dir_path);
        ArrayList<ArrayList<String>> resultData = new ArrayList<>();
        for (String file : files_name) {
            ArrayList<ArrayList<String>> csvArrayList = new ArrayList<>();
            String selectCheckSumAndEndTime = "SELECT `checksum`, `end_timestamp`" +
                    " FROM java_audit_database.java_audit_file_load_table" +
                    " ORDER BY `start_timestamp`";
            ArrayList<ArrayList<String>> checksumAndEndTimeLoadArray = MysqlConnector.runSelectQuery(selectCheckSumAndEndTime, true);
            HashMap<String, String> checksumAndEndTimeLoad = new HashMap<>();
            for (ArrayList<String> auditFileData : checksumAndEndTimeLoadArray) {
                String checksum = auditFileData.get(0);
                String endTimestamp = auditFileData.get(1);
                checksumAndEndTimeLoad.put(checksum, endTimestamp);
            }
            String checksum = Checksum.getFileCheckSum(file);
            if (checksumAndEndTimeLoad.containsKey(checksum) && checksumAndEndTimeLoad.get(checksum) != null) {
                System.out.println(String.format("Files '%s' was processed", file));
            } else if (!checksumAndEndTimeLoad.containsKey(checksum) || checksumAndEndTimeLoad.get(checksum) == null) {
                String startTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
                String regexp = "_\\w*_\\d{4}";
                Pattern pattern = Pattern.compile(regexp);
                String market = null;
                try {
                    Matcher matcher = pattern.matcher(file);
                    if (matcher.find()) {
                        int startIndex = matcher.start();
                        market = file.substring(startIndex + 1, startIndex + 4);
                    }
                } catch (Exception e) {
                    throw new IllegalStateException("Cannot recognize market", e);
                }
                String insert_into_sql_query = String.format("INSERT INTO java_audit_database.java_audit_file_load_table " +
                                "VALUES('%s', '%s', '%s', '%s', %s, %s)"
                        , checksum
                        , startTimestamp
                        , file
                        , market
                        , null
                        , null);
                MysqlConnector.runQuery(insert_into_sql_query);
                CSVReader reader = new CSVReader(new FileReader(file), ';', '"', 1);
                String[] nextLine;
                long lines_counter = 0;
                while ((nextLine = reader.readNext()) != null) {
                    String time = nextLine[0], open = nextLine[1], close = nextLine[4];
                    // String year = time.substring(0, 4), month = time.substring(5, 7);
                    csvArrayList.add(new ArrayList<>(Arrays.asList(time, open, close)));
                    lines_counter++;
                }
                String endTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
                String updateAuditTable = String.format("UPDATE java_audit_database.java_audit_file_load_table SET `end_timestamp`= '%s', `lines_inserted` = '%s' " +
                                "WHERE `checksum` = '%s' and `start_timestamp`= '%s'"
                        , endTimestamp
                        , lines_counter
                        , checksum
                        , startTimestamp);
                MysqlConnector.runQuery(updateAuditTable);
            }
            resultData.addAll(DataProcessor.getOpenCloseMonthValue(DataProcessor.groupByYearAndMonth(csvArrayList)));
            csvArrayList = null; // trick help to economy memory and time
            System.gc();
        }
        return resultData;
    }

    public static ArrayList<String> listFilesForFolder(String dir_path) {
        ArrayList<String> files_name = new ArrayList<>();
        File folder = new File(dir_path);
        for (final File fileEntry : Objects.requireNonNull(folder.listFiles((dir, name) -> name.endsWith(".csv")))) {
            files_name.add(fileEntry.getAbsolutePath());
        }
        return files_name;
    }
}








