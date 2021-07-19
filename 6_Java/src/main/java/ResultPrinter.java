import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

public class ResultPrinter {
    public static void printResult(HashMap<String, ArrayList<Double>> data) {
        TreeMap<String, ArrayList<Double>> sortedData = new TreeMap<>(data);
        String columnsNames = "year, Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec, Total".replaceAll(",", "\\\t");
        System.out.println(columnsNames);
        for (String key : sortedData.keySet()) {
            String rowData = key + " \t" + sortedData.get(key).toString().replaceAll("(\\[|\\])", "").replaceAll(",", "\\\t");
            System.out.println(rowData);
        }
    }

    public static void printResult(ArrayList<ArrayList<String>> data) {
        String columnsNames = "year, Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec, Total".replaceAll(",", "\\\t");
        System.out.println(columnsNames);
        for (ArrayList<String> row : data) {
            if (row.get(0).equals("AVG")) {
                row.set(0, "AVG ");
            }
            String rowData = row.toString().replaceAll("(\\[|\\])", "").replaceAll(",", "\\\t");
            System.out.println(rowData);
        }
    }
}