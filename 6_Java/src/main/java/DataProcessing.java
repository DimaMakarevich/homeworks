import java.util.*;

public class DataProcessing {

    public static HashMap<String, ArrayList<Double>> calculate(ArrayList<ArrayList<String>> data, String calculation_mode) {
     /*   HashMap<String, ArrayList<ArrayList<String>>> groupedData = groupByYearAndMonth(data);
        data = null;
        System.gc();
        ArrayList<ArrayList<String>> openCloseMonthData = getOpenCloseMonthValue(groupedData);
        groupedData = null;
        System.gc();*/
        ArrayList<ArrayList<String>> fullStgDataFromMySql = MysqlConnector.getIncrementalLoadStgTable(data);
        ArrayList<ArrayList<String>> prevCloseData = getPrevCloseValue(fullStgDataFromMySql);
        ArrayList<ArrayList<String>> growthRateData = getGrowthRateValue(prevCloseData, calculation_mode);
        HashMap<String, ArrayList<Double>> pivotByMonthGrowthRateData = getPivotByMonthTable(growthRateData);
        HashMap<String, ArrayList<Double>> withTotalColumn = addTotalColumn(pivotByMonthGrowthRateData);
        HashMap<String, ArrayList<Double>> withAverageRow = addAverageRow(withTotalColumn);
        MysqlConnector.updateDestinationTable(withAverageRow);
        return withAverageRow;
    }

    /**
     * Method 'groupByYearAndMonth':
     * Input: [
     * [2012-01-16 00:00:00, 2012, 01, '1291,4', '1291,4'],
     * [2012-01-16 00:00:10, 2012, 01, '1291,4', '1291,4'],
     * [2012-01-16 00:00:00, 2012, 01, '1291,6', '1291,4'],
     * ...
     * ]
     * Output: {
     * 2016-01=[[2016-01-01 00:00:00, '2024,669', '2024,669'], [2016-01-01 00:00:10, '2024,669', '2024,669'], ...],
     * 2015-10=[[2015-10-22 02:02:10, '2020,436', '2020,436'], [2015-10-22 02:02:20, '2020,436', '2020,436'], ...],
     * ...
     * }
     */
    public static HashMap<String, ArrayList<ArrayList<String>>> groupByYearAndMonth(ArrayList<ArrayList<String>> data) {
        HashMap<String, ArrayList<ArrayList<String>>> groupedByYear = new HashMap<>();
        for (int counter = 0; counter < data.size(); counter++) {
            String yearAndMonth = data.get(counter).get(0).substring(0, 7);
            String time = data.get(counter).get(0);
            String open = data.get(counter).get(1);
            String close = data.get(counter).get(2);
            if (groupedByYear.containsKey(yearAndMonth)) {
                groupedByYear.get(yearAndMonth).add(new ArrayList<>(Arrays.asList(time, open, close)));
            } else {
                ArrayList<ArrayList<String>> monthArray = new ArrayList<>();
                monthArray.add(new ArrayList<>(Arrays.asList(time, open, close)));
                groupedByYear.put(yearAndMonth, monthArray);
            }
        }
        return groupedByYear;
    }

    /**
     * Method 'getOpenCloseMonthValue':
     * Input: {
     * 2016-01=[[2016-01-01 00:00:00, '2024,669', '2024,669'], [2016-01-01 00:00:10, '2024,669', '2024,669'], ...],
     * 2015-10=[[2015-10-22 02:02:10, '2020,436', '2020,436'], [2015-10-22 02:02:20, '2020,436', '2020,436'], ...],
     * ..
     * }
     * Output: [
     * [2016-01, '2024,669',' 2024,669', 2016-01-01 00:00:00, 2016-01-01 00:03:00],
     * [2015-10, '2020,436', '2020,436', 2015-10-22 02:02:10, 2015-10-22 02:06:50],
     * ...
     * ]
     */
    public static ArrayList<ArrayList<String>> getOpenCloseMonthValue(HashMap<String, ArrayList<ArrayList<String>>> groupedData) {
        ArrayList<ArrayList<String>> openCloseMonthArray = new ArrayList<>();
        for (String yearMonth : groupedData.keySet()) {
            ArrayList<ArrayList<String>> monthArray = groupedData.get(yearMonth);
            monthArray.sort(Comparator.comparing(o -> o.get(0)));
            String open = monthArray.get(0).get(1).replace(",", ".");
            String close = monthArray.get(monthArray.size() - 1).get(2).replace(",", ".");
            String openTime = monthArray.get(0).get(0);
            String closeTime = monthArray.get(monthArray.size() - 1).get(0);
            openCloseMonthArray.add(new ArrayList<>(Arrays.asList(yearMonth, open, close, openTime, closeTime)));
        }
        return openCloseMonthArray;
    }

    /**
     * Method 'getPrevCloseValue':
     * Input: [
     * [2016-01, '2024,669',' 2024,669', 2016-01-01 00:00:00, 2016-01-01 00:03:00],
     * [2015-10, '2020,436', '2020,436', 2015-10-22 02:02:10, 2015-10-22 02:06:50],
     * ...
     * ]
     * Output: [
     * [2012-01, 1291.400, 1314.100, 1291.400],
     * [2012-08, 1409.750, 1409.640, 1314.100],
     * [2012-10, 1438.170, 1437.600, 1409.640],
     * ...
     * ]
     */
    private static ArrayList<ArrayList<String>> getPrevCloseValue(ArrayList<ArrayList<String>> openCloseMonthData) {
        ArrayList<ArrayList<String>> prevCloseArray = new ArrayList<>();
        openCloseMonthData.sort(Comparator.comparing(monthData -> monthData.get(0))); //compare by yearMonth value
        for (int counter = 0; counter < openCloseMonthData.size(); counter++) {
            String open = openCloseMonthData.get(counter).get(1);
            String close = openCloseMonthData.get(counter).get(2);
            String yearMonth = openCloseMonthData.get(counter).get(0);
            String prevClose;
            if (counter == 0) {
                prevClose = open;
            } else {
                prevClose = openCloseMonthData.get(counter - 1).get(2);
            }
            prevCloseArray.add(new ArrayList<>(Arrays.asList(yearMonth, open, close, prevClose)));
        }
        return prevCloseArray;
    }

    /**
     * Method 'getGrowthRateValue':
     * Input: [
     * [2012-01, 1291.400, 1314.100, 1291.400],
     * [2012-08, 1409.750, 1409.640, 1314.100],
     * [2012-10, 1438.170, 1437.600, 1409.640],
     * ...
     * ]
     * Output: [
     * [2012-01, '1,758'],
     * [2012-08, '-0,008'],
     * [2012-10, '-0,040'],
     * ...
     * ]
     */
    private static ArrayList<ArrayList<String>> getGrowthRateValue(ArrayList<ArrayList<String>> prevCloseData, String calculation_mode) {
        ArrayList<ArrayList<String>> growthRateArray = new ArrayList<>();
        for (ArrayList<String> monthData : prevCloseData) {
            String yearMonth = monthData.get(0);
            double open = Double.parseDouble(monthData.get(1));
            double close = Double.parseDouble(monthData.get(2));
            double prevClose = Double.parseDouble(monthData.get(3));

            double growthRate;
            switch (calculation_mode) {
                case ("oc"): {
                    growthRate = (close / open - 1) * 100;
                    break;
                }
                case ("cc"): {
                    growthRate = (close / prevClose - 1) * 100;
                    break;
                }
                default: {
                    throw new IllegalArgumentException("Invalid calculation mode");
                }
            }
            growthRateArray.add(new ArrayList<>(Arrays.asList(yearMonth, String.format("%.3f", growthRate))));
        }
        return growthRateArray;
    }

    /**
     * Method 'getPivotByMonthTable':
     * Input: [
     * [2012-01, '1,758'],
     * [2012-08, '-0,008'],
     * [2012-10, '-0,040'],
     * ...
     * ]
     * Output: {
     * 2018=[0.0, 0.0, -0.007, -0.0, 0.0, 0.0, -0.011, 0.0, 0.0, 0.0, 0.0, 0.0],
     * 2017=[0.0, 0.0, 0.0, 0.0, 0.032, -0.032, 0.0, 0.24, 0.0, 0.0, 0.0, 0.0],
     * 2016=[0.006, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5.781, 0.0, 0.0, 0.0, 0.0],
     * ...
     * }
     */
    private static HashMap<String, ArrayList<Double>> getPivotByMonthTable(ArrayList<ArrayList<String>> growthRateData) {
        HashMap<String, ArrayList<Double>> groupedByYearMonthGrowthRate = new HashMap<>();
        for (ArrayList<String> row : growthRateData) {
            String year = row.get(0).substring(0, 4);
            int indexMonthColumn = Integer.parseInt(row.get(0).substring(5, 7)) - 1;
            double growthRate = Double.parseDouble(row.get(1).replace(",", "."));
            if (groupedByYearMonthGrowthRate.containsKey(year)) {
                groupedByYearMonthGrowthRate.get(year).set(indexMonthColumn, growthRate);
            } else {
                //Double[] growthRateEmptyArray = new Double[12];
                // ArrayList<Double> growthRateArrayList = new ArrayList<>(Arrays.asList(growthRateEmptyArray));
                ArrayList<Double> growthRateArrayList = new ArrayList<>();
                int monthInYear = 12;
                for (int counter = 0; counter < monthInYear; counter++) {
                    growthRateArrayList.add(0.0);
                }
                growthRateArrayList.set(indexMonthColumn, growthRate);
                groupedByYearMonthGrowthRate.put(year, growthRateArrayList);
            }
        }
        return groupedByYearMonthGrowthRate;
    }

    /**
     * Method 'addTotalColumn':
     * Input: {
     * 2018=[0.0, 0.0, -0.007, -0.0, 0.0, 0.0, -0.011, 0.0, 0.0, 0.0, 0.0, 0.0],
     * 2017=[0.0, 0.0, 0.0, 0.0, 0.032, -0.032, 0.0, 0.24, 0.0, 0.0, 0.0, 0.0],
     * 2016=[0.006, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5.781, 0.0, 0.0, 0.0, 0.0],
     * ...
     * }
     * Output: {
     * 2018=[0.0, 0.0, -0.007, -0.0, 0.0, 0.0, -0.011, 0.0, 0.0, 0.0, 0.0, 0.0, -0.018],
     * 2017=[0.0, 0.0, 0.0, 0.0, 0.032, -0.032, 0.0, 0.24, 0.0, 0.0, 0.0, 0.0, 0.024],
     * 2016=[0.006, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5.781, 0.0, 0.0, 0.0, 0.0, 5.787],
     * ...
     * }
     */
    private static HashMap<String, ArrayList<Double>> addTotalColumn(HashMap<String, ArrayList<Double>> pivotByMonthGrowthRateData) {
        HashMap<String, ArrayList<Double>> withTotalColumn = getMapDeepCopy(pivotByMonthGrowthRateData);
        double total;
        for (String keyYear : withTotalColumn.keySet()) {
            total = withTotalColumn.get(keyYear).stream().mapToDouble(growthRateValue -> growthRateValue).sum();
            total = Double.parseDouble(String.format("%.3f", total).replace(",", "."));
            withTotalColumn.get(keyYear).add(total);
        }
        return withTotalColumn;
    }

    /**
     * Method 'addAverageRow':
     * Input: {
     * 2018=[0.0, 0.0, -0.007, -0.0, 0.0, 0.0, -0.011, 0.0, 0.0, 0.0, 0.0, 0.0, -0.018],
     * 2017=[0.0, 0.0, 0.0, 0.0, 0.032, -0.032, 0.0, 0.24, 0.0, 0.0, 0.0, 0.0, 0.024],
     * 2016=[0.006, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5.781, 0.0, 0.0, 0.0, 0.0, 5.787],
     * ...
     * }
     * Input: {
     * 2018=[0.0, 0.0, -0.007, -0.0, 0.0, 0.0, -0.011, 0.0, 0.0, 0.0, 0.0, 0.0, -0.018],
     * 2017=[0.0, 0.0, 0.0, 0.0, 0.032, -0.032, 0.0, 0.24, 0.0, 0.0, 0.0, 0.0, 0.024],
     * 2016=[0.006, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5.781, 0.0, 0.0, 0.0, 0.0, 5.787],
     * ...
     * Avg=[0.251, 0.0, 0.0, 0.0, 0.002, 0.0, 0.005, -0.004, -0.0, -0.01, 0.824, 0.304, 1.371]
     * }
     */
    private static HashMap<String, ArrayList<Double>> addAverageRow(HashMap<String, ArrayList<Double>> withTotalColumn) {
        HashMap<String, ArrayList<Double>> withAverageRow = getMapDeepCopy(withTotalColumn);
        int monthArraySize = 13;
        ArrayList<Double> averageRowValue = new ArrayList<>();
        for (int monthCounter = 0; monthCounter < monthArraySize; monthCounter++) {
            int yearCounter = 0;
            Double totalValue = 0.0;
            double averageRowMonthValue;
            for (String yearKey : withAverageRow.keySet()) {
                yearCounter++;
                totalValue += withAverageRow.get(yearKey).get(monthCounter);
            }
            try {
                averageRowMonthValue = Double.parseDouble(String.format("%.3f", totalValue / yearCounter).replace(",", "."));
            } catch (Exception e) {
                throw new IllegalArgumentException("Divided by zero");
            }
            averageRowValue.add(averageRowMonthValue);
        }
        String avgRowName = "AVG";
        withAverageRow.put(avgRowName, averageRowValue);
        return withAverageRow;
    }

    private static HashMap<String, ArrayList<Double>> getMapDeepCopy(HashMap<String, ArrayList<Double>> data) {
        HashMap<String, ArrayList<Double>> deepCopy = new HashMap<>();
        for (String Key : data.keySet()) {
            ArrayList<Double> arrayValue = new ArrayList<>(data.get(Key));
            deepCopy.put(Key, arrayValue);
        }
        return deepCopy;
    }
}
