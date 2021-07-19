import java.util.ArrayList;
import java.util.HashMap;

public class Main {
    public static void main(String[] args) throws Exception {
        ArgumentParser parser = new ArgumentParser();
        String[] args2 = new String[]{"-sdb"};
        HashMap<String, String> arguments = parser.parse(args);
        if (arguments.get("sdb").equals("true")) {
            MysqlConnector.db_setup();
        }
        if (arguments.get("lm").equals("full")) {
            MysqlConnector.cleanAllTables();
        }
        if (arguments.get("gr").equals("true")) {
            ResultPrinter.printResult(MysqlConnector.getDestinationTable());
        } else {
            long start = System.currentTimeMillis();
            ResultPrinter.printResult(DataProcessing.calculate(Loader.loadData(Config.data_path), arguments.get("cm")));
            long finish = System.currentTimeMillis();
            long duration = finish - start;
            System.out.println("Duration: " + duration / 1000);
        }
    }
}