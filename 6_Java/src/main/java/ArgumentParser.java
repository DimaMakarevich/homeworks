import org.apache.commons.cli.*;

import java.util.HashMap;

public class ArgumentParser {
    public HashMap<String, String> parse(String[] args) throws ParseException {
        Options posixOptions = new Options();
        Option calculationMode = new Option("cm", "calculation_mode", true, "choose calculation mode, default mode 'open-close'");
        Option loadMode = new Option("lm", "load_mode", true, "choose load mode, default mode 'incremental'");
        Option setupDB = new Option("sdb", "setup_DB", false, "setup db parameter");
        Option getRes = new Option("gr", "get_result", false, "get result parameter");
        posixOptions.addOption(calculationMode);
        posixOptions.addOption(loadMode);
        posixOptions.addOption(setupDB);
        posixOptions.addOption(getRes);
        CommandLineParser cmdLinePosixParser = new DefaultParser();
        CommandLine commandLine = cmdLinePosixParser.parse(posixOptions, args);
        String calculationModeValue;
        String loadModeValue;
        String setupDbFlag;
        String getResultFlag;
        if (commandLine.hasOption("cm")) {
            calculationModeValue = commandLine.getOptionValue("cm");
        } else {
            calculationModeValue = "oc";
        }

        if (commandLine.hasOption("lm")) {
            loadModeValue = commandLine.getOptionValue("lm");
        } else {
            loadModeValue = "incr";
        }

        if (commandLine.hasOption("sdb")) {
            setupDbFlag = "true";
        } else {
            setupDbFlag = "false";
        }

        if (commandLine.hasOption("gr")) {
            getResultFlag = "true";
        } else {
            getResultFlag = "false";
        }

        HashMap<String, String> arguments = new HashMap<>();
        arguments.put("cm", calculationModeValue);
        arguments.put("lm", loadModeValue);
        arguments.put("sdb", setupDbFlag);
        arguments.put("gr", getResultFlag);
        return arguments;
    }
}

