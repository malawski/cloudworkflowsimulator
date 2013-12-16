package cws.core.config;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class CommandLineBuilder {
    Options options;
    List<String> argList = new ArrayList<String>();

    public CommandLineBuilder(Options options) {
        this.options = options;
    }

    public static CommandLineBuilder fromOptions(Options options) {
        return new CommandLineBuilder(options);
    }

    public CommandLineBuilder addOption(String optionName, String value) {
        argList.add("--" + optionName);
        argList.add(value);
        return this;
    }

    public CommandLineBuilder addShortOption(String shortOptionName, String value) {
        argList.add("-" + shortOptionName);
        argList.add(value);
        return this;
    }

    public CommandLine build() throws ParseException {
        CommandLineParser parser = new PosixParser();
        return parser.parse(options, getArgs());
    }

    private String[] getArgs() {
        return argList.toArray(new String[argList.size()]);
    }
}
