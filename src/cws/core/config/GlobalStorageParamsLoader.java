package cws.core.config;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import cws.core.exception.IllegalCWSArgumentException;
import cws.core.storage.global.GlobalStorageParams;

public class GlobalStorageParamsLoader {
    static final String GS_TYPE_SHORT_OPTION_NAME = "gs";
    static final String GS_TYPE_OPTION_NAME = "global-storage";
    private static final Object DEFAULT_GS_TYPE_FILENAME = "default.gs.yaml";
    private static final boolean HAS_ARG = true;

    public static void buildCliOptions(Options options) {
        Option globalStorage = new Option(GS_TYPE_SHORT_OPTION_NAME, GS_TYPE_OPTION_NAME, HAS_ARG, String.format(
                "Global storage config filename, defaults to %s", DEFAULT_GS_TYPE_FILENAME));
        globalStorage.setArgName("FILENAME");
        options.addOption(globalStorage);
    }

    public GlobalStorageParams determineGlobalStorageParams(CommandLine cmd) {
        throw new IllegalCWSArgumentException("");
    }
}
