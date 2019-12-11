package com.gojek.beast.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Utility class that composes all helper/util methods for this application.
 */
public final class ApplicationUtil {

    public static String getFormattedDate(String pattern, Instant date) {
        return DateTimeFormatter
                .ofPattern(pattern)
                .withZone(ZoneId.systemDefault())
                .format(date);
    }
}
