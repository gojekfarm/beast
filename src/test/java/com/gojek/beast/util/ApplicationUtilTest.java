package com.gojek.beast.util;

import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertEquals;

public class ApplicationUtilTest {

    @Test
    public void testDateIsFormattedAsExpected() {
        final String expectedFormattedDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        assertEquals("Expected formatted date to match based on yyyy-MM-dd pattern", expectedFormattedDate, ApplicationUtil.getFormattedDate("yyyy-MM-dd", Instant.now()));
    }
}
