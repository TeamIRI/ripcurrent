/*
 * Copyright (c) 2022 Innovative Routines International (IRI), Inc.
 *
 * Description: Convert internal date/time values received from the database to ISO string representation.
 *
 * Contributors:
 *     devonk
 */

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

public class DateTimeConversionUtil {


    // Get in a number that represents the number of days (positive or negative) since Jan. 1, 1970.
    // Any extra day that is a part of a leap year is included in this number.
    // Leap years – every 4 years, except every 100 years – UNLESS the year is also divisible by 400.
    public static String integerToDate(Integer number) {
        LocalDate date = LocalDate.ofEpochDay(number);
        return date.toString();
    }

    public static String numberToTime(long number) { // Convert microseconds of day to time.
        LocalTime time = LocalTime.ofNanoOfDay(number * 1000);
        return time.toString();
    }

    public static String numberToDateTime(long number) { // Convert milliseconds since 1970 to datetime.
        long secondsSince1970 = TimeUnit.MILLISECONDS.toSeconds(number);
        int remainingMicros = 0;
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(secondsSince1970, remainingMicros, ZoneOffset.UTC);
        return dateTime.toString();
    }
}
