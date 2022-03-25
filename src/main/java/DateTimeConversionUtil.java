import java.time.LocalDate;

public class DateTimeConversionUtil {


    // Get in a number that represents the number of days (positive or negative) since Jan. 1, 1970.
    // Any extra day that is a part of a leap year is included in this number.
    // Leap years – every 4 years, except every 100 years – UNLESS the year is also divisible by 400.
    public static String integerToDate(Integer number) {
        LocalDate date = LocalDate.ofEpochDay(number);
        return date.toString();
    }
}
