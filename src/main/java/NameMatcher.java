/*
 * Copyright (c) 2022 Innovative Routines International (IRI), Inc.
 *
 * Description: Match on a database column name using a regular expression pattern.
 *
 * Contributors:
 *     devonk
 */

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NameMatcher {
    String patternString;
    Pattern pattern;

    NameMatcher(String patternString) {
        this.patternString = patternString;
        this.pattern = Pattern.compile(patternString);
    }

    public Boolean isMatch(String column) {
        final Matcher matcher = this.pattern.matcher(column);
        return matcher.matches();
    }

    public String getPatternString() {
        return patternString;
    }

    public void setPatternString(String patternString) {
        this.patternString = patternString;
    }
}
