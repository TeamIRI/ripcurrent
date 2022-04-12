/*
 * Copyright (c) 2022 Innovative Routines International (IRI), Inc.
 *
 * Description: Match on data using a regular expression pattern.
 *
 * Contributors:
 *     devonk
 */
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatternMatcher implements DataMatcher {
    private final String pattern;

    PatternMatcher(String pattern) {
        this.pattern = pattern;
    }

    @Override
    public Boolean isMatch(String data) {
        final Matcher matcher = Pattern.compile(this.pattern).matcher(data);
        return matcher.find();
    }
}
