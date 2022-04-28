/*
 * Copyright (c) 2022 Innovative Routines International (IRI), Inc.
 *
 * Description: Class to hold multiple types of matchers - matchers on data content, and matchers on column name.
 *
 * Contributors:
 *     devonk
 */
public class DataClassMatcher {
    private NameMatcher nameMatcher;
    private DataMatcher dataMatcher;

    DataClassMatcher(NameMatcher nameMatcher, DataMatcher dataMatcher) {
        this.nameMatcher = nameMatcher;
        this.dataMatcher = dataMatcher;
    }

    public DataMatcher getDataMatcher() {
        return dataMatcher;
    }

    public void setDataMatcher(DataMatcher dataMatcher) {
        this.dataMatcher = dataMatcher;
    }

    public NameMatcher getNameMatcher() {
        return nameMatcher;
    }

    public void setNameMatcher(NameMatcher nameMatcher) {
        this.nameMatcher = nameMatcher;
    }

}
