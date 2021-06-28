package com.tianang.mapreduce.outputformat;

import java.util.ArrayList;

public class LogArgsList {

    private LogArgsList() { }

    private ArrayList<String> argsList = new ArrayList<>();

    private static class InnerClassSingleton {
        private final static LogArgsList INNER_GLOBAL_VARIABLE = new LogArgsList();
    }

    static LogArgsList getInstance() {
        return InnerClassSingleton.INNER_GLOBAL_VARIABLE;
    }

    void add(String arg) {
        argsList.add(arg);
    }

    String get(int index) {
        return argsList.get(index);
    }
}
