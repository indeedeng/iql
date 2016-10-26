package com.indeed.squall.iql2.server.print;

public class LevelPrinter {
    private static int INDENT_LENGTH = 4;
    private StringBuilder sb;
    private int depth;

    public LevelPrinter() {
        sb = new StringBuilder();
    }

    public void push(String s) {
        depth++;
        for (int i = 0; i < depth*INDENT_LENGTH; i++) {
            sb.append(' ');
        }
        sb.append(s).append('\n');
    }

    public void pop() {
        if (--depth < 0) {
            throw new IllegalAccessError("no more level to pop");
        }
    }


    @Override
    public String toString() {
        if (depth != 0) {
            throw  new IllegalAccessError(String.format("remaning %d levels to pop", depth));
        }
        return sb.toString();
    }
}