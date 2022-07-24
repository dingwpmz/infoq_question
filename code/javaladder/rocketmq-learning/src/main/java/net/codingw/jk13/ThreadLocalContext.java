package net.codingw.jk13;

public class ThreadLocalContext {
    private static final ThreadLocal<String> tagContext  = new ThreadLocal<>();

    public static void setTag(String tag) {
        tagContext.set(tag);
    }

    public static String getTag() {
        return tagContext.get();
    }

    public static void resetTag() {
        tagContext.remove();
    }
}
