package net.codingw.datastruct;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K,V> extends LinkedHashMap<K,V> {
    private int maxCapacity;
    protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
        //如果超过了最大容量，则启动剔除机制
        return size() >= maxCapacity;
    }
    public void setMaxCapacity(int maxCapacity) {
        this.maxCapacity = maxCapacity;
    }
}
