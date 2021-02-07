package sun;

import java.util.HashMap;
import java.util.Map;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        HashMap<String, String> res = new HashMap<>();
        myEntry<String, String> a = new myEntry<String, String>("1", "2");
        System.out.println(a.getKey());
        System.out.println(a.getValue());
        System.out.println(a.setValue("3"));
    }

    static class myEntry<K, V> implements Map.Entry<K, V> {
        private K k;
        private V v;

        public myEntry(K k, V v) {
            this.k = k;
            this.v = v;
        }

        @Override
        public K getKey() {
            return k;
        }

        @Override
        public V getValue() {
            return v;
        }

        @Override
        public V setValue(V value) {
            this.v = value;
            return v;
        }
    }
}
