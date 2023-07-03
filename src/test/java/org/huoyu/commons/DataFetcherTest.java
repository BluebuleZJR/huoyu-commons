package org.huoyu.commons;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Author: huoyu
 **/
class DataFetcherTest {

    private static final String stringProp = "stringProp";
    private static final int intProp = 1;
    private static final List<String> listProp = Arrays.asList("a", "b", "c");

    @Test
    void testFetch() {
        TestData data = new TestData();
        DataFetcher<TestData> fetcher = new DataFetcher<>(data);

        fetcher.addNode(() -> stringProp, TestData::setStringProp);
        fetcher.addNode(() -> intProp, TestData::setIntProp);
        fetcher.addNode(() -> listProp, TestData::setListProp);

        fetcher.fetch();

        assertEquals(stringProp, data.getStringProp());
        assertEquals(intProp, data.getIntProp());
        assertEquals(listProp, data.getListProp());
    }

    @Test
    @Timeout(6)
    void testFetchConcurrency() {
        TestData data = new TestData();
        DataFetcher<TestData> fetcher = new DataFetcher<>(data);

        fetcher.addNode(fetcher.new Node<String>(() -> {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return stringProp;
        }, TestData::setStringProp));

        fetcher.addNode(fetcher.new Node<Integer>(() -> {
            try {
                Thread.sleep(3000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return intProp;
        }, TestData::setIntProp));

        fetcher.addNode(fetcher.new Node<List<String>>(() -> {
            try {
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return listProp;
        }, TestData::setListProp));

        fetcher.fetch();

        assertEquals(stringProp, data.getStringProp());
        assertEquals(intProp, data.getIntProp());
        assertEquals(listProp, data.getListProp());
    }


    static class TestData {
        private String stringProp;

        private int intProp;

        private List<String> listProp;


        public String getStringProp() {
            return stringProp;
        }

        public void setStringProp(String stringProp) {
            this.stringProp = stringProp;
        }

        public int getIntProp() {
            return intProp;
        }

        public void setIntProp(int intProp) {
            this.intProp = intProp;
        }

        public List<String> getListProp() {
            return listProp;
        }

        public void setListProp(List<String> listProp) {
            this.listProp = listProp;
        }
    }
}