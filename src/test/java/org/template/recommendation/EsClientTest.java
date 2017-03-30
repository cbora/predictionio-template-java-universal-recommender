package org.template.recommendation;

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.github.tlrx.elasticsearch.test.annotations.*;
import com.github.tlrx.elasticsearch.test.support.junit.runners.ElasticsearchRunner;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.node.Node;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.github.tlrx.elasticsearch.test.EsSetup.createIndex;
import static com.github.tlrx.elasticsearch.test.EsSetup.deleteAll;
import static org.junit.Assert.*;

public class EsClientTest {
    
    @Ignore
    public void deleteIndexTest() throws Exception {
    }

    @Ignore
    public void createIndexTest() throws Exception {

    }

    @Ignore
    public void refreshIndexTest() throws Exception {

    }

    @Ignore
    public void hotSwapTest() throws Exception {

    }

    @Ignore
    public void searchTest() throws Exception {

    }

    @Ignore
    public void getSourceTest() throws Exception {

    }

    @Ignore
    public void getIndexNameTest() throws Exception {

    }

    @Ignore
    public void getRDDTest() throws Exception {

    }

    private class MockTCManager implements ITCManager {
        private TransportClient tcClient;

        public MockTCManager(TransportClient tcClient) {
            this.tcClient = tcClient;
        }

        public TransportClient get() {
            return this.tcClient;
        }
    }

}