package org.template.recommendation;

import org.apache.predictionio.data.storage.Storage;
import org.apache.predictionio.data.storage.elasticsearch.StorageClient;
import org.elasticsearch.client.transport.TransportClient;

public class PioTCManager implements ITCManager {
    private final TransportClient client;

    public PioTCManager() {
        if (Storage.getConfig("ELASTICSEARCH").nonEmpty())
            this.client = new StorageClient(Storage.getConfig("ELASTICSEARCH").get()).client();
        else
            throw new IllegalStateException(
                    "No Elasticsearch client configuration detected, check your pio-env.sh for " +
                            "proper configuration settings");
    }

    @Override
    public TransportClient get() {
        return this.client;
    }
}
