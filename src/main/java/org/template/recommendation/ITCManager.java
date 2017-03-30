package org.template.recommendation;

import org.elasticsearch.client.transport.TransportClient;

/**
 * Interface for retrieving transport clients
 */
public interface ITCManager {
    /**
     * Retrieves transport client
     * @return Transport Client
     */
    TransportClient get();
}
