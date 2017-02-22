package qonduit.client.websocket;

import javax.websocket.CloseReason;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ClientHandler extends Endpoint {

    private final static Logger LOG = LoggerFactory.getLogger(ClientHandler.class);

    @Override
    public void onOpen(Session session, EndpointConfig config) {
        LOG.info("Websocket session {} opened.", session.getId());
        session.addMessageHandler(new MessageHandler.Whole<byte[]>() {

            @Override
            public void onMessage(byte[] message) {
                LOG.info("Message received on Websocket session {}: {}", session.getId(), message);
            }
        });
    }

    @Override
    public void onClose(Session session, CloseReason reason) {
        LOG.info("Websocket session {} closed.", session.getId());
    }

    @Override
    public void onError(Session session, Throwable error) {
        LOG.error("Error occurred on Websocket session" + session.getId(), error);
    }

}
