package qonduit.client.websocket;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.websocket.ClientEndpointConfig;
import javax.websocket.Decoder;
import javax.websocket.Encoder;
import javax.websocket.Extension;

import org.apache.http.cookie.Cookie;

public class QEndpointConfig implements ClientEndpointConfig {

    private String sessionCookie = null;

    private static final String FORMAT = "%s=%s;";

    public QEndpointConfig(Cookie sessionCookie) {
        if (null != sessionCookie) {
            this.sessionCookie = String.format(FORMAT, sessionCookie.getName(), sessionCookie.getValue());
        }
    }

    @Override
    public List<Class<? extends Encoder>> getEncoders() {
        return Collections.emptyList();
    }

    @Override
    public List<Class<? extends Decoder>> getDecoders() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, Object> getUserProperties() {
        return Collections.emptyMap();
    }

    @Override
    public List<String> getPreferredSubprotocols() {
        return Collections.emptyList();
    }

    @Override
    public List<Extension> getExtensions() {
        return Collections.emptyList();
    }

    @Override
    public Configurator getConfigurator() {
        return new Configurator() {

            @Override
            public void beforeRequest(Map<String, List<String>> headers) {
                super.beforeRequest(headers);
                if (null != sessionCookie) {
                    headers.put("Cookie", Collections.singletonList(sessionCookie));
                }
            }
        };
    }

}
