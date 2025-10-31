package org.com.mk.fx.qa.load.fix.clients.dsp;

import com.qa.quick.fix.core.listeners.QFInboundMessageListener;
import com.qa.quick.fix.core.listeners.QFOutboundMessageListener;
import com.qa.quick.fix.core.listeners.QFSessionEventListener;
import com.qa.quick.fix.core.pool.QFClientPoolManager;
import lombok.extern.slf4j.Slf4j;
import quickfix.Message;
import quickfix.SessionID;

import java.util.Set;

@Slf4j
public class DSPFixClient implements AutoCloseable {

    private final QFClientPoolManager clientPoolManager;

    public DSPFixClient(String clientConfiguration, String portsConfiguration, String environment, Set<String> clients) {
        try {
            this.clientPoolManager = new QFClientPoolManager("client-configs/dsp/demo-client-cfg.json"
                    , "ports.json", "TEST", Set.of("trap_client"));
            this.clientPoolManager.startAll();
            clientPoolManager.setGlobalMessageListener((sessionId, message) -> log.info("Received message: {} for session: {}", message, sessionId));
            clientPoolManager.setGlobalOutboundMessageListener((sessionId, message) -> log.info("Sent message: {} for session: {}", message, sessionId));
            clientPoolManager.setGlobalSessionEventListener(new QFSessionEventListener() {
                @Override
                public void onLogon(SessionID sessionId) {
                    log.info("Logged on to session: {}", sessionId);
                }

                @Override
                public void onLogout(SessionID sessionId) {
                    log.info("Logged out of session: {}", sessionId);
                }

                @Override
                public void onReject(SessionID sessionId, String reason) {
                    log.info("Session {} rejected: {}", sessionId, reason);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize DSPFixClient", e);
        }

    }





    @Override
    public void close() throws Exception {
        clientPoolManager.stopAll();
    }
}
