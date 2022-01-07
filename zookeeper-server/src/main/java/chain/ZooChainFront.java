package chain;

import chain.ipc.*;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.quorum.ChainZookeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;
import pt.unl.fct.di.novasys.babel.handlers.NotificationHandler;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public class ZooChainFront extends GenericProtocol {

    public final static short PROTOCOL_ID_BASE = 100;
    public final static String PROTOCOL_NAME_BASE = "ZooChainFront";
    public static final String ADDRESS_KEY = "frontend_address";
    public static final String PEER_PORT_KEY = "frontend_peer_port";
    private static final Logger logger = LoggerFactory.getLogger(ZooChainFront.class);
    protected final int PEER_PORT;
    protected final InetAddress self;
    //Forwarded
    private final ChainZookeeperServer zkServer;
    protected int peerChannel;
    protected List<InetAddress> membership;
    protected LinkedBlockingQueue<byte[]> queuedCommits = new LinkedBlockingQueue<byte[]>();
    private Host writesTo;
    private boolean writesToConnected;

    public ZooChainFront(Properties props, ChainZookeeperServer zkServer) throws IOException {
        super(PROTOCOL_NAME_BASE, PROTOCOL_ID_BASE);

        this.PEER_PORT = Integer.parseInt(props.getProperty(PEER_PORT_KEY));
        self = InetAddress.getByName(props.getProperty(ADDRESS_KEY));
        membership = null;

        this.zkServer = zkServer;

        writesTo = null;
        writesToConnected = false;
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {
        //Peer
        Properties peerProps = new Properties();
        peerProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty(ADDRESS_KEY));
        peerProps.setProperty(TCPChannel.PORT_KEY, Integer.toString(PEER_PORT));
        //peerProps.put(TCPChannel.DEBUG_INTERVAL_KEY, 10000);
        peerChannel = createChannel(TCPChannel.NAME, peerProps);
        registerMessageSerializer(peerChannel, PeerOpMessage.MSG_CODE, PeerOpMessage.serializer);
        registerMessageHandler(peerChannel, PeerOpMessage.MSG_CODE, this::onPeerOpMessage, this::uponMessageFailed);
        registerChannelEventHandler(peerChannel, InConnectionDown.EVENT_ID, this::onInConnectionDown);
        registerChannelEventHandler(peerChannel, InConnectionUp.EVENT_ID, this::onInConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionDown.EVENT_ID, this::onOutConnectionDown);
        registerChannelEventHandler(peerChannel, OutConnectionUp.EVENT_ID, this::onOutConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionFailed.EVENT_ID, this::onOutConnectionFailed);

        //Consensus
        subscribeNotification(MembershipChange.NOTIFICATION_ID, this::onMembershipChange);
        subscribeNotification(LeadNotification.NOTIFICATION_ID,
                (NotificationHandler<LeadNotification>) (protoNotification, i) -> zkServer.lead(protoNotification.getSeqN()));
        subscribeNotification(UnleadNotification.NOTIFICATION_ID,
                (NotificationHandler<UnleadNotification>) (protoNotification, i) -> zkServer.unlead());

        subscribeNotification(InstallSnapshotNotification.NOTIFICATION_ID, this::onInstallSnapshot);
        registerRequestHandler(GetSnapshotRequest.REQUEST_ID, this::onGetStateSnapshot);

        new Thread(() -> {
            logger.warn("Commit executor starting");
            try {
                while (true) {
                    byte[] take = queuedCommits.take();
                    zkServer.commitOperation(take);
                }
            } catch (Exception e) {
                logger.error("Severe unrecoverable error, from thread commit executor thread", e);
            }
        }).start();

        //ManagementFactory.getThreadMXBean().setThreadContentionMonitoringEnabled(true);
        //setupPeriodicTimer(new DebugTimer(), 7000, 5000);
        registerTimerHandler(DebugTimer.TIMER_ID, this::onDebug);

    }

    Map<Long, Long> lastCpus = new HashMap<>();
    private void onDebug(DebugTimer timer, long timerId) {
        ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
        long[] allThreadIds = tmx.getAllThreadIds();
        Map<Long, Long> curCpus = new HashMap<>();
        for (long thread : allThreadIds)
            curCpus.put(thread, tmx.getThreadCpuTime(thread)/1000000);

        StringBuilder sb = new StringBuilder();
        for (long thread : allThreadIds) {
            long cpuTime = curCpus.get(thread) - lastCpus.getOrDefault(thread, 0L);
            if(cpuTime > 2000) {
                sb.append("\n").append(String.format("%6d", cpuTime)).append(' ')
                        .append(tmx.getThreadInfo(thread).getThreadName());
            }
        }

        logger.warn(sb.toString());
        lastCpus.putAll(curCpus);
    }

    public void commit(byte[] op) {
        queuedCommits.add(op);
    }

    public void execRead(Request req){
        zkServer.execStrongRead(req);
    }

    public void submitRead(Request request) {
        sendRequest(new SubmitReadRequest(request), ZooChainProto.PROTOCOL_ID);
    }

    //Called by leader only!
    public void submitOperation(byte[] op) {
        if (membership.size() == 1) {
            zkServer.commitOperation(op);
        } else {
            sendRequest(new SubmitOpRequest(op), ZooChainProto.PROTOCOL_ID);
        }
    }

    //Called by followers only!
    public void sendOpToLeader(Request req) {
        if (writesToConnected)
            sendMessage(peerChannel, new PeerOpMessage(req), writesTo);
    }

    protected void onPeerOpMessage(PeerOpMessage msg, Host from, short sProto, int channel) {
        msg.getRequest().setOwner(this);
        zkServer.receiveOpFromFollower(msg.getRequest());
    }

    protected void onOutConnectionUp(OutConnectionUp event, int channel) {
        Host peer = event.getNode();
        if (peer.equals(writesTo)) {
            writesToConnected = true;
            logger.debug("Connected to writesTo " + event);
        } else {
            logger.warn("Unexpected connectionUp, ignoring and closing: " + event);
            closeConnection(peer, peerChannel);
        }
    }

    protected void onOutConnectionDown(OutConnectionDown event, int channel) {
        Host peer = event.getNode();
        if (peer.equals(writesTo)) {
            logger.warn("Lost connection to writesTo, re-connecting: " + event);
            writesToConnected = false;
            openConnection(writesTo, peerChannel);
        }
    }

    protected void onOutConnectionFailed(OutConnectionFailed<Void> event, int channel) {
        logger.info(event.toString());
        Host peer = event.getNode();
        if (peer.equals(writesTo)) {
            logger.warn("Connection failed to writesTo, re-trying: " + event);
            openConnection(writesTo, peerChannel);
        } else {
            logger.warn("Unexpected connectionFailed, ignoring: " + event);
        }
    }

    protected void onMembershipChange(MembershipChange notification, short emitterId) {
        //update membership and responder
        membership = notification.getOrderedMembers();

        //Writes to changed
        if (writesTo == null || !notification.getWritesTo().equals(writesTo.getAddress())) {
            //Close old writesTo
            if (writesTo != null && !writesTo.getAddress().equals(self)) {
                closeConnection(writesTo, peerChannel);
                writesToConnected = false;
            }
            //Update and open to new writesTo
            writesTo = new Host(notification.getWritesTo(), PEER_PORT);
            logger.info("New writesTo: " + writesTo.getAddress());
            if (!writesTo.getAddress().equals(self))
                openConnection(writesTo, peerChannel);
        }
    }

    public void uponMessageFailed(ProtoMessage msg, Host host, short i, Throwable throwable, int i1) {
        logger.warn("Failed: " + msg + ", to: " + host + ", reason: " + throwable.getMessage());
    }

    private void onInConnectionDown(InConnectionDown event, int channel) {
        logger.debug(event.toString());
    }

    private void onInConnectionUp(InConnectionUp event, int channel) {
        logger.debug(event.toString());
    }

    private void onInstallSnapshot(InstallSnapshotNotification not, short from) {
        throw new IllegalStateException();
    }

    public void onGetStateSnapshot(GetSnapshotRequest not, short from) {
        throw new IllegalStateException();
    }

    public static class DebugTimer extends ProtoTimer {

        public static final short TIMER_ID = 1000;

        public DebugTimer() {
            super(TIMER_ID);
        }

        @Override
        public ProtoTimer clone() {
            return null;
        }
    }

}
