package org.apache.zookeeper.server.quorum;

import chain.ZooChainDelayedProto;
import chain.ZooChainFront;
import chain.ZooChainProto;
import org.apache.jute.Record;
import org.apache.zookeeper.server.*;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.util.JvmPauseMonitor;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;
import org.apache.zookeeper.util.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.unl.fct.di.novasys.babel.core.Babel;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.network.NetworkManager;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ChainZookeeperServer extends ZooKeeperServer {

    private static final Logger LOG = LoggerFactory.getLogger(ChainZookeeperServer.class);
    protected CommitProcessor commitProcessor;
    protected SyncRequestProcessor syncProcessor;

    //Leader only
    RequestProcessor proposalProcessor;

    /*
     * Pending sync requests
     */ ConcurrentLinkedQueue<Request> pendingSyncs = new ConcurrentLinkedQueue<Request>();
    private ZooChainFront frontend;
    /**
     * Enable since request processor for writing txnlog to disk and
     * take periodic snapshot. Default is ON.
     */

    private boolean syncRequestProcessorEnabled = true;

    private boolean strongRead;

    public ChainZookeeperServer(JvmPauseMonitor jvmPauseMonitor, FileTxnSnapLog txnLogFactory, int tickTime,
                                int minSessionTimeout, int maxSessionTimeout, int clientPortListenBacklog,
                                ZKDatabase zkDb, String initialConfig) {
        super(jvmPauseMonitor, txnLogFactory, tickTime, minSessionTimeout, maxSessionTimeout,
                clientPortListenBacklog, zkDb, initialConfig);
        LOG.warn("Starting chain zoo server!");
        try {
            Properties configuration = new Properties();
            configuration.load(new FileInputStream("conf/babel.conf"));
            if (configuration.containsKey("interface")) {
                String address = getAddress(configuration.getProperty("interface"));
                if (address == null) return;
                configuration.setProperty(ZooChainFront.ADDRESS_KEY, address);
                configuration.setProperty(ZooChainProto.ADDRESS_KEY, address);
            }
            strongRead = Boolean.parseBoolean(configuration.getProperty("strongRead"));
            Babel babel = Babel.getInstance();
            frontend = new ZooChainFront(configuration, this);
            GenericProtocol proto;
            if (strongRead)
                proto = new ZooChainDelayedProto(configuration, NetworkManager.createNewWorkerGroup(), frontend);
            else
                proto = new ZooChainProto(configuration, NetworkManager.createNewWorkerGroup(), frontend);
            babel.registerProtocol(frontend);
            babel.registerProtocol(proto);
            frontend.init(configuration);
            proto.init(configuration);
            babel.start();
        } catch (Exception e) {
            LOG.error("Error creating babel protocols", e);
            System.exit(1);
        }

    }

    private static String getAddress(String inter) throws SocketException {
        NetworkInterface byName = NetworkInterface.getByName(inter);
        if (byName == null) {
            LOG.error("No interface named " + inter);
            return null;
        }
        Enumeration<InetAddress> addresses = byName.getInetAddresses();
        InetAddress currentAddress;
        while (addresses.hasMoreElements()) {
            currentAddress = addresses.nextElement();
            if (currentAddress instanceof Inet4Address)
                return currentAddress.getHostAddress();
        }
        LOG.error("No ipv4 found for interface " + inter);
        return null;
    }


    /**
     * Unlike a Follower, which sees a full request only during the PROPOSAL
     * phase, Observers get all the data required with the INFORM packet.
     * This method commits a request that has been unpacked by from an INFORM
     * received from the Leader.
     *
     * @param request
     */
    public void commitRequest(Request request) {
        if (syncRequestProcessorEnabled) {
            // Write to txnlog and take periodic snapshot
            syncProcessor.processRequest(request);
        }
        commitProcessor.commit(request);
    }

    @Override
    protected void setupRequestProcessors() {
        if (syncRequestProcessorEnabled) {
            syncProcessor = new SyncRequestProcessor(this, null);
            syncProcessor.start();
        }

        RequestProcessor finalProcessor = new FinalRequestProcessor(this);
        commitProcessor = new CommitProcessor(finalProcessor, Long.toString(getServerId()), true, getZooKeeperServerListener());
        commitProcessor.start();

        setupRequestProcessorsFollower();
    }

    private void setupRequestProcessorsLeader() {
        LOG.warn("Setting up leader request processors");
        if (strongRead)
            proposalProcessor = new ChainStrongProposalRequestProcessor(this, commitProcessor);
        else
            proposalProcessor = new ChainProposalRequestProcessor(this, commitProcessor);

        firstProcessor = new PrepRequestProcessor(this, proposalProcessor);
        ((PrepRequestProcessor) firstProcessor).start();
    }

    private void setupRequestProcessorsFollower() {
        LOG.warn("Setting up follower request processors");
        if (strongRead) {
            firstProcessor = new ChainStrongFollowerRequestProcessor(this, commitProcessor);
            ((ChainStrongFollowerRequestProcessor) firstProcessor).start();
        } else {
            firstProcessor = new ChainFollowerRequestProcessor(this, commitProcessor);
            ((ChainFollowerRequestProcessor) firstProcessor).start();
        }
    }

    /*
     * Process a sync request
     */
    public synchronized void sync() {
        if (pendingSyncs.size() == 0) {
            LOG.warn("Not expecting a sync.");
            return;
        }

        Request r = pendingSyncs.remove();
        commitProcessor.commit(r);
    }

    @Override
    public String getState() {
        return "chainObserver";
    }

    @Override
    public synchronized void shutdown() {
        if (!canShutdown()) {
            LOG.debug("ZooKeeper server is not running, so not proceeding to shutdown!");
            return;
        }
        super.shutdown();
        if (syncRequestProcessorEnabled && syncProcessor != null) {
            syncProcessor.shutdown();
        }
    }

    /**
     * From {@link LearnerHandler#run}
     */
    public void receiveOpFromFollower(Request req) {
        //TODO if leader
        ((PrepRequestProcessor) firstProcessor).processRequest(req);
    }


    public void strongRead(Request request) {
        frontend.submitRead(request);
    }

    public void execStrongRead(Request request) {
        commitProcessor.processRequest(request);
    }

    /**
     * Follower only
     * From {@link Learner#request}
     */
    public void sendToLeader(Request request) {
        //System.out.println("Requesting " + request + " to leader");
        if (request.isThrottled()) {
            LOG.error("Throttled request sent to leader: {}. Exiting", request);
            ServiceUtils.requestSystemExit(ExitCode.UNEXPECTED_ERROR.getValue());
        }
        frontend.sendOpToLeader(request);
    }

    /**
     * Leader only
     * From {@link Leader#propose}
     */
    public void sendToConsensus(Request request) {
        /*if(request.type != ZooDefs.OpCode.setData && request.type != ZooDefs.OpCode.createSession
                && request.type != ZooDefs.OpCode.create){
            LOG.warn("Proposing " + request + " to consensus");
        }*/

        if (request.isThrottled()) {
            LOG.error("Throttled request sent to leader: {}. Exiting", request);
            ServiceUtils.requestSystemExit(ExitCode.UNEXPECTED_ERROR.getValue());
        }
        byte[] bytes = SerializeUtils.serializeRequest(request);
        frontend.submitOperation(bytes);
    }

    /**
     * From {@link Observer#processPacket}
     */
    public void commitOperation(byte[] op) {
        try {
            TxnLogEntry logEntry = SerializeUtils.deserializeTxn(op);
            TxnHeader hdr = logEntry.getHeader();
            Record txn = logEntry.getTxn();
            TxnDigest digest = logEntry.getDigest();
            Request request = new Request(hdr.getClientId(), hdr.getCxid(), hdr.getType(), hdr, txn, 0);
            request.logLatency(ServerMetrics.getMetrics().COMMIT_PROPAGATION_LATENCY);
            request.setTxnDigest(digest);
            //System.out.println("Committing: " + request);
            commitRequest(request);

        } catch (IOException e) {
            LOG.error("Error reading received op", e);
        }
    }

    public void lead(int seqN) {
        //TODO should cleanup follower request processors. Not very important since we are not testing membership changes
        setZxid(ZxidUtils.makeZxid(seqN, 0));
        setupRequestProcessorsLeader();
    }

    public void unlead() {
        //TODO should cleanup leader request processors. Not very important since we are not testing membership changes
        setupRequestProcessorsFollower();
    }

}
