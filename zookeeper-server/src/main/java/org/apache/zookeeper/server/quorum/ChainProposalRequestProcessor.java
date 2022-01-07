
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import chain.ZooChainFront;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor simply forwards requests to an AckRequestProcessor and
 * SyncRequestProcessor.
 */
public class ChainProposalRequestProcessor implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ChainProposalRequestProcessor.class);

    ChainZookeeperServer zks;
    RequestProcessor nextProcessor;

    public ChainProposalRequestProcessor(ChainZookeeperServer zks, RequestProcessor nextProcessor) {
        this.zks = zks;
        this.nextProcessor = nextProcessor;
    }

    public void processRequest(Request request) throws RequestProcessorException {
        if(isMine(request))
            nextProcessor.processRequest(request);

        if(needCommit(request))
            zks.sendToConsensus(request);

    }

    private boolean isMine(Request request) {
        return !(request.getOwner() instanceof ZooChainFront);
    }

    /** From {@link CommitProcessor#needCommit} */
    protected boolean needCommit(Request request) {
        if (request.isThrottled()) {
            return false;
        }
        switch (request.type) {
            case ZooDefs.OpCode.create:
            case ZooDefs.OpCode.create2:
            case ZooDefs.OpCode.createTTL:
            case ZooDefs.OpCode.createContainer:
            case ZooDefs.OpCode.delete:
            case ZooDefs.OpCode.deleteContainer:
            case ZooDefs.OpCode.setData:
            case ZooDefs.OpCode.reconfig:
            case ZooDefs.OpCode.multi:
            case ZooDefs.OpCode.setACL:
            case ZooDefs.OpCode.check:
                return true;
            case ZooDefs.OpCode.createSession:
            case ZooDefs.OpCode.closeSession:
                return !request.isLocalSession();
            default:
                return false;
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        nextProcessor.shutdown();
    }
}
