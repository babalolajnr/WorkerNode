package com.workernode;

public class NodeManager {
    private final String nodeName;
    private final String nodeIp;
    private final int nodePort;

    public NodeManager(String nodeName, String nodeIp, int nodePort) {
        this.nodeName = nodeName;
        this.nodeIp = nodeIp;
        this.nodePort = nodePort;
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getNodeIp() {
        return nodeIp;
    }

    public int getNodePort() {
        return nodePort;
    }

}
