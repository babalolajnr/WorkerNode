package com.workernode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * General worker node program
 */
public class Main {

    private static final ExecutorService executors = Executors.newFixedThreadPool(1000);
    private static ConcurrentHashMap<Integer, AsyncServerSocket> currentConnections = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        if (Objects.isNull(currentConnections)) {
            currentConnections = new ConcurrentHashMap<>();
        }
        Scanner input = new Scanner(System.in);

        System.out.println("Enter number of nodes to setup");
        int node_number = Integer.parseInt(input.nextLine());

        List<NodeManager> nodeList = new ArrayList<>();
        int hostCount = 1;
        if (node_number != 0) {
            for (int i = 0; i < node_number; i++) {

                System.out.println("Host Name " + hostCount + ":");
                String nodeName = input.nextLine();

                System.out.println("Host IP " + hostCount + ":");
                String nodeIP = input.nextLine();

                System.out.println("Host Port " + hostCount + ":");
                int nodePort = Integer.parseInt(input.nextLine());

                NodeManager node = new NodeManager(nodeName, nodeIP, nodePort);
                nodeList.add(node);
                hostCount++;

            }
        }

        //starting each registered node
        for (NodeManager node : nodeList) {
            if (!currentConnections.containsKey(node.getNodePort())) {
                System.out.println("Starting " + node.getNodeName() + " on " + node.getNodeIp() + ":" + node.getNodePort());
                executors.submit(() -> {
                    currentConnections.put(node.getNodePort(), new AsyncServerSocket(node));
                });

            }
        }

    }
}