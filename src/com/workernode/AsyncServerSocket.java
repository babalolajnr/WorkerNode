package com.workernode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AsyncServerSocket {
    private final NodeManager node;

    private AsynchronousServerSocketChannel server;


    private final ExecutorService executors = Executors.newFixedThreadPool(50);

    private final List<AsynchronousSocketChannel> connectedClients = new ArrayList<>();


    public AsyncServerSocket(NodeManager node) {
        this.node = node;
        run();
    }


    public void run() {
        try {
            server = AsynchronousServerSocketChannel.open();

            server.bind(new InetSocketAddress(this.node.getNodeIp(), this.node.getNodePort()));

            server.accept(new ConnectionData(server), new ConnectionHandler());

            System.out.println("Node " + this.node.getNodeName() +
                    "Listening for connections on port " + this.node.getNodePort()
            );
        } catch (IOException e) {
            System.out.println(e.getLocalizedMessage());
        }
    }

    static class ConnectionData {
        AsynchronousServerSocketChannel server;

        public ConnectionData(AsynchronousServerSocketChannel server) {
            this.server = server;
        }
    }

    class ConnectionHandler implements CompletionHandler<AsynchronousSocketChannel, ConnectionData> {
        @Override
        public void completed(AsynchronousSocketChannel client, ConnectionData connectionData) {
            try {
                /*
                  new connection, so re-attach to accept another one
                 */
                connectionData.server.accept(new ConnectionData(server), this);
                SocketAddress clientAddress = client.getRemoteAddress();
                notifyConnect(client);
                System.out.println("Accepted a connection from " + clientAddress);
                /*
                  Handle data reading and writing for accepted connection
                 */
                setupFirstTwoBytesReading(client);

            } catch (IOException e) {
                System.out.println("Could not process client connection" + e);
            }
        }

        @Override
        public void failed(Throwable exc, ConnectionData attachment) {
            System.out.println("Could not connect successfully to client " + exc);
        }
    }

    private void setupFirstTwoBytesReading(AsynchronousSocketChannel client) {
        ReadData lenData = new ReadData();
        lenData.client = client;
        lenData.buffer = ByteBuffer.allocate(2);
        client.read(lenData.buffer, lenData, new FirstTwoBytesReader());
    }


    static class ReadData {
        AsynchronousSocketChannel client;
        ByteBuffer buffer;
    }

    class FirstTwoBytesReader implements CompletionHandler<Integer, ReadData> {

        @Override
        public void failed(Throwable exc, ReadData firstTwoBytesData) {
            notifyDisconnect(firstTwoBytesData.client);
//            System.out.println("could not read first two bytes"+exc);
        }

        @Override
        public void completed(Integer result, ReadData readLenData) {
            if (result == -1) {
                notifyDisconnect(readLenData.client);
                return;
            }
            readLenData.buffer.flip();
            ReadData actualData = new ReadData();
            actualData.client = readLenData.client;
            short len = readLenData.buffer.getShort();
            actualData.buffer = ByteBuffer.allocate(len);
            System.out.println("Length bytes (2) read. expecting {} bytes" + len);
            executors.submit(() -> actualData.client.read(actualData.buffer, actualData, new RemainingBytesHandler()));
        }
    }


    class RemainingBytesHandler implements CompletionHandler<Integer, ReadData> {

        @Override
        public void failed(Throwable exc, ReadData remainingBytesData) {
            notifyDisconnect(remainingBytesData.client);
        }

        @Override
        public void completed(Integer result, ReadData remainingBytesData) {
            if (result == -1) {
                notifyDisconnect(remainingBytesData.client);
                return;
            }

            setupFirstTwoBytesReading(remainingBytesData.client);
            remainingBytesData.buffer.flip();

            System.out.println("Remaining " + new String(remainingBytesData.buffer.array()));

            handleRequest(remainingBytesData.buffer.array(), remainingBytesData);
        }
    }

    private void handleRequest(byte[] request, ReadData readData) {
        byte[] response;
        byte[] fullResponse;

        try {
            String serverResponse = new String(request) + " from " + node.getNodeName();
            response = serverResponse.getBytes();

            fullResponse = prependLenBytes(response); //add length to message
            ByteBuffer responseBuffer = ByteBuffer.wrap(fullResponse);

            synchronized (readData.client) {
                readData.client.write(responseBuffer);
            }


            System.out.println("response " + serverResponse);


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static byte[] prependLenBytes(byte[] data) {
        short len = (short) data.length;
        byte[] newBytes = new byte[len + 2];
        newBytes[0] = (byte) (len / 256);
        newBytes[1] = (byte) (len & 255);
        System.arraycopy(data, 0, newBytes, 2, len);
        return newBytes;
    }


    private void notifyConnect(AsynchronousSocketChannel socketChannel) throws IOException {
        if (socketChannel == null) {
            throw new IOException("Socket channel is null");
        }
        connectedClients.add(socketChannel);
    }

    private void notifyDisconnect(AsynchronousSocketChannel socketChannel) {
        if (socketChannel == null) {
            System.out.println("Socket channel is null");
        }

        int connectionSize = connectedClients.size();
        for (int i = 0; i < connectionSize; i++) {
            if (connectedClients.get(i).equals(socketChannel)) {
                connectedClients.remove(socketChannel);
                System.out.println("Removed socket channel");
                break;
            }
        }
    }


}
