package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileOutputStream;
import java.io.File;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

/*
    Message types:
    JOIN-%remotePort%                                               - Message for joining the system
    ADD-%predecessor%-%successor%                                   - Update the new nodes predecessor/successor
    UPDATEP-%predecessor%                                           - Update a nodes predecessor information
    UPDATES-%successor%                                             - Update a nodes successor information
 */

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();

    static final int SERVER_PORT = 10000;
    static final String[] REMOTE_PORTS = new String[] {"11108", "11112", "11116", "11120", "11124"};
    static final String JOIN_PORT = "11108";
    static String myPort;

    // Variables for maintaining the CHORD Ring
    private static String nodeId = null;
    private static String preNode = null;
    private static String nextNode = null;

    // This is used only for avd 5554
    private static int nodeCount = 1;

    // uri of content provider
    private static Uri mUri;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String hashId = null;
        String preId = null;
        String nextId = null;

        try {
            hashId = genHash(String.valueOf(values.getAsString("key")));
            preId = genHash(String.valueOf(Integer.parseInt(preNode)/2));
            nextId = genHash(String.valueOf(Integer.parseInt(nextNode)/2));
        } catch (NoSuchAlgorithmException c) {
            c.printStackTrace();
        }

        Log.e("addNode", "Checking with current nodes hash: " + nodeId + " and key hash: " +  hashId);

        if(hashId.compareTo(nodeId) > 0)
            Log.e("Insert Comparision", "hashId > currentId");
        if(hashId.compareTo(nodeId) < 0)
            Log.e("Insert Comparision", "hashId < currentId");
        if (hashId.compareTo(nextId) > 0)
            Log.e("Insert Comparision", "hashId > nextId");
        if (hashId.compareTo(nextId) < 0)
            Log.e("Insert Comparision", "hashId < nextId");

        // check if there is only one node in the system
        if(myPort == preNode && myPort == nextNode) {
            // send a message to the remotePort informing about successor and predecessor information
            return insertIntoProvider(uri, values);
        } else if(hashId.compareTo(nodeId) < 0) {
            // send an update message with info about successor/predecessor to new node
            if(hashId.compareTo(preId) > 0 || nodeId.compareTo(preId) < 0) {
                return insertIntoProvider(uri, values);
                //Log.e("insert", "Added new node. current node count is " + nodeCount);
                //Log.e("addNode", "currentNode: " + myPort + " predecessor: " + preNode + " successor: " + nextNode);
            } else {
                Log.e("insert", "Forwarding the key to its predecessor: " + preNode);
                String msg = "INSERTKEY-" + values.getAsString("key") + "-" + values.getAsString("value");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, preNode);
            }
        } else if(hashId.compareTo(nodeId) > 0){
            if(nodeId.compareTo(preId) < 0) {
                return insertIntoProvider(uri, values);
            } else {
                Log.e("insert", "Forwarding the key to its successor: " + nextNode);
                String msg = "INSERTKEY-" + values.getAsString("key") + "-" + values.getAsString("value");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, nextNode);
            }
        }
        return null;
    }

    public Uri insertIntoProvider(Uri uri, ContentValues values) {
        Log.e("insertIntoProvider", "Inserting key: " + values.getAsString("key") + " into the provider");
        String filename = values.getAsString("key");
        String message = values.getAsString("value");

        try {
            FileOutputStream outputStream = new FileOutputStream(new File(getContext().getFilesDir() + "/" + filename));
            outputStream.write(message.getBytes());
            outputStream.close();
        } catch (Exception e) {
            Log.e(TAG, "File write failed");
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        Log.e(TAG, "Starting a new node");
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        final String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        // When a new node is created we can assume that there is only one node in the system. So set Predecessor and
        // Successor ID's to itself
        // I am storing port numbers because conversion from portNumber -> id is eaiser than the other way round.
        preNode = myPort;
        nextNode = myPort;

        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

        try {
            nodeId = genHash(String.valueOf(Integer.parseInt(myPort)/2));
        } catch (NoSuchAlgorithmException c) {
            c.printStackTrace();
        }

        if(ringStart()) {
            Log.e("On Create", "**** Ring Start ****");
        }

        if(ringEnd()) {
            Log.e("On Create", "**** Ring End ****");
        }

        // start a server task which accepts new node joins (Only for avd 5554) and communicates with other avd's for Insert
        // and Query requests.
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT, 20);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        Log.e(TAG, "Server task created on node with ID " + nodeId);

        // start a client async task, which is used for a node to join the ring
        if(!myPort.equals(JOIN_PORT)) {
            String msg = "JOIN-" + myPort;
            String remotePort = "11108";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, remotePort);
        }
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    /*
        First handle a case in which there is only one node in the system
        This function checks if the new node will be a successor or a predecessor of the current node.
        If yes, then set this node as successor/predecessor and update the new node successor/predecessor information and
            then update the current node successor/predecessor with the new information
        If Not, then forward the message to successor/predecessor

        TODO: The nodes in the ring are not consistent when I start avd in order 0 -> 2 -> 4 -> 1 -> 3 and when I start
        TODO: avd 0 first and select all avds next
     */
    void addNode(String remotePort) {
        String remoteId = null;
        String preId = null;
        String nextId = null;

        try {
            remoteId = genHash(String.valueOf(Integer.parseInt(remotePort)/2));
            preId = genHash(String.valueOf(Integer.parseInt(preNode)/2));
            nextId = genHash(String.valueOf(Integer.parseInt(nextNode)/2));
        } catch (NoSuchAlgorithmException c) {
            c.printStackTrace();
        }

        Log.e("addNode", "Checking with currPort: " + myPort + " and remotePort: " +  remotePort);

        if(remoteId.compareTo(nodeId) > 0)
            Log.e("Comparision", "remoteId > currentId");
        if(remoteId.compareTo(nodeId) < 0)
            Log.e("Comparision", "remoteId < currentId");
        if (remoteId.compareTo(nextId) > 0)
            Log.e("Comparision", "remoteId > nextId");
        if (remoteId.compareTo(nextId) < 0)
            Log.e("Comparision", "remoteId < nextId");

        // check if there is only one node in the system
        if(myPort == preNode && myPort == nextNode) {
            // send a message to the remotePort informing about successor and predecessor information
            String msg = "ADD-" + myPort + "-" + myPort;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, remotePort);
            // update the current nodes successor/predecessor information
            preNode = remotePort;
            nextNode = remotePort;
            nodeCount++;

            Log.e("addNode", "Added new node. current node count is " + nodeCount);
            Log.e("addNode", "currentNode: " + myPort + " predecessor: " + preNode + " successor: " + nextNode);
        } else if(remoteId.compareTo(nodeId) > 0) {
            // send an update message with info about successor/predecessor to new node
            if(remoteId.compareTo(nextId) < 0 || nodeId.compareTo(nextId) > 0) {
                String msg = "ADD-" + myPort + "-" + nextNode;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, remotePort);
                // send an update message with info about predecessor to current node successor
                msg = "UPDATEP-" + remotePort;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, nextNode);
                // update current node successor
                nextNode = remotePort;
                nodeCount++;
                Log.e("addNode", "Added new node. current node count is " + nodeCount);
                Log.e("addNode", "currentNode: " + myPort + " predecessor: " + preNode + " successor: " + nextNode);
            } else {
                Log.e("addNode", "Forwarding new node join request to its successor: " + nextNode);
                String msg = "JOIN-" + remotePort;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, nextNode);
            }
        } else if(remoteId.compareTo(nodeId) < 0){
            if(remoteId.compareTo(preId) > 0 || nodeId.compareTo(preId) < 0) {
                String msg = "ADD-" + preNode + "-" + myPort;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, remotePort);
                // send an update message with info about predecessor to current node successor
                msg = "UPDATES-" + remotePort;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, preNode);
                // update current node successor
                preNode = remotePort;
                nodeCount++;
                Log.e("addNode", "Added new node. current node count is " + nodeCount);
                Log.e("addNode", "currentNode: " + myPort + " predecessor: " + preNode + " successor: " + nextNode);
            } else {
                Log.e("addNode", "Forwarding new node join request to its successor: " + preNode);
                String msg = "JOIN-" + remotePort;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, preNode);
            }
        }

    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.e(TAG, "Starting a server on this Node");
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            while (true) {
                try {
                    String line = null;

                    Socket client = serverSocket.accept();
                    BufferedReader input = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    while((line = input.readLine()) != null ) {
                        line.trim();
                        if(line.contains("JOIN")) {
                            String remotePort = line.split("-")[1];
                            String newNodeId = null;

                            // get the nodeID by using hash function
                            try {
                                newNodeId = genHash(String.valueOf(Integer.parseInt(remotePort)/2));
                            } catch (NoSuchAlgorithmException c) {
                                c.printStackTrace();
                            }
                            Log.e(TAG, "Received JOIN request from client " + remotePort + " With nodeId " + newNodeId);
                            addNode(remotePort);

                        }

                        if(line.contains("ADD")) {
                            /*
                                Message Format:
                                ADD-%predecessor%-%successor%       - Update the new nodes predecessor/successor
                             */
                            String[] msg = line.split("-");
                            preNode = msg[1];
                            nextNode = msg[2];
                            Log.e("ADD", "Hurray!!. I am added to the ring");
                            Log.e("ADD", "currentNode: " + myPort + " predecessor: " + preNode + " successor: " + nextNode);
                        }

                        if(line.contains("UPDATEP")) {
                            /*
                                Message Format:
                                UPDATEP-%predecessor%               - Update a nodes predecessor information
                             */
                            preNode = line.split("-")[1];
                            Log.e("UPDATEP", "New node added to the ring");
                            Log.e("ADD", "currentNode: " + myPort + " predecessor: " + preNode + " successor: " + nextNode);
                        }

                        if(line.contains("UPDATES")) {
                            /*
                                Message Format:
                                UPDATES-%successor%               - Update a nodes predecessor information
                             */
                            nextNode = line.split("-")[1];
                            Log.e("UPDATEP", "New node added to the ring");
                            Log.e("ADD", "currentNode: " + myPort + " predecessor: " + preNode + " successor: " + nextNode);
                        }

                        if(line.contains("INSERTKEY")) {
                            /*
                                Message Format:
                                INSERTKEY-%key%-%value%           - Inserts key into the provider
                             */
                            String[] msg = line.split("-");
                            ContentValues values = new ContentValues();
                            values.put(KEY_FIELD, msg[1]);
                            values.put(VALUE_FIELD, msg[2]);
                            insert(mUri, values);
                        }
                    }

                } catch (IOException e) {
                    Log.e(TAG, "Failed to accept new connection");
                    e.printStackTrace();
                    break;
                }

            }
            return null;
        }

        protected void onProgressUpdate(String... strings) {

        }

    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            //Log.e(TAG, "Sending join request to AVD-5554");


            try {
                // Connect to AVD-5554
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[1]));

                Log.e("ClientTask", "Sending Message: " + msgs[0] + " to remotePort " + msgs[1]);

                msgs[0].trim();

                // Used this piece of code from http://stackoverflow.com/questions/17440795/send-a-string-instead-of-byte-through-socket-in-java
                PrintWriter out = new PrintWriter(socket.getOutputStream());
                out.print(msgs[0]);
                out.flush();
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
                e.printStackTrace();
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                e.printStackTrace();
            }

            return null;
        }

    }

    private boolean ringStart() {
        boolean isStart = true;
        try {
            for(int i=0; i<REMOTE_PORTS.length; i++) {
                if(!myPort.equals(REMOTE_PORTS[i])) {
                    if(nodeId.compareTo(genHash(String.valueOf(Integer.parseInt(REMOTE_PORTS[i])/2))) > 0) {
                        isStart = false;
                    }
                }
            }
        } catch (NoSuchAlgorithmException c) {
            c.printStackTrace();
        }
        return  isStart;
    }

    private boolean ringEnd() {
        boolean isStart = true;
        try {
            for(int i=0; i<REMOTE_PORTS.length; i++) {
                if(!myPort.equals(REMOTE_PORTS[i])) {
                    if(nodeId.compareTo(genHash(String.valueOf(Integer.parseInt(REMOTE_PORTS[i])/2))) < 0) {
                        isStart = false;
                    }
                }
            }
        } catch (NoSuchAlgorithmException c) {
            c.printStackTrace();
        }
        return  isStart;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
}
