package edu.buffalo.cse.cse486586.groupmessenger2;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.PriorityBlockingQueue;
import java.io.Serializable;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

import android.app.Activity;
import android.os.AsyncTask;
import android.content.Context;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;
import android.widget.EditText;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */

/* Data structures */

enum state {
    DELIVERED, UNDELIVERED
}

class clientList {
    private int cl_id;
    private String cl_port;
    private Socket cl_socket;
    private ObjectOutputStream cl_ostream;
    private ObjectInputStream cl_istream;

    public clientList(int id, String port, Socket sk) {
        this.cl_id  = id;
        this.cl_port = port;
        this.cl_socket = sk;
        this.cl_istream = null;
        this.cl_ostream = null;
    }

    public int getCl_id() {
        return this.cl_id;
    }

    public String getClientPort() {
        return this.cl_port;
    }

    public Socket getClientSocket() {
        return this.cl_socket;
    }

    public ObjectOutputStream getOutputStream() {
        return this.cl_ostream;
    }

    public ObjectInputStream getInputStream() {
        return this.cl_istream;
    }

    public void setSocket(Socket sk) {
        this.cl_socket = sk;
    }

    public void setOutputStream(ObjectOutputStream ostream) {
        this.cl_ostream = ostream;
    }

    public void setInputStream(ObjectInputStream istream) {
        this.cl_istream = istream;
    }
}

class queueNode {
    // <msg, initalSeqNo, messagedClientId, proposedSeqNo, proposedClientId, state(undelivered)>
    private String message;
    private int messagedClientId;
    private int proposedClientId;
    private int initialSeqNo;
    private int proposedSeqNo;
    private state messageState;

    public queueNode(String msg, int mCId, int pCId, int iSeqNo, int pSeqNo) {
        this.message = msg;
        this.messagedClientId = mCId;
        this.proposedClientId = pCId;
        this.initialSeqNo = iSeqNo;
        this.proposedSeqNo = pSeqNo;
        this.messageState = state.UNDELIVERED;
    }

    public String getMessage() {
        return this.message;
    }

    public int getMessagedClientId() {
        return this.messagedClientId;
    }

    public int getProposedClientId() {
        return this.proposedClientId;
    }

    public int getInitialSeqNo() {
        return this.initialSeqNo;
    }

    public int getProposedSeqNo() {
        return this.proposedSeqNo;
    }

    public state getMessageState() {
        return this.messageState;
    }

    public void setProposedClientId(int pCid) {
        this.proposedClientId = pCid;
    }

    public void setProposedSeqNo(int pSeqNo) {
        this.proposedSeqNo = pSeqNo;
    }

    public void setMessageState(state s) {
        this.messageState = s;
    }
}

class initialMessage implements Serializable{
    private String message;
    private int initialSeqNo;
    private int messagedClientId;

    public initialMessage(String msg, int seqNo, int clientId) {
        this.message = msg;
        this.initialSeqNo = seqNo;
        this.messagedClientId = clientId;
    }

    public String getMessage() {
        return this.message;
    }

    public int getInitialSeqNo() {
        return this.initialSeqNo;
    }

    public int getMessagedClientId() {
        return this.messagedClientId;
    }

}

class proposedMessage implements Serializable {
    private int initialSeqNo;
    private int proposedSeqNo;
    private int senderId;

    public proposedMessage(int iSeqNo, int pSeqNo, int sId) {
        this.initialSeqNo = iSeqNo;
        this.proposedSeqNo = pSeqNo;
        this.senderId = sId;
    }

    public int getInitialSeqNo() {
        return this.initialSeqNo;
    }

    public int getProposedSeqNo() {
        return this.proposedSeqNo;
    }

    public int getSenderId() {
        return this.senderId;
    }
}

class finalMessage implements Serializable {
    private int initialSeqNo;
    private int messagedClientId;
    private int finalSeqNo;
    private int proposedClientId;

    public finalMessage(int iSeqNo, int mCid, int fSeqNo, int pCid) {
        this.initialSeqNo = iSeqNo;
        this.messagedClientId = mCid;
        this.finalSeqNo = fSeqNo;
        this.proposedClientId = pCid;
    }

    public int getInitialSeqNo() {
        return this.initialSeqNo;
    }

    public int getMessagedClientId() {
        return this.messagedClientId;
    }

    public int getFinalSeqNo() {
        return this.finalSeqNo;
    }

    public int getProposedClientId() {
        return this.proposedClientId;
    }

}


public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();

    static final int SERVER_PORT = 10000;
    static final String[] REMOTE_PORTS = new String[] {"11108", "11112", "11116", "11120", "11124"};
    static String myPort;

    private static clientList[] cl;
    private static int clientCount = 5;

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private static Uri mUri;

    private static int nextSeqNo = 0;
    private static int nextProposedSeqNo = 0;
    private static int deliveredSeqNo = 0;

private static PriorityBlockingQueue<queueNode> holdBackQueue = new PriorityBlockingQueue<queueNode>(10, new Comparator<queueNode>() {
    @Override
    public int compare(queueNode lhs, queueNode rhs) {
        return Integer.compare(lhs.getProposedSeqNo(), rhs.getProposedSeqNo());
    }
});
    private static HashMap<Integer, ArrayList<proposedMessage>> proposedValues = new HashMap<Integer, ArrayList<proposedMessage>>();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        final String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        cl = new clientList[clientCount];
        for(int i=0; i<clientCount; i++) {
            cl[i] = new clientList(getClientId(REMOTE_PORTS[i]), REMOTE_PORTS[i], null);
        }


        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT, 20);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        findViewById(R.id.button4).setOnClickListener(
                new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        String msg = ((EditText) findViewById(R.id.editText1)).getText().toString();
                        ((EditText) findViewById(R.id.editText1)).setText("");
                        nextSeqNo++;
                       // proposedValues.put(nextSeqNo, new ArrayList<proposedMessage>());
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
                    }
                });
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            while(true) {
                try {
                    Socket client = serverSocket.accept();


                    //publishProgress(new DataInputStream(client.getInputStream()).readUTF());
                    //client.close();

                    /*
                    Terminology:
                        msg - is the actual message to be sent
                        initalSeqNo - First Sequence number proposed by client sending the message
                        messagedClientId - Unique identifier for the client sending the message
                        proposedSeqNo - Sequence number proposed by a client
                        proposedClientId - Unique identifier of the client which proposed this client id
                        finalSeqNo - Sequence number agreed upon by all clients

                        2. Accept the connection and Start a Asynctask to handle ordering messages from a client.
                                [Below operations will be done in the new Asynctask]
                        4. On B-Deliver <msg, initalSeqNo, messagedClientId> propose an proposedid for this message using queue length. (here clientid is the client which sent this message)
                        5. Send a message <initalSeqNo, proposedSeqNo> on this socket
                        6. Add <msg, initalSeqNo, messagedClientId, proposedSeqNo, proposedClientId, state(undelivered)> into priority queue.(On what basis will you store the elements in the priority queue)
                        9. On B-Deliver <initalSeqNo, messagedClientId, finalSeqNo, proposedClientId> modify the message with <msgid, clientid> in the queue
                            -   change the proposed sequence number to the finalid
                            -   change the clientid which proposed it
                            -   change the message from undelivered to delivered
                        10. Rearrange the queue based on the sequence number
                        11. If there are any messages at the head of the queue that has status delivered then print it and remove the messages from queue.
                     */

                    // Read the message from socket
                    ObjectInputStream inputStream = new ObjectInputStream(client.getInputStream());
                    ObjectOutputStream outputStream = new ObjectOutputStream(client.getOutputStream());
                    initialMessage iMessage = (initialMessage) inputStream.readObject();
                    //System.out.println("New Message from client " + iMessage.message + " " + Integer.toString(iMessage.initialSeqNo) + " " + Integer.toString(iMessage.messagedClientId));
                    // Generate a proposed Sequence number and send Message as described on step 5.
                    // Question: Is nextProposedSeqNo accessed by many threads? If so then we need synchronization
                    Log.e("avd"+getClientId(myPort), "Recieved B-M from sender["+ iMessage.getMessagedClientId() + "] with message [ID:" + iMessage.getInitialSeqNo() + " Message:" + iMessage.getMessage() + "]");
                    nextProposedSeqNo++;
                    proposedMessage pMessage = new proposedMessage(iMessage.getInitialSeqNo(), nextProposedSeqNo, getClientId(myPort));
                    outputStream.writeObject(pMessage);
                    // Add the queueNode into priority queue
                    // TODO: Need to order the messages in the priority queue based on proposed sequence number. Smallest at the head
                    queueNode msgNode = new queueNode(iMessage.getMessage(), iMessage.getMessagedClientId(), getClientId(myPort), pMessage.getInitialSeqNo(), pMessage.getProposedSeqNo());
                    holdBackQueue.offer(msgNode);

                    finalMessage fMessage = (finalMessage) inputStream.readObject();
                    Log.e("avd"+getClientId(myPort), "Recieved final sequence number"); // add complete message for debugging

                    queueNode qNode = null;
                    Iterator itr = holdBackQueue.iterator();
                    while(itr.hasNext()) {
                        qNode = (queueNode) itr.next();
                        if(qNode.getInitialSeqNo() == fMessage.getInitialSeqNo() && qNode.getMessagedClientId() == fMessage.getMessagedClientId()) {
                            break;
                        }
                    }
                    holdBackQueue.remove(qNode);
                    qNode.setProposedClientId(fMessage.getProposedClientId());
                    qNode.setProposedSeqNo(fMessage.getFinalSeqNo());
                    qNode.setMessageState(state.DELIVERED);
                    holdBackQueue.offer(qNode);
                    while(true) {
                        qNode = holdBackQueue.peek();
                        if(qNode != null && qNode.getMessageState() == state.DELIVERED) {
                            publishProgress(holdBackQueue.poll().getMessage());
                        } else {
                            break;
                        }
                    }

                } catch (IOException e) {
                    Log.e(TAG, "Failed to accept new connection");
                    break;
                } catch (ClassNotFoundException c) {
                    c.printStackTrace();
                }

            }

            return null;
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * Process the multicast message and then add the message with key as sequenceNumber into
             * ContentProvider and Increment the sequence number.
             */
            String strReceived = strings[0].trim();
            TextView tv = (TextView) findViewById(R.id.textView1);
            tv.append(strReceived + "\t\n");
            ContentValues cv = new ContentValues();
            cv.put(KEY_FIELD, Integer.toString(deliveredSeqNo));
            cv.put(VALUE_FIELD, strReceived);
            getContentResolver().insert(mUri, cv);
            deliveredSeqNo++;
            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            /*
             *  Multicast the message to all avd's
             */
            try {
                //For every message add a messageId into hashmap so that it can hold proposed values by all clients
                proposedValues.put(nextSeqNo, new ArrayList<proposedMessage>());

                for(int i=0; i<REMOTE_PORTS.length; i++) {
                    String message = msgs[0];
                    String remotePort = REMOTE_PORTS[i];
                    int messagedClientId = getClientId(msgs[1]);


                    Socket socket = null;
                    ObjectOutputStream outputStream = null;
                    ObjectInputStream inputStream = null;
                    if(cl[i].getClientSocket() == null) {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));
                        cl[getClientId(remotePort) - 1].setSocket(socket);
                        Log.e(TAG, "Creating a new socket" + cl[getClientId(remotePort) - 1].getClientSocket().toString());
                        outputStream = new ObjectOutputStream(socket.getOutputStream());
                        inputStream = new ObjectInputStream(socket.getInputStream());
                        cl[getClientId(remotePort) - 1].setOutputStream(outputStream);
                        cl[getClientId(remotePort) - 1].setInputStream(inputStream);

                    } else {
                        socket = cl[getClientId(remotePort) - 1].getClientSocket();
                        outputStream = cl[getClientId(remotePort) - 1].getOutputStream();
                        inputStream = cl[getClientId(remotePort)-1].getInputStream();
                        Log.e(TAG, "Using an existing socket" + socket.toString());
                    }
                    //String msgToSend = msgs[0];
                    //DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    //out.writeUTF(msgToSend);
                    //socket.close();

                    /*
                        Terminology:
                        msg - is the actual message to be sent
                        initalSeqNo - First Sequence number proposed by client sending the message
                        messagedClientId - Unique identifier for the client sending the message
                        proposedSeqNo - Sequence number proposed by a client
                        proposedClientId - Unique identifier of the client which proposed this client id
                        finalSeqNo - Sequence number agreed upon by all clients



                        1. First create a socket and connect a client
                        3. First send a B-Multicast message B-M < msg, initalSeqNo, messagedClientId>
                        7. On B-Deliver <initalSeqNo, proposedSeqNo> add it to a list of proposed sequence numbers for message msgid
                            At any point if we recieve messages from all clients then pick the highest number.
                            choose the smallest proposed client id which proposes this id incase of conflict
                        8. B-Multicast message <initalSeqNo, messagedClientId, finalSeqNo, proposedClientId>
                     */

                    // Sending a message < msg, initalSeqNo, messagedClientId> to client

                    initialMessage iMessage = new initialMessage(message, nextSeqNo, messagedClientId);
                    outputStream.writeObject(iMessage);



                    // TODO: From step 7
                    proposedMessage pMessage = (proposedMessage) inputStream.readObject();
                    Log.e("avd" + getClientId(myPort), "Received proposed message number from client: " + getClientId(remotePort) + " With proposed Seqno: " + pMessage.getProposedSeqNo());
                    //add this to proposed list
                    proposedValues.get(pMessage.getInitialSeqNo()).add(pMessage);
                    //outputStream.close();
                    //inputStream.close();
                    if(i == 4) {
                        // get the highest proposed sequence number and multicast it to every client
                        ArrayList<proposedMessage> pMsgList = proposedValues.get(pMessage.getInitialSeqNo());
                        int maxProp = Integer.MAX_VALUE, maxSender = Integer.MAX_VALUE;
                        for(int j=0; j<pMsgList.size(); j++) {
                            proposedMessage pMsg = pMsgList.get(j);
                            if(pMsg.getProposedSeqNo() < maxProp) {
                                maxProp = pMsg.getProposedSeqNo();
                                maxSender = pMsg.getSenderId();
                            }
                        }
                        // Now I got a final sequence number. Multicast it to all clients
                        finalMessage fMessage = new finalMessage(pMessage.getInitialSeqNo(), messagedClientId, maxProp, maxSender);
                        for(int j=0; j<cl.length; j++) {
                            if(!cl[j].getClientSocket().isConnected())
                                Log.e(TAG, "Socket is closed" + cl[j].getClientSocket().toString());
                            else {
                                Log.e(TAG, "Socket is connected" + cl[j].getClientSocket().toString());
                            }

                                    cl[i].getOutputStream().writeObject(fMessage);
                            //ostream.writeObject(fMessage);
                        }
                    }
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private int getClientId(String port) {
        int result;
        for(int i=0; i<REMOTE_PORTS.length; i++) {
            if(REMOTE_PORTS[i].equals(port))
                return i+1;
        }
        return -1;
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
}
