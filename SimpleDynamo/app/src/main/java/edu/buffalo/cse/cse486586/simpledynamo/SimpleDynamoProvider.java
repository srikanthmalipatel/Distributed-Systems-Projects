package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.net.SocketTimeoutException;
import java.io.StreamCorruptedException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.File;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();

    static final int SERVER_PORT = 10000;
    static final String[] RING = {"11124", "11112", "11108", "11116", "11120"};
    static HashMap<String, String> RING_PORT = new HashMap<String, String>();
    static HashMap<String, String> RING_PORT_PRED = new HashMap<String, String>();
    static String myPort;

    // Variables for failure handling
    private static boolean isAvdRestarted = false;
    private static int recoveredAvdCount = 0;

    // Variables for maintaining the Dynamo Ring
    private static String nodeId = null;
    private static String preNode = null;
    private static String nextNode = null;

    // uri of content provider
    private static Uri mUri;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static final String[] columns = new String[] {"key", "value"};

    // Replication parameters
    private static final int repCount = 2;

    // Variables for query functionality
    private static boolean isFound = true;
    private static boolean isQueryRunning = false;
    private static String queryResult = null;
    private static int queryResCount = 0;
    private static String currQuerySelection = null;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
        if(selection.contains("*") || selection.contains("@")) {
            try {
                // refered directory listing code from http://stackoverflow.com/questions/4917326/how-to-iterate-over-the-files-of-a-certain-directory-in-java
                Log.d("query", "File path: " + getContext().getFilesDir());
                File dir = new File(getContext().getFilesDir() + "");
                File[] directoryListing = dir.listFiles();
                if (directoryListing != null) {
                    for (File child : directoryListing) {
                        if(!child.getName().equals("restart")) {
                            Log.d("Delete", "Deleting file " + child.getName());
                            child.delete();
                        }
                    }
                }
            } catch (Exception e) {
                Log.d(TAG, "File Read failed");
            }

            //MatrixCursor mcursor = new MatrixCursor(projection);
            return 0;
        } else {
            try {
                Log.d("delete", "Deleteing key " + selection);
                String deletePort = getKeyLocation(selection);

                String msg = "DELETEKEY-" + selection;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, deletePort);
                Log.e("delete", "Successors " + RING_PORT.get(deletePort));
                String[] remotePorts = (RING_PORT.get(deletePort)).split("%");
                Log.e("insert", "remotePorts: " + remotePorts[0] + " " + remotePorts[1]);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, remotePorts[0]);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, remotePorts[1]);

                /*File dir = new File(getContext().getFilesDir() + "");
                File[] directoryListing = dir.listFiles();
                if (directoryListing != null) {
                    for (File child : directoryListing) {
                        if (child.getName().equals(selection)) {
                            Log.d("Delete", "Deleting file " + child.getName());
                            child.delete();
                        }
                    }
                }*/
            } catch (Exception e) {
                Log.d(TAG, "File Read failed");
            }
        }
        return 0;
	}

    public void deleteKey(String selection) {
        try {
            File dir = new File(getContext().getFilesDir() + "");
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {
                for (File child : directoryListing) {
                    if (child.getName().equals(selection)) {
                        Log.d("Delete", "Deleting file " + child.getName());
                        child.delete();
                    }
                }
            }
        } catch (Exception e) {
            Log.d(TAG, "File Read failed");
        }
    }

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

        while(recoveredAvdCount != 2);

        String insertPort = getKeyLocation(values.getAsString("key"));
        Log.d("insert", "Inserting key " + values.getAsString("key") + " at Node " + insertPort);

        String msg = "INSERTKEY-" + values.getAsString("key") + "-" + values.getAsString("value");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, insertPort);
        Log.e("insert", "Successors " + RING_PORT.get(insertPort));
        String[] remotePorts = (RING_PORT.get(insertPort)).split("%");
        Log.e("insert", "remotePorts: " + remotePorts[0] + " " + remotePorts[1]);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, remotePorts[0]);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, remotePorts[1]);
        return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        final String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        Log.d(TAG, "Launching AVD-" + portStr);

        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

        nodeId = genHashFromPort(myPort);

        initializeRing();

        Log.d(TAG, "currentNode: " + myPort + " predecessor: " + preNode + " successor: " + nextNode);
        recoveredAvdCount = 0;

        try {
            int byteRead;
            StringBuffer sBuff = new StringBuffer("");
            FileInputStream inputStream = new FileInputStream(getContext().getFilesDir() + "/restart");
            while((byteRead = inputStream.read()) != -1) {
                sBuff.append((char)byteRead);
            }
            inputStream.close();

            Log.d(TAG, "This AVD has been RESTARTED");

            isAvdRestarted = true;

            recoverKeys();


        } catch (Exception e) {
            Log.d(TAG, "This AVD is running for first time");
            try {
                recoveredAvdCount = 2;

                String msg = "restart";
                FileOutputStream outputStream = new FileOutputStream(new File(getContext().getFilesDir() + "/restart"));
                outputStream.write(msg.getBytes());
                outputStream.close();
            } catch (Exception ex) {
                Log.d(TAG, "Unable to create RESTART file");
                ex.printStackTrace();
            }
        }

        if(isAvdRestarted) {
            // handle recovery
            Log.d(TAG, "RESTARTED AVD - recovering keys");
            // get this nodes keys from successor and replica keys from its 2 predecessors

        }

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT, 20);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.d(TAG, "Can't create a ServerSocket");
            return false;
        }

        Log.d(TAG, "Server task created on AVD-" + portStr);

		return false;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
        //while(isQueryRunning);
        //isQueryRunning = true;
        while(recoveredAvdCount != 2);
        Log.d("Query", "Currently running query on key:" + selection);
        queryResCount = 0;

		// TODO Auto-generated method stub
        if(selection.contains("@")) {
            try {
                int byteRead;
                MatrixCursor mcursor = new MatrixCursor(columns);

                // refered directory listing code from http://stackoverflow.com/questions/4917326/how-to-iterate-over-the-files-of-a-certain-directory-in-java
                Log.d("query", "File path: " + getContext().getFilesDir());
                File dir = new File(getContext().getFilesDir() + "");
                File[] directoryListing = dir.listFiles();
                if (directoryListing != null) {
                    for (File child : directoryListing) {
                        if(!child.getName().equals("restart")) {
                            StringBuffer sBuff = new StringBuffer("");
                            FileInputStream inputStream = new FileInputStream(child);
                            while ((byteRead = inputStream.read()) != -1) {
                                sBuff.append((char) byteRead);
                            }
                            inputStream.close();
                            mcursor.addRow(new String[]{child.getName(), sBuff.toString()});
                        }
                    }
                }
                isQueryRunning = false;
                return mcursor;
            } catch (Exception e) {
                Log.d(TAG, "File Read failed");
            }

            //MatrixCursor mcursor = new MatrixCursor(projection);
            Log.v("query", selection);
        } else if(selection.contains("*")) {
            Cursor resCursor = queryAllInMyNode();
            if(myPort == preNode && myPort == nextNode)
                return resCursor;
            queryResult = "";
            queryResult += buildString(resCursor);
            Log.d("Query", "My results: " + queryResult);

            //forwardQuery(myPort, "*", msg);

            String msg = "QUERYALL-" + myPort;
            for(int i=0; i<RING.length; i++) {
                if(!myPort.equals(RING[i]))
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, RING[i]);
            }

            while(isFound);

            Log.d("Final String", queryResult);
            MatrixCursor mcursor = new MatrixCursor(columns);
            String[] qryArr = queryResult.split("::");
            for(String res : qryArr) {
                Log.d("Process each string", res);
                if(!res.equals("")) {
                    String[] keyVals = res.split("%");
                    mcursor.addRow(new String[]{keyVals[0], keyVals[1]});
                }
            }
            queryResult = null;
            isFound = true;
            isQueryRunning = false;
            return mcursor;
        }
        else {
            try {
                int byteRead;
                MatrixCursor mcursor = new MatrixCursor(columns);
                String queryPort = getKeyLocation(selection);
                String[] succPort = (RING_PORT.get(queryPort)).split("%");
                Log.d("query", "Querying key: " + selection + " at Node: " + queryPort);

                currQuerySelection = selection;
                if(myPort.equals(queryPort)) {

                    File dir = new File(getContext().getFilesDir() + "");
                    File[] directoryListing = dir.listFiles();
                    if (directoryListing != null) {
                        for (File child : directoryListing) {
                            if(child.getName().equals(selection)) {
                                Log.d("query", "Found selection: " + selection);
                                StringBuffer sBuff = new StringBuffer("");
                                FileInputStream inputStream = new FileInputStream(child);
                                while ((byteRead = inputStream.read()) != -1) {
                                    sBuff.append((char) byteRead);
                                }
                                inputStream.close();
                                mcursor.addRow(new String[]{child.getName(), sBuff.toString()});
                            }
                        }
                    }
                } else {

                    String msg = "QUERYF-" + myPort + "-" + selection;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, queryPort);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, succPort[0]);
                    //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, succPort[1]);

                    // wait till the key is found in other avd's or till it completes full circle in the ring
                    while (isFound) ;

                    // if any avd has returned the key then create a cursor and return it
                    String res[] = queryResult.split("%");
                    Log.e("Query:", "Retrieved key:" + res[0] + "%" + res[1] + " from remote avd");

                    mcursor.addRow(new String[]{res[0], res[1]});
                    queryResult = null;

                    Log.d("Query", "Completed query on key:" + selection);

                }
                currQuerySelection = null;
                isFound = true;
                return mcursor;

            } catch (Exception e) {
                Log.d(TAG, "File Read failed");
            }
            Log.v("query", selection);
        }
        Log.d("Query", "Completed query on key:" + selection);
        isQueryRunning = false;
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.d(TAG, "Starting a server on this Node");
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
                        Log.d("ServerTask", "Recieved MSG: " + line);


                        /*String[] msgArr = line.split("-");
                        String ackMsg = "ACK-" + msgArr[0];
                        OutputStream os = client.getOutputStream();
                        os.write(ackMsg.getBytes());
                        os.flush();*/


                        // add your code here
                        if (line.contains("INSERTKEY")) {
                            /*
                                Message Format:
                                INSERTKEY-%key%-%value%           - Inserts key into the provider
                             */
                            String[] msg = line.split("-");
                            ContentValues values = new ContentValues();
                            values.put(KEY_FIELD, msg[1]);
                            values.put(VALUE_FIELD, msg[2]);
                            insertIntoProvider(mUri, values);
                        } else if (line.contains("REPLICATEKEY")) {
                            /*
                                Message Format:
                                REPLICATEKEY-%key%-%value%-%RepCount%           - Replicates the key on to its successor
                             */
                            String[] msg = line.split("-");
                            ContentValues values = new ContentValues();
                            values.put(KEY_FIELD, msg[1]);
                            values.put(VALUE_FIELD, msg[2]);
                            //int count = Integer.parseInt(msg[3]) - 1;
                            insertIntoProvider(mUri, values);
                        } else if (line.contains("QUERYF")) {
                            String[] msg = line.split("-");
                            Cursor resCursor = queryMyNode(msg[2]);
                            forwardQueryResult(msg[1], resCursor);
                        } else if (line.contains("QUERYR")) {
                            String[] msg = line.split("-");
                            if((currQuerySelection != null) && msg[1].contains(currQuerySelection)) {
                                queryResCount++;
                                if (queryResCount == 1) {
                                    queryResult = msg[1];
                                    isFound = false;
                                }
                            }
                        } else if (line.contains("QUERYALL")) {
                            String[] msg = line.split("-");
                            String qryRes = buildString(queryAllInMyNode());
                            forwardQueryAll(msg[1], qryRes);

                        } else if(line.contains("QRESULTALL")) {
                            String[] msg = line.split("-");
                            Log.d("ServerTask", "Got queryAllResult " + msg[2]);
                            queryResCount++;
                            Log.d("ServerTask", "Total string before: " + queryResult);
                            queryResult += "::" + msg[2];
                            Log.d("ServerTask", "Total string after: " + queryResult);
                            if(queryResCount == 3) {
                                isFound = false;
                            }
                        } else if(line.contains("GETMYKEYS")) {
                            String[] msg = line.split("-");
                            sendKeys(msg[1]);
                        } else if(line.contains("RECOVERYKEYS")) {
                            String[] msg = line.split("-");
                            if(msg.length == 2) {
                                extractMyKeys(msg[1]);
                            } else {
                                recoveredAvdCount++;
                            }
                        }else if(line.contains("GETYOURKEYS")) {
                            String[] msg = line.split("-");
                            sendReplicaKeys(msg[1]);
                        } else if(line.contains("DELETEKEY")) {
                            String[] msg = line.split("-");
                            deleteKey(msg[1]);
                        }

                    }

                } catch (IOException e) {
                    Log.d(TAG, "Failed to accept new connection");
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
            //Log.d(TAG, "Sending join request to AVD-5554");


            try {
                // Connect to AVD-5554
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[1]));
                socket.setSoTimeout(5000);

                Log.d("ClientTask", "Sending Message: " + msgs[0] + " to remotePort " + msgs[1]);

                msgs[0].trim();

                //Log.d("ClientTask", "hello");

                // Used this piece of code from http://stackoverflow.com/questions/17440795/send-a-string-instead-of-byte-through-socket-in-java
                /*PrintWriter out = new PrintWriter(socket.getOutputStream());
                out.print(msgs[0]);
                out.flush();
                socket.close();*/
                OutputStream os = socket.getOutputStream();
                os.write(msgs[0].getBytes());
                os.flush();

                /*String line = null;
                Log.e("ClientTask", "Waiting for ack");
                BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                line = input.readLine();
                Log.d("Client", "Recieved ACK: " + line);*/

                socket.close();

            }/* catch (SocketTimeoutException e) {
                Log.e("ClientTask", "SocketTimeoutException on node " + msgs[1] + " - Handling failure now");
                Log.e("ClientTask", "Message sent to node " + msgs[1] + " is " + msgs[0]);
                handleFailure(msgs[0], msgs[1]);
            } catch (StreamCorruptedException e) {
                Log.e("ClientTask", "StreamCorruptedException on node " + msgs[1] + " - Handling failure now");
                Log.e("ClientTask", "Message sent to node " + msgs[1] + " is " + msgs[0]);
                handleFailure(msgs[0], msgs[1]);
            } catch (EOFException e) {
                Log.e("ClientTask", "EOFException on node " + msgs[1] + " - Handling failure now");
                Log.e("ClientTask", "Message sent to node " + msgs[1] + " is " + msgs[0]);
                handleFailure(msgs[0], msgs[1]);
            } catch ( IOException e) {
                Log.e("ClientTask", "IOException on node " + msgs[1] + " - Handling failure now");
                Log.e("ClientTask", "Message sent to node " + msgs[1] + " is " + msgs[0]);
                handleFailure(msgs[0], msgs[1]);
            }*/ catch ( Exception e) {
                e.printStackTrace();
            }


            return null;
        }

    }

    public void handleFailure(String msg, String remPort) {
        // handle query
        if(msg.contains("QUERYF")) {
            // send this message to its successor
            String[] succPorts = (RING_PORT.get(remPort)).split("%");
            Log.e("handleFailure", "Failure detected! Forwarding the query to its successor " + succPorts[0]);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, succPorts[0]);
        } else if(msg.contains("QUERYALL")) {
            Log.e("handleFailure", "Failure detected! Forwarding the query to its successor " + (RING_PORT.get(myPort)).split("%")[1]);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, (RING_PORT.get(myPort)).split("%")[1]);
        }
    }

    public void recoverKeys() {
        // TODO: since I will be sending the keys to one of its successors. what if that is failed
        Log.d("recoverKeys", "Will be recovering this AVD keys from its successor and replica keys from its predecessor");
        String msg = "GETMYKEYS-" + myPort;
        String[] succPorts = (RING_PORT.get(myPort)).split("%");
        Log.d("recoverKeys", "Sending message to its successor " + succPorts[0] + " to retrieve its keys");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, succPorts[0]);
        msg = "GETYOURKEYS-" + myPort;
        String[] predPorts = (RING_PORT_PRED.get(myPort).split("%"));
        Log.d("recoverKeys", "Sending message to its predecessor " + predPorts[0] + " to retrieve its keys");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, predPorts[0]);
    }

    public void sendKeys(String srcPort) {
        String msg = "RECOVERYKEYS-";
        try {
            int byteRead, i = 0;
            //MatrixCursor mcursor = new MatrixCursor(columns);

            // refered directory listing code from http://stackoverflow.com/questions/4917326/how-to-iterate-over-the-files-of-a-certain-directory-in-java
            //Log.d("queryNode", "File path: " + getContext().getFilesDir());
            File dir = new File(getContext().getFilesDir() + "");
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {
                for (File child : directoryListing) {
                    if(srcPort.equals(getKeyLocation(child.getName()))) {
                        Log.d("sendKeys", "Sending key " + child.getName() + " to Node-" + srcPort);
                        StringBuffer sBuff = new StringBuffer("");
                        FileInputStream inputStream = new FileInputStream(child);
                        while ((byteRead = inputStream.read()) != -1) {
                            sBuff.append((char) byteRead);
                        }
                        inputStream.close();
                        if(i != 0)
                            msg += "::";
                        msg += child.getName() + "%" + sBuff.toString();
                        i++;
                    }
                    //mcursor.addRow(new String[]{child.getName(), sBuff.toString()});
                }
            }
            //return mcursor;
        } catch (Exception e) {
            Log.d(TAG, "File Read failed");
        }
        Log.d("sendKeys", "Sending all of its keys to node " + srcPort);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, srcPort);

    }

    public void sendReplicaKeys(String srcPort) {
        String msg = "RECOVERYKEYS-";
        try {
            int byteRead, i = 0;
            String[] predPorts = (RING_PORT_PRED.get(srcPort)).split("%");
            //MatrixCursor mcursor = new MatrixCursor(columns);

            // refered directory listing code from http://stackoverflow.com/questions/4917326/how-to-iterate-over-the-files-of-a-certain-directory-in-java
            //Log.d("queryNode", "File path: " + getContext().getFilesDir());
            File dir = new File(getContext().getFilesDir() + "");
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {
                for (File child : directoryListing) {
                    if(predPorts[0].equals(getKeyLocation(child.getName())) || predPorts[1].equals(getKeyLocation(child.getName()))) {
                        Log.d("sendReplicaKeys", "Sending key " + child.getName() + " to Node-" + srcPort);
                        StringBuffer sBuff = new StringBuffer("");
                        FileInputStream inputStream = new FileInputStream(child);
                        while ((byteRead = inputStream.read()) != -1) {
                            sBuff.append((char) byteRead);
                        }
                        inputStream.close();
                        if(i != 0)
                            msg += "::";
                        msg += child.getName() + "%" + sBuff.toString();
                        i++;
                    }
                    //mcursor.addRow(new String[]{child.getName(), sBuff.toString()});
                }
            }
            //return mcursor;
        } catch (Exception e) {
            Log.d(TAG, "File Read failed");
        }
        Log.d("sendReplicaKeys", "Sending all of its keys to node " + srcPort);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, srcPort);

    }

    public void extractMyKeys(String msg) {

        String[] keysArr = msg.split("::");
        for(String res : keysArr) {
            Log.d("Process each string", res);
            if(!res.equals("")) {
                String[] keyVals = res.split("%");
                try {
                    Log.d("extractMyKeys", "Recieved key " + keyVals[0]);
                    FileOutputStream outputStream = new FileOutputStream(new File(getContext().getFilesDir() + "/" + keyVals[0]));
                    outputStream.write(keyVals[1].getBytes());
                    outputStream.close();
                } catch (Exception e) {
                    Log.d(TAG, "File write failed");
                }
            }
        }
        recoveredAvdCount++;
        Log.e("extractKeys", "Retrieved all my keys from my successor");
        return;
    }

    /*
        When inserting the key if I know the which avd the key goes into then we can send insert message to the avd
        When querying the key if I know the avd of the key the we can send query message to the avd
     */


    public String getKeyLocation(String key) {
        try {
            // static String[] RING_PORTS = new String[] {"11124", "11112", "11108", "11116", "11120"};
            String keyHash = genHash(key);
            // check for cases at ring start and ring end
            int keyHashprevHashCompare = keyHash.compareTo(genHashFromPort("11120"));
            int keyHashcurrHashCompare = keyHash.compareTo(genHashFromPort("11124"));
            if((keyHashprevHashCompare > 0 && keyHashcurrHashCompare > 0) || (keyHashprevHashCompare < 0 && keyHashcurrHashCompare < 0)) {
                return "11124";
            }

            keyHashprevHashCompare = keyHashcurrHashCompare;
            keyHashcurrHashCompare = keyHash.compareTo(genHashFromPort("11112"));
            if(keyHashprevHashCompare > 0 && keyHashcurrHashCompare < 0) {
                return "11112";
            }

            keyHashprevHashCompare = keyHashcurrHashCompare;
            keyHashcurrHashCompare = keyHash.compareTo(genHashFromPort("11108"));
            if(keyHashprevHashCompare > 0 && keyHashcurrHashCompare < 0) {
                return "11108";
            }

            keyHashprevHashCompare = keyHashcurrHashCompare;
            keyHashcurrHashCompare = keyHash.compareTo(genHashFromPort("11116"));
            if(keyHashprevHashCompare > 0 && keyHashcurrHashCompare < 0) {
                return "11116";
            }

            keyHashprevHashCompare = keyHashcurrHashCompare;
            keyHashcurrHashCompare = keyHash.compareTo(genHashFromPort("11120"));
            if(keyHashprevHashCompare > 0 && keyHashcurrHashCompare < 0) {
                return "11120";
            }



        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }


    // Given a key and value insert this into the provider. Currently using local storage(i.e files with key as name and
    //  value as content) as content provider
    public Uri insertIntoProvider(Uri uri, ContentValues values) {
        Log.d("insertIntoProvider", "Inserting key: " + values.getAsString("key") + " into the provider");
        String filename = values.getAsString("key");
        String message = values.getAsString("value");

        try {
            FileOutputStream outputStream = new FileOutputStream(new File(getContext().getFilesDir() + "/" + filename));
            outputStream.write(message.getBytes());
            outputStream.close();
        } catch (Exception e) {
            Log.d(TAG, "File write failed");
        }

        /*if((count - 1) >= 0) {
            // Implementing replication
            Log.d("insertIntoProvider", "Replicating the key onto its next" + count + "successors");
            Log.d("insertIntoProvider", "Sending Replicate message to its successor: " + nextNode);
            String msg = "REPLICATEKEY-" + values.getAsString("key") + "-" + values.getAsString("value") + "-" + (count - 1);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, nextNode);
        }*/
        return uri;
    }

    public void forwardQueryAll(String srcPort, String keyVals) {
        //Log.d("query", "Could not find selection in this Node! Forwarding the query to its successor");
        String msg = null;
        msg = "QRESULTALL-" + myPort + "-" + keyVals;
        Log.d("forwardQueryAll", "Forwarding Query all Result to source port " + srcPort);

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, srcPort);
    }

    public Cursor queryMyNode(String selection) {
        Log.d("queryMyNode", "querying for selection: " + selection);
        try {
            int byteRead;
            MatrixCursor mcursor = new MatrixCursor(columns);
            boolean isSelectionFound = false;

            // refered directory listing code from http://stackoverflow.com/questions/4917326/how-to-iterate-over-the-files-of-a-certain-directory-in-java
            //Log.d("queryNode", "File path: " + getContext().getFilesDir());
            File dir = new File(getContext().getFilesDir() + "");
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {
                for (File child : directoryListing) {
                    if(child.getName().equals(selection)) {
                        Log.d("queryMyNode", "Found selection: " + selection);
                        isSelectionFound = true;
                        StringBuffer sBuff = new StringBuffer("");
                        FileInputStream inputStream = new FileInputStream(child);
                        while ((byteRead = inputStream.read()) != -1) {
                            sBuff.append((char) byteRead);
                        }
                        inputStream.close();
                        mcursor.addRow(new String[]{child.getName(), sBuff.toString()});
                    }
                }
            }
            if(isSelectionFound)
                return mcursor;
            else
                return null;
        } catch (Exception e) {
            Log.d(TAG, "File Read failed");
        }

        //MatrixCursor mcursor = new MatrixCursor(projection);
        Log.v("queryMyNode", selection);
        return null;
    }

    public Cursor queryAllInMyNode() {
        try {
            int byteRead;
            MatrixCursor mcursor = new MatrixCursor(columns);

            // refered directory listing code from http://stackoverflow.com/questions/4917326/how-to-iterate-over-the-files-of-a-certain-directory-in-java
            //Log.d("queryNode", "File path: " + getContext().getFilesDir());
            File dir = new File(getContext().getFilesDir() + "");
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {
                for (File child : directoryListing) {
                    if(!child.getName().equals("restart")) {
                        StringBuffer sBuff = new StringBuffer("");
                        FileInputStream inputStream = new FileInputStream(child);
                        while ((byteRead = inputStream.read()) != -1) {
                            sBuff.append((char) byteRead);
                        }
                        inputStream.close();
                        mcursor.addRow(new String[]{child.getName(), sBuff.toString()});
                    }
                }
            }
            return mcursor;
        } catch (Exception e) {
            Log.d(TAG, "File Read failed");
        }

        //MatrixCursor mcursor = new MatrixCursor(projection);
        return null;
    }

    public void forwardQueryResult(String srcPort, Cursor resCursor) {
        Log.d("forwardQueryResult", "Found the selection, forwarding the result to Source");
        String msg = "QUERYR-";
        int i = 0;
        if(resCursor == null)
            return;
        while(resCursor.moveToNext()) {
            if(i != 0)
                msg += "::";
            String key = resCursor.getString(resCursor.getColumnIndex("key"));
            String value = resCursor.getString(resCursor.getColumnIndex("value"));
            msg += key + "%" + value;
            i++;
        }
        resCursor.close();
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, srcPort);
    }

    public String buildString(Cursor resCursor) {
        Log.d("buildString", "Constructing result with all keys and values");
        String msg = "";
        int i = 0;
        while(resCursor.moveToNext()) {
            if(i != 0)
                msg += "::";
            String key = resCursor.getString(resCursor.getColumnIndex("key"));
            String value = resCursor.getString(resCursor.getColumnIndex("value"));
            msg += key + "%" + value;
            i++;
        }
        resCursor.close();
        return msg;
    }

    private void initializeRing() {
        Log.d("initializeRing", "Initializing Ring");
        if(myPort.equals("11108")) {
            preNode = "11112";
            nextNode = "11116";
        } else if(myPort.equals("11112")) {
            preNode = "11124";
            nextNode = "11108";
        } else if(myPort.equals("11116")) {
            preNode = "11108";
            nextNode = "11120";
        } else if(myPort.equals("11120")) {
            preNode = "11116";
            nextNode = "11124";
        } else if(myPort.equals("11124")) {
            preNode = "11120";
            nextNode = "11112";
        }
        RING_PORT.put("11108", "11116%11120");
        RING_PORT.put("11112", "11108%11116");
        RING_PORT.put("11116", "11120%11124");
        RING_PORT.put("11120", "11124%11112");
        RING_PORT.put("11124", "11112%11108");

        RING_PORT_PRED.put("11108", "11112%11124");
        RING_PORT_PRED.put("11112", "11124%11120");
        RING_PORT_PRED.put("11116", "11108%11112");
        RING_PORT_PRED.put("11120", "11116%11108");
        RING_PORT_PRED.put("11124", "11120%11116");
    }

    private String genHashFromPort(String port) {
        try {
            return genHash(String.valueOf(Integer.parseInt(port)/2));
        } catch (NoSuchAlgorithmException c) {
            c.printStackTrace();
        }
        return null;
    }

    private synchronized String genHash(String input) throws NoSuchAlgorithmException {
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
