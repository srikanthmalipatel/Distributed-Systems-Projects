package edu.buffalo.cse.cse486586.simpledynamo;

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
import java.io.FileInputStream;
import java.io.File;

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
    static String[] RING_PORTS = new String[] {"11124", "11112", "11108", "11116", "11120"};
    static String myPort;

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
                        Log.d("Delete", "Deleting file " + child.getName());
                        child.delete();
                    }
                }
            } catch (Exception e) {
                Log.d(TAG, "File Read failed");
            }

            //MatrixCursor mcursor = new MatrixCursor(projection);
            return 0;
        } else {
            try {
                Log.d("query", "File path: " + getContext().getFilesDir());
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

        String insertPort = getKeyLocation(values.getAsString("key"));
        Log.d("insert", "Inserting key " + values.getAsString("key") + " at Node " + insertPort);

        String msg = "INSERTKEY-" + values.getAsString("key") + "-" + values.getAsString("value");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, insertPort);
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
        Log.d("Query", "Currently running query on key:" + selection);

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
                        StringBuffer sBuff = new StringBuffer("");
                        FileInputStream inputStream = new FileInputStream(child);
                        while((byteRead = inputStream.read()) != -1) {
                            sBuff.append((char)byteRead);
                        }
                        inputStream.close();
                        mcursor.addRow(new String[] {child.getName(), sBuff.toString()});
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
            String msg = buildString(resCursor);
            forwardQuery(myPort, "*", msg);

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
                String queryPort = getKeyLocation(selection);
                Log.d("query", "Querying key: " + selection + " at Node: " + queryPort);

                String msg = "QUERYF-" + myPort + "-" + selection;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, queryPort);

                // wait till the key is found in other avd's or till it completes full circle in the ring
                while(isFound);

                // if any avd has returned the key then create a cursor and return it
                String res[] = queryResult.split("%");
                Log.d("Query:" , "Retrieved key:" + res[0] + " from remote avd");

                MatrixCursor mcursor = new MatrixCursor(columns);
                mcursor.addRow(new String[] {res[0], res[1]});
                isFound = true;
                queryResult = null;
                isQueryRunning = false;
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
                        // add your code here
                        if(line.contains("INSERTKEY")) {
                            /*
                                Message Format:
                                INSERTKEY-%key%-%value%           - Inserts key into the provider
                             */
                            String[] msg = line.split("-");
                            ContentValues values = new ContentValues();
                            values.put(KEY_FIELD, msg[1]);
                            values.put(VALUE_FIELD, msg[2]);
                            insertIntoProvider(mUri, values, repCount);
                        } else if(line.contains("REPLICATEKEY")) {
                            /*
                                Message Format:
                                REPLICATEKEY-%key%-%value%-%RepCount%           - Replicates the key on to its successor
                             */
                            String[] msg = line.split("-");
                            ContentValues values = new ContentValues();
                            values.put(KEY_FIELD, msg[1]);
                            values.put(VALUE_FIELD, msg[2]);
                            //int count = Integer.parseInt(msg[3]) - 1;
                            insertIntoProvider(mUri, values, Integer.parseInt(msg[3]));
                        } else if(line.contains("QUERYF")) {
                            String[] msg = line.split("-");
                            Cursor resCursor = queryMyNode(msg[2]);
                            forwardQueryResult(msg[1], resCursor);
                        } else if(line.contains("QUERYR")) {
                            String[] msg = line.split("-");
                            queryResult = msg[1];
                            isFound = false;
                        } else if(line.contains("QUERYALL")) {
                            String[] msg = line.split("-");
                            if(myPort.equals(msg[1])) {
                                if(msg.length == 2) {
                                    queryResult = "";
                                } else {
                                    queryResult = msg[2];
                                }
                                isFound = false;
                            } else {
                                String qryRes = buildString(queryAllInMyNode());
                                if(msg.length == 2) {
                                    forwardQuery(msg[1], "*", qryRes);
                                } else {
                                    msg[2] += "::" + qryRes;
                                    forwardQuery(msg[1], "*", msg[2]);
                                }
                            }
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

                Log.d("ClientTask", "Sending Message: " + msgs[0] + " to remotePort " + msgs[1]);

                msgs[0].trim();

                // Used this piece of code from http://stackoverflow.com/questions/17440795/send-a-string-instead-of-byte-through-socket-in-java
                PrintWriter out = new PrintWriter(socket.getOutputStream());
                out.print(msgs[0]);
                out.flush();
                socket.close();

            } catch (UnknownHostException e) {
                Log.d(TAG, "ClientTask UnknownHostException");
                e.printStackTrace();
            } catch (IOException e) {
                Log.d(TAG, "ClientTask socket IOException");
                e.printStackTrace();
            }

            return null;
        }

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
    public Uri insertIntoProvider(Uri uri, ContentValues values, int count) {
        if(count >= 0) {
            Log.d("insertIntoProvider", "Inserting key: " + values.getAsString("key") + " into the provider with replication count: " + count);
            String filename = values.getAsString("key");
            String message = values.getAsString("value");

            try {
                FileOutputStream outputStream = new FileOutputStream(new File(getContext().getFilesDir() + "/" + filename));
                outputStream.write(message.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.d(TAG, "File write failed");
            }

            if((count - 1) >= 0) {
                // Implementing replication
                Log.d("insertIntoProvider", "Replicating the key onto its next" + count + "successors");
                Log.d("insertIntoProvider", "Sending Replicate message to its successor: " + nextNode);
                String msg = "REPLICATEKEY-" + values.getAsString("key") + "-" + values.getAsString("value") + "-" + (count - 1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, nextNode);
            }
        }

        return uri;
    }

    public void forwardQuery(String srcPort, String selection, String keyVals) {
        //Log.d("query", "Could not find selection in this Node! Forwarding the query to its successor");
        String msg = null;
        if(selection.equals("*")) {
            msg = "QUERYALL-" + srcPort + "-" + keyVals;
            Log.d("forwardQuery", "Forwarding Query all to its successor");
        }else {
            msg = "QUERYF-" + srcPort + "-" + selection;
            Log.d("forwardQuery", "Could not find selection in this Node! Forwarding the query to its successor");
        }
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, nextNode);
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
                    StringBuffer sBuff = new StringBuffer("");
                    FileInputStream inputStream = new FileInputStream(child);
                    while ((byteRead = inputStream.read()) != -1) {
                        sBuff.append((char) byteRead);
                    }
                    inputStream.close();
                    mcursor.addRow(new String[]{child.getName(), sBuff.toString()});
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
