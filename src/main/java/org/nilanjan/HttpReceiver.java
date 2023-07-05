package org.nilanjan;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpReceiver extends Receiver<String> {
    private String url;

    public HttpReceiver(String url) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.url = url;
    }

    @Override
    public void onStart() {
        new Thread(this::receive).start();
    }

    @Override
    public void onStop() {
    }

    private void receive() {
        try {
            URL endpoint = new URL(url);
            HttpURLConnection connection = (HttpURLConnection)endpoint.openConnection();

            if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String line;
                while ((line = reader.readLine()) != null && !isStopped()) {
                    store(line);
                }
                reader.close();
            }
            else {
                System.out.println("Connection failed with HTTP error code: " + connection.getResponseCode());
            }
            connection.disconnect();
        }
        catch (Exception e) {
            System.out.println("Error receiving data from HTTP endpoint: " + e.getMessage());
        }
        if (!isStopped()) {
            restart("Restarting receiver for new data");
        }
    }

}

