import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

public class DataStore {

    public static final String DELIMITER = "|";
    private static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;
    private static final String DATA_STORE_FILE_FORMAT = ".txt";
    private static final int CHAR_LEN = 2;
    private static final int SIXTEEN_KB_BYTES = 16 * 1024;
    private static final int MAX_KEY_LEN = 32;

    private String dataPath;
    Map<String, Value> dataStore;
    private static final Object DATA_STORE_LOCK = new Object();
    private Thread deleteThread;

    public DataStore(String dataPath) {
        this.dataStore = new ConcurrentHashMap<>();
        this.dataPath = dataPath;
        try {
            new File(this.dataPath).createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public DataStore() {
        this(getDataStorePath());
    }

    private static String getDataStorePath() {
        String path = "/tmp/data_store";
        int index = 0;
        while (new File(path + index + DATA_STORE_FILE_FORMAT).exists()) {
            index++;
        }
        return path + index + DATA_STORE_FILE_FORMAT;
    }

    public void init() throws FileNotFoundException {
        readData();

        deleteThread = new Thread(this::checkExpiryAndRemove);
        deleteThread.start();
    }

    private void readData() throws FileNotFoundException {
        File myObj = new File(this.dataPath);
        Scanner myReader = new Scanner(myObj);
        while (myReader.hasNextLine()) {
            String data = myReader.nextLine();
            String[] split = data.split("\\|");
            this.dataStore.put(split[0], new Value(split[1], LocalDateTime.parse(split[2], DATE_FORMATTER)));
        }
        myReader.close();
    }

    private void checkExpiryAndRemove() {
        while (true) {
            for (String key : this.dataStore.keySet()) {
                Value value = this.dataStore.get(key);
                if (value.getExpiryTime().isBefore(LocalDateTime.now())) {
                    delete(key);
                }
            }
        }
    }

    public boolean create(String key, String jsonData, Integer timeToLive) {
        if (this.dataStore.containsKey(key)) {
            System.out.println("Key Already Present");
            return false;
        } else {
            if (!isAValidKey(key)) {
                System.out.println("Invalid Key: Key is greater than 32 characters");
                return false;
            }
            try {
                validateValue(jsonData);
            } catch (Exception e) {
                System.out.println("Invalid value: " + e.getMessage());
                return false;
            }
            Value value = new Value(jsonData, LocalDateTime.now().plusSeconds(timeToLive));
            this.dataStore.put(key, value);
            return true;
        }
    }

    private void validateValue(String data) {
        try {
            JsonParser.parseString(data);
        } catch (JsonParseException e) {
            throw new IllegalArgumentException("Not a valid JSON string");
        }

        if (data.length() * CHAR_LEN > SIXTEEN_KB_BYTES) {
            throw new IllegalArgumentException("Value is greater than 16 KB");
        }
    }

    private boolean isAValidKey(String key) {
        return key.length() <= MAX_KEY_LEN;
    }

    public String read(String key) {
        synchronized (DATA_STORE_LOCK) {
            if (this.dataStore.containsKey(key)) {
                return this.dataStore.get(key).getData();
            }
        }
        System.out.println("Key Not Found");
        return null;
    }

    private boolean delete(String key) {
        synchronized (DATA_STORE_LOCK) {
            if (this.dataStore.containsKey(key)) {
                this.dataStore.remove(key);
                return true;
            }
        }
        System.out.println("Key Not Found");
        return false;
    }

    public void close() {
        deleteThread.stop();
    }

    public void commit() throws IOException {
        FileWriter fileWrite = new FileWriter(this.dataPath);
        for (String key : this.dataStore.keySet()) {
            Value value = this.dataStore.get(key);
            fileWrite.write(key + DELIMITER + value.getData() + DELIMITER + value.getExpiryTime().format(DATE_FORMATTER) + "\n");
        }
        fileWrite.close();
    }

    class Value {
        private String data;
        private LocalDateTime expiryTime;

        public Value(String jsonData, LocalDateTime expiryTime) {
            this.data = jsonData;
            this.expiryTime = expiryTime;
        }


        public String getData() {
            return data;
        }

        public LocalDateTime getExpiryTime() {
            return expiryTime;
        }
    }
}