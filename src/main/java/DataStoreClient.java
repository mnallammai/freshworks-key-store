import java.io.IOException;

class DataStoreClient {

    public static void main(String[] args) throws InterruptedException, IOException {
        DataStore dataStore = new DataStore("/tmp/data_store3.csv");
        dataStore.init();

        dataStore.create("tmp-data", "welcome", 2);
        System.out.println(dataStore.read("tmp-data"));
        Thread.sleep(1000);
        System.out.println(dataStore.read("tmp-data"));
        Thread.sleep(1000);
        System.out.println(dataStore.read("tmp-data"));

        dataStore.create("perm-data", "welcome", 200);

        dataStore.commit();
        dataStore.close();

        // Open and close the store again with same file path.
        dataStore = new DataStore("/tmp/data_store.csv");
        dataStore.init();

        System.out.println(dataStore.read("perm-data"));

        dataStore.commit();
        dataStore.close();
    }

}