package cws.core;

// FIXME(mequrel) temporary class, created to avoid changing all VM constructors in order to pass Storage Manager reference

import cws.core.storage.StorageManager;

public class ResourceLocator {
    private static StorageManager storageManager;

    public static StorageManager getStorageManager() {
        return storageManager;
    }

    public static void setStorageManager(StorageManager storageManager) {
        ResourceLocator.storageManager = storageManager;
    }
}
