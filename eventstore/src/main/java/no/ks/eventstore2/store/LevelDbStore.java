package no.ks.eventstore2.store;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

public class LevelDbStore {

    public static final Logger log = LoggerFactory.getLogger(LevelDbStore.class);
    private final String directory;
    private final int cacheSizeInMB;


    private DB db;

    public LevelDbStore(String directory, int cacheSizeInMB) {
        this.directory = directory;
        this.cacheSizeInMB = cacheSizeInMB;
    }

    public void open() {
        if (db == null) {
            Options options = new Options();
            options.cacheSize(cacheSizeInMB * 1048576L); // MB cache
            options.createIfMissing(true);
            if(!new File(directory).exists()) {
            	new File(directory).mkdirs();
            }
            try {
                db = factory.open(new File(directory), options);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void printDB() throws IOException {
        DBIterator iterator = db.iterator();
        try {
            for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                String key = asString(iterator.peekNext().getKey());
                String value = asString(iterator.peekNext().getValue());
                java.lang.System.out.println(key + " = " + value);
            }
        } finally {
            // Make sure you close the iterator to avoid resource leaks.
            iterator.close();
        }
    }

    public DB getDb(){
        return db;
    }

    public void doBackup(String backupDir, String filename) {
        try {
            close();
            new File(backupDir).mkdirs();
            String backupfile = backupDir + File.separator + filename + ".zip";
            ZipFile zipFile = new ZipFile(backupfile);
            zipFile.addFolder(directory,new ZipParameters());
            log.info("Backup of leveldb {} done to file {}" ,directory, backupfile);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            open();
        }

    }

    public void close() {
        try {
            if(db != null){
                db.close();
                db = null;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
