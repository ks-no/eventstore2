package no.ks.eventstore2;

import java.io.Serializable;

public class TakeBackup implements Serializable {

    private String backupdir;

    public TakeBackup() {
    }

    public TakeBackup(String backupdir) {
        this.backupdir = backupdir;
    }

    public String getBackupdir() {
        return backupdir;
    }

    public void setBackupdir(String backupdir) {
        this.backupdir = backupdir;
    }

    @Override
    public String toString() {
        return "Take backup to dir " + backupdir;
    }
}
