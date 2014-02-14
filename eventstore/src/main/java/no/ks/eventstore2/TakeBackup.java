package no.ks.eventstore2;

import java.io.File;
import java.io.Serializable;

public class TakeBackup implements Serializable {

    private String backupdir;
    private String backupfilename;

    public TakeBackup() {
    }

    public TakeBackup(String backupdir, String backupfilename) {
        this.backupdir = backupdir;
        this.backupfilename = backupfilename;
    }

    public String getBackupfilename() {
        return backupfilename;
    }

    public void setBackupfilename(String backupfilename) {
        this.backupfilename = backupfilename;
    }

    public String getBackupdir() {
        return backupdir;
    }

    public void setBackupdir(String backupdir) {
        this.backupdir = backupdir;
    }

    @Override
    public String toString() {
        return backupdir + File.separator + backupfilename;
    }
}
