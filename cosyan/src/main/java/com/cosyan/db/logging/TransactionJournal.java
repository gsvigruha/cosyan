package com.cosyan.db.logging;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.CRC32;

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.cosyan.db.conf.Config;
import com.cosyan.db.logging.MetaJournal.DBException;

public class TransactionJournal {

  private final Config config;
  private FileOutputStream stream = null;

  private final ByteArrayOutputStream bos = new ByteArrayOutputStream(13);
  private final DataOutputStream dos = new DataOutputStream(bos);

  private static final byte START = 1;
  private static final byte SUCCESS = 2;
  private static final byte USER_ERROR = 3;

  private static final byte IO_READ_ERROR = 4;
  private static final byte IO_WRITE_ERROR = 5;

  private static final byte CRASH = 6;

  private static final byte CHECKPOINT = 7;

  public TransactionJournal(Config config) throws IOException {
    this.config = config;
    Files.createDirectories(Paths.get(config.journalDir()));
  }

  private synchronized void log(byte event, long trxNumber) throws DBException {
    try {
      if (stream == null || !stream.getChannel().isOpen()) {
        this.stream = new FileOutputStream(
            config.journalDir() + File.separator + "transaction.journal",
            /* append= */true);
      }
      bos.reset();
      CRC32 checksum = new CRC32();
      dos.write(event);
      dos.writeLong(trxNumber);
      byte[] b = bos.toByteArray();
      checksum.update(b);
      dos.writeInt((int) checksum.getValue());
      stream.write(bos.toByteArray());
      stream.flush();
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  public synchronized void close() throws IOException {
    stream.close();
  }

  public void start(long trxNumber) throws DBException {
    log(START, trxNumber);
  }

  public void success(long trxNumber) throws DBException {
    log(SUCCESS, trxNumber);
  }

  public void userError(long trxNumber) throws DBException {
    log(USER_ERROR, trxNumber);
  }

  public void ioReadError(long trxNumber) throws DBException {
    log(IO_READ_ERROR, trxNumber);
  }

  public void ioWriteError(long trxNumber) throws DBException {
    log(IO_WRITE_ERROR, trxNumber);
  }

  public void crash(long trxNumber) throws DBException {
    log(CRASH, trxNumber);
  }

  public void checkpoint(long trxNumber) throws DBException {
    log(CHECKPOINT, trxNumber);
  }
}
