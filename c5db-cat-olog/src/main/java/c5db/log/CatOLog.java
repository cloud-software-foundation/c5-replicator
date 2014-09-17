/*
 * Copyright (C) 2014  Ohm Data
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package c5db.log;

import c5db.generated.OLogHeader;
import c5db.interfaces.log.SequentialEntryCodec;
import c5db.replication.generated.QuorumConfigurationMessage;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.channels.Channels;
import java.util.Formatter;
import java.util.List;
import java.util.Locale;

import static c5db.log.EntryEncodingUtil.decodeAndCheckCrc;
import static c5db.log.LogPersistenceService.BytePersistence;
import static c5db.log.LogPersistenceService.PersistenceReader;

public class CatOLog {
  // TODO replace formatting constants with command line parameters
  private static final int HEX_ADDRESS_DIGITS = 8;
  private static final int LONG_DIGITS = 8;
  private static final int INT_DIGITS = 8;

  /**
   * Output to System.out the contents of an OLog file, with one entry on each line.
   *
   * @param args Accepts only one argument, the name of the log file.
   * @throws IOException
   */
  public static void main(String args[]) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: CatOLog filename");
      System.exit(-1);
    }

    File inputLogFile = new File(args[0]);

    if (!inputLogFile.exists() || inputLogFile.isDirectory()) {
      System.err.println("File does not exist, or is a directory");
      System.exit(-1);
    }

    describeLogFileToOutput(inputLogFile, System.out);
  }

  private static void describeLogFileToOutput(File inputLogFile, PrintStream out) throws IOException {
    openFileAndParseEntries(inputLogFile,
        (header, validCrc) ->
            out.println(formatLogHeader(header, validCrc)),
        (address, entry) -> {
          out.print(toHex(address) + ": ");
          out.println(formatEntry(entry));
        });
  }

  private static final SequentialEntryCodec<OLogEntryDescription> CODEC = new OLogEntryDescription.Codec();

  private static void openFileAndParseEntries(File inputLogFile,
                                              HeaderWithCrcValidity doWithHeader,
                                              EntryWithAddress doForEach) throws IOException {
    try (BytePersistence persistence = new FilePersistence(inputLogFile.toPath());
         PersistenceReader reader = persistence.getReader();
         InputStream inputStream = Channels.newInputStream(reader)) {

      decodeAndUseLogHeader(inputStream, doWithHeader);

      //noinspection InfiniteLoopStatement
      do {
        long address = reader.position();
        OLogEntryDescription entry = CODEC.decode(inputStream);
        doForEach.accept(address, entry);
      } while (true);
    } catch (EOFException ignore) {
    }
  }

  private interface EntryWithAddress {
    void accept(long address, OLogEntryDescription entry);
  }

  private interface HeaderWithCrcValidity {
    void accept(OLogHeader header, boolean validCrc);
  }

  private static String toHex(long address) {
    return String.join(" ",
        Splitter
            .fixedLength(4)
            .split(String.format("%0" + HEX_ADDRESS_DIGITS + "x", address)));
  }

  private static String formatLogHeader(OLogHeader header, boolean validCrc) {
    StringBuilder sb = new StringBuilder();
    Formatter formatter = new Formatter(sb, Locale.US);

    formatter.format("HEADER [base term: %" + LONG_DIGITS + "d]", header.getBaseTerm());
    formatter.format(" [base seq: %" + LONG_DIGITS + "d]", header.getBaseSeqNum());
    formatter.format(" [base config: ");
    formatConfiguration(formatter, header.getBaseConfiguration());
    formatter.format("]");

    if (!validCrc) {
      formatter.format(" <invalid log header CRC>");
    }

    return formatter.toString();
  }

  private static String formatEntry(OLogEntryDescription entry) {
    StringBuilder sb = new StringBuilder();
    Formatter formatter = new Formatter(sb, Locale.US);

    formatter.format(" [term: %" + LONG_DIGITS + "d]", entry.getElectionTerm());
    formatter.format(" [seq: %" + LONG_DIGITS + "d]", entry.getSeqNum());

    formatContent(formatter, entry);

    if (!entry.isHeaderCrcValid()) {
      formatter.format(" <invalid header CRC>");
    }

    if (!entry.isContentCrcValid()) {
      formatter.format(" <invalid content CRC>");
    }

    return formatter.toString();
  }

  private static void formatContent(Formatter formatter, OLogEntryDescription entry) {
    switch (entry.getType()) {
      case QUORUM_CONFIGURATION:
        formatter.format(" [quorum configuration: ");
        formatConfiguration(formatter, entry.getQuorumConfiguration().toProtostuff());
        formatter.format("]");
        break;
      case DATA:
        formatter.format(" [content length: %" + INT_DIGITS + "d]", entry.getContentLength());
        break;
      default:
        throw new AssertionError("Unhandled enum value in CatOLog#formatContent");
    }
  }

  private static void formatConfiguration(Formatter formatter, QuorumConfigurationMessage message) {
    formatter.format("(");
    if (message.getTransitional()) {
      formatPeerIdList(formatter, message.getPrevPeersList());
      formatter.format(" -> ");
      formatPeerIdList(formatter, message.getNextPeersList());
    } else {
      formatPeerIdList(formatter, message.getAllPeersList());
    }
    formatter.format(")");
  }

  private static void formatPeerIdList(Formatter formatter, List<Long> peerIdList) {
    Joiner joiner = Joiner.on(", ");
    formatter.format(joiner.join(peerIdList));
  }

  private static void decodeAndUseLogHeader(InputStream inputStream, HeaderWithCrcValidity doWithHeader)
      throws IOException {
    OLogHeader header;
    boolean validCrc = true;
    try {
      header = decodeAndCheckCrc(inputStream, OLogHeader.getSchema());
    } catch (EntryEncodingUtil.CrcError e) {
      validCrc = false;
      header = OLogHeader.getSchema().newMessage();
    }

    doWithHeader.accept(header, validCrc);
  }
}
