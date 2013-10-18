package ohmdb.replication;

import ohmdb.ConfigDirectory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A small persister that writes files to a directory.
*/
class Persister implements RaftInfoPersistence {

    private ConfigDirectory configDirectory;

    Persister(ConfigDirectory configDirectory) {
        this.configDirectory = configDirectory;
    }

    @Override
    public long readCurrentTerm(String quorumId) throws IOException {
        return getLongofFile(quorumId, 0);
    }

    @Override
    public long readVotedFor(String quorumId) throws IOException {
        return getLongofFile(quorumId, 1);
    }

    private long getLongofFile(String quorumId, int whichLine) throws IOException {
        List<String> datas = configDirectory.readFile(quorumId, "raft-data");
        if (datas.size() != 2)
            return 0; // corrupt file?

        try {
            return Long.parseLong(datas.get(whichLine));
        } catch (NumberFormatException e) {
            return 0; // corrupt file sucks?
        }
    }

    @Override
    public void writeCurrentTermAndVotedFor(String quorumId, long currentTerm, long votedFor) throws IOException {
        List<String> datas = new ArrayList<>(2);
        datas.add(Long.toString(currentTerm));
        datas.add(Long.toString(votedFor));
        configDirectory.writeFile(quorumId, "raft-data", datas);
    }
}
