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
package c5db.replication;

/**
 *
 */
public interface RaftInformationInterface {
    public long currentTimeMillis();

    /**
     * How often to check if the election needs to be rerun.
     *
     * TODO revisit this to see if necessary or can be set to another derivative value.
     * @return
     */
    public long electionCheckRate();

    /**
     * The election timeout (straight from the paper).
     * @return
     */
    public long electionTimeout();

    /**
     * How frequently we should check the append queue, and send RPCs to the clients.
     * @return
     */
    public long groupCommitDelay();
}
