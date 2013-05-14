/*
 * Copyright (C) 2013  Ohm Data
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
package ohmdb.election;

import org.xtreemfs.foundation.flease.MasterEpochHandlerInterface;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;

public class RamMasterEpochHandler implements MasterEpochHandlerInterface {
    volatile long masterEpoch;

    public RamMasterEpochHandler() {
        this(0);
    }

    public RamMasterEpochHandler(long masterEpoch) {
        this.masterEpoch = masterEpoch;
    }

    @Override
    public void sendMasterEpoch(FleaseMessage response, Continuation callback) {
        response.setMasterEpochNumber(masterEpoch);
        callback.processingFinished();
    }

    @Override
    public void storeMasterEpoch(FleaseMessage request, Continuation callback) {
        masterEpoch = request.getMasterEpochNumber();
        callback.processingFinished();
    }
}
