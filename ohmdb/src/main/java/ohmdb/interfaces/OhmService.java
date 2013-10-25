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
package ohmdb.interfaces;

import com.google.common.util.concurrent.Service;
import ohmdb.messages.ControlMessages;

/**
 * An internal service.  Extends guava services.
 * TODO service dependencies so if you stop one service, you have to stop the dependents.
 */
public interface OhmService extends Service {

    public ControlMessages.ServiceType getServiceType();
    public boolean hasPort();
    public int port();

    // TODO formalize dependencies for automated start order, etc etc
    //public List<ControlMessages.ServiceType> getDependencies();

}
