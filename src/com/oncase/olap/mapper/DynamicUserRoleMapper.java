/*!
 * This program is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
 * Foundation.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
 * or from the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 * Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
 */

package com.oncase.olap.mapper;

import org.pentaho.platform.api.engine.PentahoAccessControlException;
import org.pentaho.platform.plugin.action.mondrian.mapper.MondrianOneToOneUserRoleListMapper;

import java.util.ArrayList;

public class DynamicUserRoleMapper extends MondrianOneToOneUserRoleListMapper{

  /**
   * This mapper maps is just a simple test to ensure that
   * a schema file without any roles set can receive a role from
   * the mapper and then get it into the DSP to work with.
   * coppied from: MondrianOneToOneUserRoleListMapper.java {@literal @}github
   */
  @Override
  protected String[] mapRoles( String[] mondrianRoles, String[] platformRoles ) throws PentahoAccessControlException {

    ArrayList<String> rtnRoles = new ArrayList<String>();
    rtnRoles.add("Administrator");
    rtnRoles.add("Authenticated");
	System.out.println("---------------------------------------------");
	System.out.println("-- ROLEMAPPER "+rtnRoles.toArray( new String[rtnRoles.size()] )
			.toString() );
	System.out.println("---------------------------------------------");
    return rtnRoles.toArray( new String[rtnRoles.size()] );

  }
  
 
}