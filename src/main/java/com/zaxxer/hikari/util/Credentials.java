/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zaxxer.hikari.util;

import javax.management.ConstructorParameters;

/**
 * A simple class to hold connection credentials and is designed to be immutable.
 * 连接凭证 不可变
 */
public final class Credentials {

   private final String username;
   private final String password;

   /**
    * Construct an immutable Credentials object with the supplied username and password.
    *
    * @param username the username
    * @param password the password
    * @return a new Credentials object
    */
   public static Credentials of(final String username, final String password) {
      return new Credentials(username, password);
   }

   /**
    * Construct an immutable Credentials object with the supplied username and password.
    *
    * @param username the username
    * @param password the password
    */
   @ConstructorParameters({"username", "password"})
   public Credentials(final String username, final String password) {
      this.username = username;
      this.password = password;
   }

   /**
    * Get the username.
    *
    * @return the username
    */
   public String getUsername() {
      return username;
   }

   /**
    * Get the password.
    *
    * @return the password
    */
   public String getPassword() {
      return password;
   }
}
