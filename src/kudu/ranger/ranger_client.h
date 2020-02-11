// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>
#include <string>
#include <unordered_set>

#include "kudu/gutil/port.h"
#include "kudu/ranger/ranger_action.h"
#include "kudu/subprocess/server.h"
#include "kudu/util/status.h"

namespace kudu {
namespace ranger {

// A client for the Ranger service that communicates with a Java subprocess.
class RangerClient {
 public:
  // Create a RangerClient instance with the subprocess server provided.
  explicit RangerClient(const std::shared_ptr<kudu::subprocess::SubprocessServer>& server);

  // Starts the RangerClient, initializes the subprocess server and client.
  Status Start() WARN_UNUSED_RESULT;

  // Returns true if Ranger integration is enabled.
  static bool IsEnabled();

  // Authorizes listing tables. If there is at least one table that user is
  // authorized to access metadata of, it returns OK and sets 'table_names' to
  // the tables the user is authorized to list. Otherwise it returns
  // NotAuthorized and it doesn't modify 'table_names'.
  Status AuthorizeList(const std::string& user_name,
                       std::unordered_set<std::string>* table_names) WARN_UNUSED_RESULT;

  // Authorizes an action on the table. Returns OK if 'user_name' is authorized
  // to perform 'action' on 'table_name', NotAuthorized otherwise.
  Status AuthorizeAction(const std::string& user_name, const Action& action,
                         const std::string& table_name) WARN_UNUSED_RESULT;

  // Authorizes a scan on a table. Returns OK if 'user_name' is authorized to
  // scan the whole table or at least one of the specified columns,
  // NotAuthorized otherwise. If the user isn't authorized to scan the whole
  // table, 'column_names' is changed to contain only the columns the user is
  // authorized to scan.
  Status AuthorizeScan(const std::string& user_name, const std::string& table_name,
                       std::unordered_set<std::string>* column_names) WARN_UNUSED_RESULT;

 private:
  Status SendRequest(AuthzRequestPB* req, AuthzResponsePB* resp) WARN_UNUSED_RESULT;

  static Status AddTable(AuthzRequestPB* req, const std::string& table_name) WARN_UNUSED_RESULT;

  static void AddTable(std::unordered_set<std::string>* table_names, const TablePB& table);

  std::shared_ptr<kudu::subprocess::SubprocessServer> server_;
};

} // namespace ranger
} // namespace kudu
