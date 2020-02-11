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

#include "kudu/ranger/ranger_client.h"

#include <string>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/any.pb.h>

#include "kudu/common/table_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/ranger/ranger.pb.h"
#include "kudu/ranger/ranger_action.h"
#include "kudu/subprocess/server.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

DEFINE_string(ranger_policy_server, "",
              "Address of the Ranger policy server. Setting this enables "
              "Ranger authorization. sentry_service_rpc_addresses must not be "
              "set if this is set.");

using google::protobuf::Any;
using kudu::subprocess::SubprocessResponsePB;
using kudu::subprocess::SubprocessRequestPB;
using kudu::subprocess::SubprocessServer;
using std::make_shared;
using std::move;
using std::shared_ptr;
using std::string;
using std::unordered_set;
using strings::Substitute;

namespace kudu {
namespace ranger {

RangerClient::RangerClient(const std::shared_ptr<SubprocessServer>& server) :
  server_(move(server)) {}

Status RangerClient::Start() {
  return server_->Init();
}

bool RangerClient::IsEnabled() {
  return !FLAGS_ranger_policy_server.empty();
}

Status RangerClient::SendRequest(AuthzRequestPB* req, AuthzResponsePB* resp) {
  SubprocessRequestPB sreq;
  SubprocessResponsePB sresp;

  auto packed_req = new Any();
  packed_req->PackFrom(*req);
  sreq.set_allocated_request(packed_req);
  RETURN_NOT_OK(server_->Execute(&sreq, &sresp));

  sresp.response().UnpackTo(resp);

  return Status::OK();
}

Status RangerClient::AddTable(AuthzRequestPB* req, const string& table_name) {
    string db;
    Slice tbl;
    bool default_database;
    RETURN_NOT_OK(ParseRangerTableIdentifier(table_name, &db, &tbl, &default_database));

    TablePB* table = req->add_table();
    table->set_database(db);
    table->set_table(tbl.ToString());
    table->set_default_database(default_database);

    return Status::OK();
}

void RangerClient::AddTable(unordered_set<string>* table_names, const TablePB& table) {
    DCHECK(table.has_table());

    string database_name;
    if (!table.default_database()) {
      database_name = table.database() + '.';
    }

    table_names->emplace(database_name + table.table());
}

Status RangerClient::AuthorizeAction(const string& user_name,
                                     const Action& action,
                                     const string& table_name) {
  AuthzRequestPB req;
  AuthzResponsePB resp;

  req.set_action(ActionToActionPB(action));
  req.set_user(user_name);
  RETURN_NOT_OK(AddTable(&req, table_name));

  RETURN_NOT_OK(SendRequest(&req, &resp));

  if (resp.table_size() == 1) {
    return Status::OK();
  }

  return Status::NotAuthorized(Substitute("User %s is not authorized to perform %s on %s",
                                          user_name, ActionToString(action), table_name));
}

Status RangerClient::AuthorizeScan(const string& user_name,
                                   const string& table_name,
                                   unordered_set<string>* column_names) {
  DCHECK(!column_names->empty());
  AuthzRequestPB req;
  AuthzResponsePB resp;

  req.set_action(ActionToActionPB(Action::SCAN));
  req.set_user(user_name);
  RETURN_NOT_OK(AddTable(&req, table_name));
  for (const auto& column_name : *column_names) {
    req.add_column(column_name);
  }

  RETURN_NOT_OK(SendRequest(&req, &resp));

  if (resp.table_size() == 1) {
    return Status::OK();
  }

  if (resp.column_size() == 0) {
    return Status::NotAuthorized(Substitute("User %s is not authorized to "
                                            "perform SCAN on table %s",
                                            user_name, table_name));
  }

  column_names->clear();

  for (auto i = 0; i < resp.column_size(); ++i) {
    column_names->emplace(resp.column(i));
  }

  return Status::OK();
}

Status RangerClient::AuthorizeList(const string& user_name,
                                   unordered_set<string>* table_names) {
  if (table_names->empty()) {
    return Status::InvalidArgument("Empty set of tables");
  }

  AuthzRequestPB req;
  AuthzResponsePB resp;

  req.set_action(ActionToActionPB(Action::METADATA));
  req.set_user(user_name);
  for (const auto& table_name : *table_names) {
    RETURN_NOT_OK(AddTable(&req, table_name));
  }

  RETURN_NOT_OK(SendRequest(&req, &resp));

  if (resp.table_size() == 0) {
    return Status::NotAuthorized(Substitute("User %s is not authorized to "
                                            "perform METADATA on %d tables.",
                                            user_name, table_names->size()));
  }

  table_names->clear();

  for (auto i = 0; i < resp.table_size(); ++i) {
    AddTable(table_names, resp.table(i));
  }

  return Status::OK();
}

} // namespace ranger
} // namespace kudu
