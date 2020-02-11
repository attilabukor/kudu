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

#include "kudu/master/ranger_authz_provider.h"

#include <algorithm>
#include <memory>

#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/ranger/ranger_action.h"
#include "kudu/security/token.pb.h"
#include "kudu/subprocess/server.h"
#include "kudu/util/status.h"

namespace kudu {
namespace master {

using kudu::security::ColumnPrivilegePB;
using kudu::security::TablePrivilegePB;
using kudu::ranger::Action;
using kudu::ranger::RangerClient;
using kudu::subprocess::SubprocessServer;
using std::make_shared;
using std::string;
using std::unordered_set;

RangerAuthzProvider::RangerAuthzProvider() :
  client_(make_shared<SubprocessServer>(std::vector<std::string>({"ranger"}))) {}

Status RangerAuthzProvider::Start() {
  RETURN_NOT_OK(client_.Start());

  return Status::OK();
}

void RangerAuthzProvider::Stop() {
}

Status RangerAuthzProvider::ResetCache() {
  return Status::OK();
}

Status RangerAuthzProvider::AuthorizeCreateTable(const string& table_name,
                                                 const string& user,
                                                 const string& /*owner*/) {
  if (IsTrustedUser(user)) {
    return Status::OK();
  }
  return client_.AuthorizeAction(user, Action::CREATE, table_name);
}

Status RangerAuthzProvider::AuthorizeDropTable(const string& table_name,
                                               const string& user) {
  if (IsTrustedUser(user)) {
    return Status::OK();
  }
  return client_.AuthorizeAction(user, Action::DROP, table_name);
}

Status RangerAuthzProvider::AuthorizeAlterTable(const string& old_table,
                                                const string& new_table,
                                                const string& user) {
  if (IsTrustedUser(user)) {
    return Status::OK();
  }
  if (old_table == new_table) {
    return client_.AuthorizeAction(user, Action::ALTER, old_table);
  }

  RETURN_NOT_OK(client_.AuthorizeAction(user, Action::ALL, old_table));
  return client_.AuthorizeAction(user, Action::CREATE, new_table);
}

Status RangerAuthzProvider::AuthorizeGetTableMetadata(const string& table_name,
                                                      const string& user) {
  if (IsTrustedUser(user)) {
    return Status::OK();
  }
  return client_.AuthorizeAction(user, Action::METADATA, table_name);
}

Status RangerAuthzProvider::AuthorizeListTables(const string& user,
                                                unordered_set<string>* table_names,
                                                bool* checked_table_names) {
  if (IsTrustedUser(user)) {
    *checked_table_names = false;
    return Status::OK();
  }

  *checked_table_names = true;
  return client_.AuthorizeList(user, table_names);
}

Status RangerAuthzProvider::AuthorizeGetTableStatistics(const string& table_name,
                                                        const string& user) {
  if (IsTrustedUser(user)) {
    return Status::OK();
  }
  return client_.AuthorizeAction(user, Action::SCAN, table_name);
}

Status RangerAuthzProvider::FillTablePrivilegePB(const string& table_name,
                                                 const string& user,
                                                 const SchemaPB& schema_pb,
                                                 TablePrivilegePB* pb) {
  DCHECK(pb);
  DCHECK(pb->has_table_id());
  if (IsTrustedUser(user) || client_.AuthorizeAction(user, Action::ALL, table_name).ok()) {
    pb->set_delete_privilege(true);
    pb->set_insert_privilege(true);
    pb->set_scan_privilege(true);
    pb->set_update_privilege(true);
    return Status::OK();
  }

  if (client_.AuthorizeAction(user, Action::DELETE, table_name).ok()) {
    pb->set_delete_privilege(true);
  }
  if (client_.AuthorizeAction(user, Action::INSERT, table_name).ok()) {
    pb->set_insert_privilege(true);
  }
  if (client_.AuthorizeAction(user, Action::UPDATE, table_name).ok()) {
    pb->set_update_privilege(true);
  }

  if (client_.AuthorizeAction(user, Action::SCAN, table_name).ok()) {
    pb->set_scan_privilege(true);
  } else {
    static ColumnPrivilegePB scan_col_privilege;
    scan_col_privilege.set_scan_privilege(true);

    unordered_set<string> column_names;
    for (const auto& col : schema_pb.columns()) {
      column_names.emplace(col.name());
    }
    RETURN_NOT_OK(client_.AuthorizeScan(user, table_name, &column_names));
    for (const auto& col : schema_pb.columns()) {
      if (ContainsKey(column_names, col.name())) {
        InsertIfNotPresent(pb->mutable_column_privileges(), col.id(), scan_col_privilege);
      }
    }
  }

  return Status::OK();
}

bool RangerAuthzProvider::IsEnabled() {
  return RangerClient::IsEnabled();
}

} // namespace master
} // namespace kudu
