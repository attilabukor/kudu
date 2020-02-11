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

#include <memory>
#include <ostream>
#include <string>
#include <unordered_set>
#include <utility>

#include <glog/logging.h>
#include <google/protobuf/any.pb.h>
#include <gtest/gtest.h>

#include "kudu/ranger/ranger.pb.h"
#include "kudu/ranger/ranger_action.h"
#include "kudu/subprocess/server.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace ranger {

using google::protobuf::Any;
using kudu::subprocess::SubprocessRequestPB;
using kudu::subprocess::SubprocessResponsePB;
using kudu::subprocess::SubprocessServer;
using std::make_shared;
using std::shared_ptr;
using std::string;
using std::unordered_set;
using std::vector;

class MockSubprocessServer : public SubprocessServer {
 public:
  shared_ptr<AuthzResponsePB> next_response_;

  Status Init() {
    // don't want to start anything
    return Status::OK();
  }

  ~MockSubprocessServer() {}

  MockSubprocessServer() :
    SubprocessServer({"mock"}),
    next_response_(make_shared<AuthzResponsePB>()) {}

  Status Execute(SubprocessRequestPB* /*req*/,
                 SubprocessResponsePB* resp) {
    auto packed_resp = new Any();
    packed_resp->PackFrom(*next_response_);
    resp->set_allocated_response(packed_resp);

    return Status::OK();
  }
};

class RangerClientTest : public KuduTest {
 public:
  RangerClientTest() :
    server_(make_shared<MockSubprocessServer>()),
    client_(server_) {}

  void SetUp() override {
    server_->next_response_ = make_shared<AuthzResponsePB>();
  }

 protected:
  shared_ptr<MockSubprocessServer> server_;
  RangerClient client_;
};

TEST_F(RangerClientTest, TestAuthorizeCreateTableUnauthorized) {
  auto s = client_.AuthorizeAction("jdoe", Action::CREATE, "bar.baz");
  ASSERT_TRUE(s.IsNotAuthorized());
}

TEST_F(RangerClientTest, TestAuthorizeCreateTableAuthorized) {
  TablePB* table = server_->next_response_->add_table();
  table->set_table("bar");
  table->set_database("foo");
  table->set_default_database(false);
  ASSERT_OK(client_.AuthorizeAction("jdoe", Action::CREATE, "foo.bar"));
}

TEST_F(RangerClientTest, TestAuthorizeListNoTables) {
  unordered_set<string> tables;
  tables.emplace("foo.bar");
  tables.emplace("foo.baz");
  auto s = client_.AuthorizeList("jdoe", &tables);
  ASSERT_TRUE(s.IsNotAuthorized());
}

TEST_F(RangerClientTest, TestAuthorizeMetadataSubsetOfTablesAuthorized) {
  auto table = server_->next_response_->add_table();
  table->set_database("default");
  table->set_table("foobar");
  table->set_default_database(false);
  unordered_set<string> tables;
  tables.emplace("default.foobar");
  tables.emplace("barbaz");
  ASSERT_OK(client_.AuthorizeList("jdoe", &tables));
  ASSERT_EQ(1, tables.size());
  ASSERT_EQ("default.foobar", *tables.begin());
}

TEST_F(RangerClientTest, TestAuthorizeMetadataAllAuthorized) {
  auto resp = server_->next_response_;
  auto table = resp->add_table();
  table->set_database("default");
  table->set_table("foobar");
  table->set_default_database(false);
  table = resp->add_table();
  table->set_database("default");
  table->set_table("barbaz");
  table->set_default_database(true);
  unordered_set<string> tables;
  tables.emplace("default.foobar");
  tables.emplace("barbaz");
  ASSERT_OK(client_.AuthorizeList("jdoe", &tables));
  ASSERT_EQ(2, tables.size());
  ASSERT_TRUE(tables.find("default.foobar") != tables.end());
  ASSERT_TRUE(tables.find("barbaz") != tables.end());
}

TEST_F(RangerClientTest, TestAuthorizeScanSubsetAuthorized) {
  auto resp = server_->next_response_;
  resp->add_column("col1");
  resp->add_column("col3");
  unordered_set<string> columns;
  columns.emplace("col1");
  columns.emplace("col2");
  columns.emplace("col3");
  columns.emplace("col4");
  ASSERT_OK(client_.AuthorizeScan("jdoe", "default.foobar", &columns));
  ASSERT_EQ(2, columns.size());
  ASSERT_TRUE(columns.find("col1") != columns.end());
  ASSERT_TRUE(columns.find("col3") != columns.end());
  ASSERT_TRUE(columns.find("col2") == columns.end());
  ASSERT_TRUE(columns.find("col4") == columns.end());
}

TEST_F(RangerClientTest, TestAuthorizeScanAllColumnsAuthorized) {
  auto resp = server_->next_response_;
  resp->add_column("col1");
  resp->add_column("col2");
  resp->add_column("col3");
  resp->add_column("col4");
  unordered_set<string> columns;
  columns.emplace("col1");
  columns.emplace("col2");
  columns.emplace("col3");
  columns.emplace("col4");
  ASSERT_OK(client_.AuthorizeScan("jdoe", "default.foobar", &columns));
  ASSERT_EQ(4, columns.size());
  ASSERT_TRUE(columns.find("col1") != columns.end());
  ASSERT_TRUE(columns.find("col3") != columns.end());
  ASSERT_TRUE(columns.find("col2") != columns.end());
  ASSERT_TRUE(columns.find("col4") != columns.end());
}

TEST_F(RangerClientTest, TestAuthorizeScanNoColumnsAuthorized) {
  unordered_set<string> columns;
  columns.emplace("col1");
  columns.emplace("col2");
  columns.emplace("col3");
  columns.emplace("col4");
  auto s = client_.AuthorizeScan("jdoe", "default.foobar", &columns);
  ASSERT_TRUE(s.IsNotAuthorized());
}

} // namespace ranger
} // namespace kudu
