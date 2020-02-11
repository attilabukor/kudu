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

#include "kudu/ranger/ranger_action.h"

#include <string>

#include "kudu/ranger/ranger.pb.h"

namespace kudu {
namespace ranger {

ActionPB ActionToActionPB(const Action& action) {
  switch (action) {
    case Action::SCAN:
      return ActionPB::SCAN;
    case Action::INSERT:
      return ActionPB::INSERT;
    case Action::UPDATE:
      return ActionPB::UPDATE;
    case Action::DELETE:
      return ActionPB::DELETE;
    case Action::ALTER:
      return ActionPB::ALTER;
    case Action::CREATE:
      return ActionPB::CREATE;
    case Action::DROP:
      return ActionPB::DROP;
    case Action::ALL:
      return ActionPB::ALL;
    case Action::METADATA:
      return ActionPB::METADATA;
  }
}

std::string ActionToString(const Action& action) {
  switch (action) {
    case Action::ALL:
      return "ALL";
    case Action::ALTER:
      return "ALTER";
    case Action::CREATE:
      return "CREATE";
    case Action::DELETE:
      return "DELETE";
    case Action::DROP:
      return "DROP";
    case Action::INSERT:
      return "INSERT";
    case Action::METADATA:
      return "METADATA";
    case Action::SCAN:
      return "SCAN";
    case Action::UPDATE:
      return "UPDATE";
  }
}

} // namespace ranger
} // namespace kudu
