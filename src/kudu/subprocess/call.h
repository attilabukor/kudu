// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
#pragma once

#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/subprocess/subprocess_protocol.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status_callback.h"

namespace kudu {
namespace subprocess {

class SubprocessRequestPB;
class SubprocessResponsePB;
struct RequestLogicalSize;

// Encapsulates the pending state of a request that is in the process of being
// sent to a subprocess.
class SubprocessCall {
 public:
  SubprocessCall(const SubprocessRequestPB* req,
                 SubprocessResponsePB* resp,
                 StdStatusCallback* cb)
      : id_(req->id()),
        start_time_(MonoTime::Now()),
        req_(req), resp_(resp), cb_(cb) {}

  int64_t id() const {
    return id_;
  }

  const MonoTime& start_time() const {
    return start_time_;
  }

  // Sends the request with the given message protocol, ensuring that the
  // sending of the request doesn't overlap with the calling of the callback
  // (which may, in turn, delete the request state). If the request couldn't be
  // sent, runs the callback with an appropriate error.
  void SendRequest(SubprocessProtocol* message_protocol) {
    std::lock_guard<simple_spinlock> l(lock_);

    // If we've already run the callback, we shouldn't try sending the message;
    // the request and response may have been destructed.
    if (!cb_) {
      return;
    }
    Status s = message_protocol->SendMessage(*req_);
    // If we failed to send the message, return the error to the caller.
    if (PREDICT_FALSE(!s.ok())) {
      (*cb_)(s);
      cb_ = nullptr;
    }
  }

  void RespondSuccess(SubprocessResponsePB&& resp) {
    std::lock_guard<simple_spinlock> l(lock_);
    // If we've already run the callback, there's nothing to do.
    if (!cb_) {
      return;
    }
    *resp_ = std::move(resp);
    (*cb_)(Status::OK());
    cb_ = nullptr;
  }

  void RespondError(const Status& s) {
    DCHECK(!s.ok());
    std::lock_guard<simple_spinlock> l(lock_);
    // If we've already run the callback, there's nothing to do.
    if (!cb_) {
      return;
    }
    (*cb_)(s);
    cb_ = nullptr;
  }

 private:
  friend struct RequestLogicalSize;

  // Lock used to ensure that the sending of the request doesn't intersect with
  // the calling of the callback. This is important because the callback may
  // destroy the request.
  mutable simple_spinlock lock_;

  // ID of this call.
  const int64_t id_;

  // Time at which this call was constructed.
  const MonoTime start_time_;

  // Request and response associated with this call.
  const SubprocessRequestPB* req_;
  SubprocessResponsePB* resp_;

  // Callback to wake up the caller that enqueued this call. This is called
  // exactly once per SubprocessCall.
  StdStatusCallback* cb_;
};

} // namespace subprocess
} // namespace kudu
