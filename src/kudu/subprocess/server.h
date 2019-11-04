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

#include <atomic>
#include <deque>
#include <memory>
#include <string>
#include <thread>
#include <map>
#include <utility>
#include <vector>

#include "kudu/gutil/callback.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/subprocess/call.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/subprocess/subprocess_protocol.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/locks.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/thread.h"

namespace kudu {
namespace subprocess {

class SubprocessCall;

// Used by BlockingQueue to determine the size of messages.
struct RequestLogicalSize {
  static size_t logical_size(const std::shared_ptr<SubprocessCall>& call) {
    return call->req_->ByteSizeLong();
  }
};
struct ResponseLogicalSize {
  static size_t logical_size(const SubprocessResponsePB& response) {
    return response.ByteSizeLong();
  }
};

typedef BlockingQueue<std::shared_ptr<SubprocessCall>, RequestLogicalSize> SubprocessCallQueue;
typedef BlockingQueue<SubprocessResponsePB, ResponseLogicalSize> ResponseQueue;

// Wrapper for a subprocess that communicates via protobuf. A server is
// comprised of a few things to facilitate concurrent communication with an
// underlying subprocess:
//
// - An outbound queue of SubprocessCalls to send to the subprocess. When a
//   user enqueues a call, that call is first added to the outbound queue.
//
// - One "writer" thread: this thread pulls work off of the outbound queue and
//   writes it to the subprocess pipe. When a SubprocessCall's request is
//   written to the pipe, the call is tracked, and its callback may be called
//   at any time by the deadline checker or upon receiving a valid response.
//
// - One "reader" thread: this thread reads messages from subprocess pipe and
//   puts it on the inbound response queue.
//
// - An inbound queue of SubprocessResponsePBs that is populated by the reader
//   thread.
//
// - Many "responder" threads: each thread looks for a response on the inbound
//   queue and calls the appropriate callback for it, based on the response ID.
//
// - One "deadline-checker" thread: this thread looks through the oldest calls
//   that have been sent to the subprocess and runs their callbacks with a
//   TimedOut error if they are past their deadline.
class SubprocessServer {
 public:
  SubprocessServer(std::vector<std::string> subprocess_argv);
  ~SubprocessServer();

  // Initialize the server, starting the subprocess and worker threads.
  Status Init() WARN_UNUSED_RESULT;

  // Synchronously send a request to the subprocess and populate 'resp' with
  // contents returned from the subprocess, or return an error if anything
  // failed or timed out along the way.
  Status Execute(SubprocessRequestPB* req, SubprocessResponsePB* resp) WARN_UNUSED_RESULT;

 private:
  FRIEND_TEST(SubprocessServerTest, TestCallsReturnWhenShuttingDown);

  // Stop the subprocess and stop processing messages.
  void Shutdown();

  // Add the call to the outbound queue, returning an error if the call timed
  // out before successfully adding it to the queue, or if the queue is shut
  // down.
  //
  // The call's callback is run asynchronously upon receiving a response from
  // the subprocess, matched by ID, or when the deadline checker thread detects
  // that the call has timed out.
  Status QueueCall(std::shared_ptr<SubprocessCall> call) WARN_UNUSED_RESULT;

  // Long running task that repeatedly looks at the in-flight call with the
  // lowest ID, checks whether its deadline has expired, and runs its callback
  // with a TimedOut error if so.
  void CheckDeadlinesTask();

  // Pulls responses from the inbound response queue and calls the associated
  // callbacks.
  void ResponderTask();

  // Pulls enqueued calls from the outbound request queue and sends their
  // associated requests to the subprocess.
  void SendMessagesTask();

  // Receives messages from the subprocess and puts the responses onto the
  // inbound response queue.
  void ReceiveMessagesTask();

  // Next request ID to be assigned.
  std::atomic<int64_t> next_id_;

  // Set to true if the server is shutting down.
  std::atomic<bool> closing_;

  // The underlying subprocess.
  std::shared_ptr<Subprocess> process_;

  // Protocol with which to send and receive bytes to and from 'process_'.
  std::shared_ptr<SubprocessProtocol> message_protocol_;

  // Pulls requests off the request queue and serializes them to via the
  // message protocol.
  scoped_refptr<Thread> write_thread_;

  // Reads from the message protocol, constructs the response, and puts it on
  // the response queue.
  scoped_refptr<Thread> read_thread_;

  // Looks at the front of the queue for calls that are past their deadlines
  // and triggers their callbacks.
  scoped_refptr<Thread> deadline_checker_;

  // Pull work off the response queue and trigger the associated callbacks if
  // appropriate.
  std::vector<scoped_refptr<Thread>> responder_threads_;

  // Outbound queue of calls to send to the subprocess.
  SubprocessCallQueue outbound_call_queue_;

  // Inbound queue of responses sent by the subprocess.
  ResponseQueue response_queue_;

  std::atomic<int64_t> overflow_timeout_;

  // Calls that are currently in-flight (the requests are being sent over the
  // pipe or waiting for a response), ordered by ID. This ordering allows for
  // lookup by ID, and gives us a rough way to get the calls with earliest
  // start times which is useful for deadline checking.
  //
  // Only a single thread may remove a given call; that thread must run the
  // call's callback.
  mutable simple_spinlock call_lock_;
  std::map<int64_t, std::shared_ptr<SubprocessCall>> call_by_id_;
};

} // namespace subprocess
} // namespace kudu
