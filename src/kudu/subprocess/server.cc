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

#include "kudu/subprocess/server.h"

#include <memory>
#include <string>
#include <vector>

#include <gflags/gflags.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/subprocess/call.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/subprocess/subprocess_protocol.h"
#include "kudu/util/async_util.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

DEFINE_int32(subprocess_request_queue_size_bytes, 4 * 1024 * 1024,
             "Maximum size in bytes of the outbound request queue");
TAG_FLAG(subprocess_request_queue_size_bytes, advanced);

DEFINE_int32(subprocess_response_queue_size_bytes, 4 * 1024 * 1024,
             "Maximum size in bytes of the inbound response queue");
TAG_FLAG(subprocess_response_queue_size_bytes, advanced);

DEFINE_int32(subprocess_num_responder_threads, 3,
             "Number of threads that will be dedicated to reading responses "
             "from the inbound queue and returning to callers");
TAG_FLAG(subprocess_num_responder_threads, advanced);

DEFINE_int32(subprocess_timeout_secs, 15,
             "Number of seconds a call to the subprocess is allowed to "
             "take before an error is returned to the calling process");
TAG_FLAG(subprocess_timeout_secs, runtime);

DEFINE_int32(subprocess_queue_full_retry_ms, 50,
             "Number of milliseconds between attempts to add enqueue the "
             "request to the subprocess");
TAG_FLAG(subprocess_queue_full_retry_ms, runtime);

using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace subprocess {

namespace {
// Get the deadline for the given call.
MonoTime GetDeadline(const shared_ptr<SubprocessCall>& call) {
  return call->start_time() + MonoDelta::FromSeconds(FLAGS_subprocess_timeout_secs);
}
} // anonymous namespace

SubprocessServer::SubprocessServer(vector<string> subprocess_argv)
    : next_id_(1), closing_(false),
      process_(new Subprocess(std::move(subprocess_argv))),
      outbound_call_queue_(FLAGS_subprocess_request_queue_size_bytes),
      response_queue_(FLAGS_subprocess_response_queue_size_bytes) {
  process_->ShareParentStdin(false);
  process_->ShareParentStdout(false);
}

SubprocessServer::~SubprocessServer() {
  Shutdown();
}

Status SubprocessServer::Init() {
  VLOG(2) << "Starting the subprocess";
  RETURN_NOT_OK_PREPEND(process_->Start(), "Failed to start subprocess");

  // Start the message protocol.
  CHECK(!message_protocol_);
  message_protocol_.reset(new SubprocessProtocol(SubprocessProtocol::SerializationMode::PB,
                                                 SubprocessProtocol::CloseMode::CLOSE_ON_DESTROY,
                                                 process_->ReleaseChildStdoutFd(),
                                                 process_->ReleaseChildStdinFd()));
  const int num_threads = FLAGS_subprocess_num_responder_threads;
  responder_threads_.resize(num_threads);
  for (int i = 0; i < num_threads; i++) {
    RETURN_NOT_OK(Thread::Create("subprocess", "responder", &SubprocessServer::ResponderTask,
                                 this, &responder_threads_[i]));
  }
  RETURN_NOT_OK(Thread::Create("subprocess", "reader", &SubprocessServer::ReceiveMessagesTask,
                               this, &read_thread_));
  RETURN_NOT_OK(Thread::Create("subprocess", "writer", &SubprocessServer::SendMessagesTask,
                               this, &write_thread_));
  RETURN_NOT_OK(Thread::Create("subprocess", "deadline-checker",
                               &SubprocessServer::CheckDeadlinesTask,
                               this, &deadline_checker_));
  return Status::OK();
}

Status SubprocessServer::Execute(SubprocessRequestPB* req,
                                 SubprocessResponsePB* resp) {
  DCHECK(!req->has_id());
  req->set_id(next_id_++);
  Synchronizer sync;
  auto cb = sync.AsStdStatusCallback();
  shared_ptr<SubprocessCall> call(new SubprocessCall(req, resp, &cb));
  RETURN_NOT_OK(QueueCall(std::move(call)));
  return sync.Wait();
}

void SubprocessServer::Shutdown() {
  if (closing_ || !process_->IsStarted()) {
    return;
  }
  // Stop further work from happening by killing the subprocess and shutting
  // down the queues.
  closing_.store(true);
  WARN_NOT_OK(process_->KillAndWait(SIGTERM), "failed to stop subprocess");
  response_queue_.Shutdown();
  outbound_call_queue_.Shutdown();

  // Clean up our threads.
  write_thread_->Join();
  read_thread_->Join();
  deadline_checker_->Join();
  for (auto t : responder_threads_) {
    t->Join();
  }

  // Call any of the remaining callbacks.
  std::map<int64_t, shared_ptr<SubprocessCall>> calls;
  {
    std::lock_guard<simple_spinlock> l(call_lock_);
    calls = std::move(call_by_id_);
  }
  for (const auto& id_and_call : calls) {
    const auto& call = id_and_call.second;
    call->RespondError(Status::ServiceUnavailable("subprocess is shutting down"));
  }
}

void SubprocessServer::ReceiveMessagesTask() {
  DCHECK(message_protocol_) << "message protocol is not initialized";
  while (!closing_.load()) {
    // Receive a new response from the subprocess.
    SubprocessResponsePB response;
    Status s = message_protocol_->ReceiveMessage(&response);
    if (s.IsEndOfFile()) {
      // The underlying pipe was closed. We're likely shutting down.
      return;
    }
    WARN_NOT_OK(s, "failed to receive response from the subprocess");
    if (s.ok() && !response_queue_.BlockingPut(std::move(response))) {
      // The queue has been shut down and we should shut down too.
      VLOG(2) << "failed to put response onto inbound queue";
      return;
    }
  }
}

void SubprocessServer::ResponderTask() {
  while (!closing_.load()) {
    SubprocessResponsePB resp;
    if (!response_queue_.BlockingGet(&resp)) {
      VLOG(2) << "get failed, inbound queue shut down";
      return;
    }
    if (!resp.has_id()) {
      LOG(WARNING) << Substitute("Received invalid response: $0",
                                 pb_util::SecureDebugString(resp));
      continue;
    }
    shared_ptr<SubprocessCall> call;
    {
      std::lock_guard<simple_spinlock> l(call_lock_);
      call = EraseKeyReturnValuePtr(&call_by_id_, resp.id());
    }
    if (call) {
      call->RespondSuccess(std::move(resp));
    }
    // If we didn't find our call, it timed out and the its callback has
    // already been called by the deadline checker.
  }
}

void SubprocessServer::CheckDeadlinesTask() {
  while (!closing_.load()) {
    MonoTime now = MonoTime::Now();
    shared_ptr<SubprocessCall> timed_out_call;
    {
      std::lock_guard<simple_spinlock> l(call_lock_);
      if (!call_by_id_.empty()) {
        const auto& id_and_call = call_by_id_.begin();
        const auto& oldest_call = id_and_call->second;
        if (now > GetDeadline(oldest_call)) {
          timed_out_call = oldest_call;
          call_by_id_.erase(id_and_call);
        }
      }
    }
    if (timed_out_call) {
      timed_out_call->RespondError(Status::TimedOut("timed out while in flight"));
    }
  }
}

void SubprocessServer::SendMessagesTask() {
  while (!closing_.load()) {
    shared_ptr<SubprocessCall> call;
    if (!outbound_call_queue_.BlockingGet(&call)) {
      VLOG(2) << "outbound queue shut down";
      return;
    }
    {
      std::lock_guard<simple_spinlock> l(call_lock_);
      EmplaceOrDie(&call_by_id_, call->id(), call);
    }
    // NOTE: it's possible that before sending the request, the call already
    // timed out and the deadline checker already called its callback. If so,
    // the following call will no-op.
    call->SendRequest(message_protocol_.get());
  }
}

Status SubprocessServer::QueueCall(shared_ptr<SubprocessCall> call) {
  if (MonoTime::Now() > GetDeadline(call)) {
    return Status::TimedOut("timed out before queueing call");
  }

  do {
    QueueStatus queue_status = outbound_call_queue_.Put(call);
    switch (queue_status) {
      case QUEUE_SUCCESS:
        return Status::OK();
      case QUEUE_SHUTDOWN:
        return Status::ServiceUnavailable("outbound queue shutting down");
      case QUEUE_FULL: {
        // If we still have more time allotted for this call, wait for a bit
        // and try again; otherwise, time out.
        if (MonoTime::Now() > GetDeadline(call)) {
          return Status::TimedOut("timed out trying to queue call");
        }
        SleepFor(MonoDelta::FromMilliseconds(FLAGS_subprocess_queue_full_retry_ms));
      }
    }
  } while (true);

  return Status::OK();
}

} // namespace subprocess
} // namespace kudu
