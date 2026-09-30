/*
 * Copyright (c) 2026 Apple Inc. All rights reserved.
 *
 * @APPLE_APACHE_LICENSE_HEADER_START@
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @APPLE_APACHE_LICENSE_HEADER_END@
 */

// Read and write sources observe the peer closing its end of a pipe or
// socket: every source receives the EOF exactly once and can be cancelled,
// including when many sources are set up and torn down concurrently.

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>

#include <dispatch/dispatch.h>

#include "dispatch_test.h"
#include <bsdtests.h>

#define PIPE_THREADS 8
#define PIPE_ITERATIONS_PER_THREAD 20000
#define PIPE_MAX_IN_FLIGHT 256

static atomic_long pipe_eofs;
static atomic_long pipe_cancels;
static atomic_long pipe_bad_eofs;
static dispatch_group_t pipe_group;
static dispatch_semaphore_t pipe_in_flight;

static void
set_nonblocking(int fd)
{
	int flags = fcntl(fd, F_GETFL);
	if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
		test_errno("fcntl", errno, 0);
		test_stop();
	}
}

// Reads everything available on fd. Returns true once the end of the stream
// is reached. A socket whose peer closed with unread data reports
// ECONNRESET rather than EOF.
static bool
drain_until_eof(int fd)
{
	char buf[64];
	for (;;) {
		ssize_t n = read(fd, buf, sizeof(buf));
		if (n == 0) {
			return true;
		}
		if (n < 0) {
			if (errno == EINTR) continue;
			if (errno == ECONNRESET) return true;
			if (errno != EAGAIN) {
				test_errno("read", errno, 0);
			}
			return false;
		}
	}
}

static void
pipe_iteration(void)
{
	int fds[2];
	if (pipe(fds) < 0) {
		test_errno("pipe", errno, 0);
		test_stop();
	}
	int rfd = fds[0], wfd = fds[1];
	set_nonblocking(rfd);

	dispatch_group_enter(pipe_group);
	dispatch_source_t ds = dispatch_source_create(DISPATCH_SOURCE_TYPE_READ,
			(uintptr_t)rfd, 0, dispatch_get_global_queue(0, 0));
	if (!ds) {
		test_ptr_notnull("dispatch_source_create", ds);
		test_stop();
	}
	__block int eofs = 0;
	dispatch_source_set_event_handler(ds, ^{
		if (drain_until_eof(rfd)) {
			if (++eofs == 1) {
				atomic_fetch_add(&pipe_eofs, 1);
			}
			dispatch_source_cancel(ds);
		}
	});
	dispatch_source_set_cancel_handler(ds, ^{
		if (eofs != 1) {
			atomic_fetch_add(&pipe_bad_eofs, 1);
		}
		close(rfd);
		dispatch_release(ds);
		atomic_fetch_add(&pipe_cancels, 1);
		dispatch_semaphore_signal(pipe_in_flight);
		dispatch_group_leave(pipe_group);
	});
	dispatch_resume(ds);

	if (write(wfd, "data", 4) != 4) {
		test_errno("write", errno, 0);
	}
	close(wfd);
}

static void *
pipe_worker(void *ctxt)
{
	(void)ctxt;
	for (int i = 0; i < PIPE_ITERATIONS_PER_THREAD; i++) {
		dispatch_semaphore_wait(pipe_in_flight, DISPATCH_TIME_FOREVER);
		pipe_iteration();
	}
	return NULL;
}

// Many pipe read sources are registered while others observe the EOF and
// are torn down, so registration and teardown interleave with event
// delivery on the same event loop.
static void
test_concurrent_pipe_eof(void)
{
	pipe_group = dispatch_group_create();
	pipe_in_flight = dispatch_semaphore_create(PIPE_MAX_IN_FLIGHT);

	pthread_t threads[PIPE_THREADS];
	for (int i = 0; i < PIPE_THREADS; i++) {
		int err = pthread_create(&threads[i], NULL, pipe_worker, NULL);
		if (err) {
			test_errno("pthread_create", err, 0);
			test_stop();
		}
	}
	for (int i = 0; i < PIPE_THREADS; i++) {
		pthread_join(threads[i], NULL);
	}
	test_group_wait(pipe_group);

	long expected = (long)PIPE_THREADS * PIPE_ITERATIONS_PER_THREAD;
	test_long("pipe sources that received EOF", atomic_load(&pipe_eofs),
			expected);
	test_long("pipe sources cancelled", atomic_load(&pipe_cancels), expected);
	test_long("pipe sources with unexpected EOF count",
			atomic_load(&pipe_bad_eofs), 0);

	dispatch_release(pipe_in_flight);
	dispatch_release(pipe_group);
}

// A read source and a write source monitor the same socket, and the peer
// closes the connection. Both sources are notified and can be cancelled.
static void
test_socket_eof_with_reader_and_writer(void)
{
	int fds[2];
	if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) < 0) {
		test_errno("socketpair", errno, 0);
		test_stop();
	}
	int fd = fds[0], peer = fds[1];
	set_nonblocking(fd);

	// Fill the socket buffers so that the write source only fires once the
	// peer closes the connection.
	char buf[4096] = { 0 };
	for (;;) {
		ssize_t n = write(fd, buf, sizeof(buf));
		if (n < 0) {
			if (errno == EINTR) continue;
			if (errno != EAGAIN) {
				test_errno("write", errno, 0);
				test_stop();
			}
			break;
		}
	}

	dispatch_group_t g = dispatch_group_create();
	dispatch_queue_t q = dispatch_queue_create("socket_eof", NULL);
	__block bool reader_eof = false, writer_fired = false;
	__block int remaining = 2;

	dispatch_source_t rs = dispatch_source_create(DISPATCH_SOURCE_TYPE_READ,
			(uintptr_t)fd, 0, q);
	dispatch_source_t ws = dispatch_source_create(DISPATCH_SOURCE_TYPE_WRITE,
			(uintptr_t)fd, 0, q);
	if (!rs || !ws) {
		test_ptr_notnull("dispatch_source_create", rs ? ws : rs);
		test_stop();
	}
	dispatch_source_set_event_handler(rs, ^{
		if (drain_until_eof(fd)) {
			reader_eof = true;
			dispatch_source_cancel(rs);
		}
	});
	dispatch_source_set_event_handler(ws, ^{
		writer_fired = true;
		dispatch_source_cancel(ws);
	});
	dispatch_block_t cancel_handler = ^{
		if (--remaining == 0) {
			close(fd);
		}
		dispatch_group_leave(g);
	};
	dispatch_source_set_cancel_handler(rs, cancel_handler);
	dispatch_source_set_cancel_handler(ws, cancel_handler);

	dispatch_group_enter(g);
	dispatch_group_enter(g);
	dispatch_resume(rs);
	dispatch_resume(ws);

	close(peer);
	test_group_wait(g);

	test_long("reader received EOF", reader_eof, true);
	test_long("writer received event", writer_fired, true);

	dispatch_release(rs);
	dispatch_release(ws);
	dispatch_release(q);
	dispatch_release(g);
}

int
main(void)
{
	dispatch_test_start("Dispatch Source EOF");

	test_socket_eof_with_reader_and_writer();
	test_concurrent_pipe_eof();

	test_stop();
}
