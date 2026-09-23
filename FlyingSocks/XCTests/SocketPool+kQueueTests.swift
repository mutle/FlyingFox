//
//  SocketPool+kQueueTests.swift
//  FlyingFox
//
//  Created by Simon Whitty on 10/09/2022.
//  Copyright © 2022 Simon Whitty. All rights reserved.
//
//  Distributed under the permissive MIT license
//  Get the latest version from here:
//
//  https://github.com/swhitty/FlyingFox
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to deal
//  in the Software without restriction, including without limitation the rights
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included in all
//  copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//  SOFTWARE.
//

#if canImport(Darwin)
@testable import FlyingSocks
import XCTest

final class kQueueTests: XCTestCase {

    func testRemovalPreservesFirstErrorAndRetriesUnexpectedFailures() throws {
        for code in [Int32(ENOENT), EBADF, EIO] {
            for failAgain in [false, true] {
                var queue = try kQueue.make()
                defer { try! queue.stop() }
                let (socket, peer) = try Socket.makeNonBlockingPair()
                defer { try! socket.close(); try! peer.close() }
                try queue.addEvents(.connection, for: socket.file)
                let original = SocketError.failed(type: "original removal", errno: code, message: "first failure")
                var attempted: [Socket.Event] = []
                let nativeQueue = queue
                XCTAssertThrowsError(try queue.removeEvents(.connection, for: socket.file) { event, file in
                    attempted.append(event)
                    if attempted.count == 1 { throw original }
                    errno = EINVAL
                    if failAgain { throw SocketError.makeFailed("later removal") }
                    try nativeQueue.removeEvent(event, for: file)
                }) { error in
                    XCTAssertEqual(error as? SocketError, original)
                }
                XCTAssertEqual(attempted.count, 2)
                let first = try XCTUnwrap(attempted.first)
                var retained: Socket.Events = code == EIO ? [first] : []
                if failAgain, let last = attempted.last { retained.insert(last) }
                XCTAssertEqual(queue.existing[socket.file] ?? [], retained)
                var retried: Socket.Events = []
                try queue.removeEvents(.connection, for: socket.file) { event, file in
                    retried.insert(event)
                    try nativeQueue.removeEvent(event, for: file)
                }
                XCTAssertEqual(retried, retained)
                XCTAssertNil(queue.existing[socket.file])
            }
        }
    }

    func testClosedSocketRemovalClearsCache() throws {
        for events: Socket.Events in [.read, .write, .connection] {
            var queue = try kQueue.make()
            defer { try! queue.stop() }
            let (socket, peer) = try Socket.makeNonBlockingPair()
            defer { try! peer.close() }
            try queue.addEvents(events, for: socket.file)
            try socket.close()
            XCTAssertThrowsError(try queue.removeEvents(events, for: socket.file))
            XCTAssertNil(queue.existing[socket.file])
            try queue.removeEvents(events, for: socket.file)
        }
    }

    func testReusedDescriptorReportsReadiness() throws {
        for events: Socket.Events in [.read, .write, .connection] {
            for removeFirst in [false, true] {
                var queue = try kQueue.make()
                defer { try! queue.stop() }
                let (target, oldPeer) = try Socket.makeNonBlockingPair()
                let (replacement, peer) = try Socket.makeNonBlockingPair()
                defer {
                    try! target.close()
                    try! oldPeer.close()
                    try! replacement.close()
                    try! peer.close()
                }
                try queue.addEvents(events, for: target.file)
                // Atomically close the old socket while retaining ownership of the descriptor.
                XCTAssertEqual(dup2(replacement.file.rawValue, target.file.rawValue), target.file.rawValue)
                if removeFirst {
                    XCTAssertThrowsError(try queue.removeEvents(events, for: target.file))
                    XCTAssertNil(queue.existing[target.file])
                }
                try queue.addEvents(events, for: target.file)
                let data = Data([42])
                _ = try peer.write(data, from: data.startIndex)
                XCTAssertEqual(try queue.readyEvents(for: target.file), events)
            }
        }
    }

    func testPartialRemovalStillDeletesOtherFilter() throws {
        for missing: Socket.Event in [.read, .write] {
            var queue = try kQueue.make()
            defer { try! queue.stop() }
            let (socket, peer) = try Socket.makeNonBlockingPair()
            defer { try! socket.close(); try! peer.close() }
            try queue.addEvents(.connection, for: socket.file)
            try queue.removeEvent(missing, for: socket.file)
            XCTAssertThrowsError(try queue.removeEvents(.connection, for: socket.file))
            XCTAssertNil(queue.existing[socket.file])
            let data = Data([42])
            _ = try peer.write(data, from: data.startIndex)
            XCTAssertTrue(try queue.readyEvents(for: socket.file).isEmpty)
        }
    }

    func testSelectiveAndRepeatedOperationsPreserveOtherInterests() throws {
        var queue = try kQueue.make()
        defer { try! queue.stop() }
        let (socket, peer) = try Socket.makeNonBlockingPair()
        defer { try! socket.close(); try! peer.close() }
        try queue.addEvents([], for: socket.file)
        XCTAssertNil(queue.existing[socket.file])
        try queue.addEvents(.connection, for: socket.file)
        try queue.addEvents(.connection, for: socket.file)
        try queue.removeEvents([], for: socket.file)
        XCTAssertEqual(queue.existing[socket.file], .connection)
        try queue.removeEvents(.read, for: socket.file)
        try queue.removeEvents(.read, for: socket.file)
        XCTAssertEqual(queue.existing[socket.file], .write)
        XCTAssertEqual(try queue.readyEvents(for: socket.file), .write)
        try queue.removeEvents(.write, for: socket.file)
        XCTAssertNil(queue.existing[socket.file])
        XCTAssertTrue(try queue.readyEvents(for: socket.file).isEmpty)
    }

    func testInvalidQueueDoesNotHideCachedAddOrRemovalErrors() throws {
        var queue = try kQueue.make()
        defer { try! queue.stop() }
        let (socket, peer) = try Socket.makeNonBlockingPair()
        defer { try! socket.close(); try! peer.close() }
        try queue.addEvents(.connection, for: socket.file)
        // Keep ownership of the queue descriptor so parallel tests cannot reuse it.
        let replacement = open("/dev/null", O_RDONLY)
        XCTAssertGreaterThanOrEqual(replacement, 0)
        defer { XCTAssertEqual(Darwin.close(replacement), 0) }
        XCTAssertEqual(dup2(replacement, queue.file.rawValue), queue.file.rawValue)
        XCTAssertThrowsError(try queue.addEvents(.connection, for: socket.file))
        XCTAssertThrowsError(try queue.removeEvents(.connection, for: socket.file))
        XCTAssertNil(queue.existing[socket.file])
        XCTAssertThrowsError(try queue.addEvents(.read, for: socket.file))
        XCTAssertNil(queue.existing[socket.file])
    }

    func testQueueCloses() throws {
        var queue = try kQueue.make()
        XCTAssertNoThrow(try queue.stop())
    }

    func testQueueThrowsError_Closes() throws {
        XCTAssertThrowsError(try kQueue.closeQueue(file: .validMock))
    }

    func testQueueThrowsError_Make() throws {
        XCTAssertThrowsError(try kQueue.makeQueue(file: -1))
    }

    func testAddingEventToInvalidDescriptor_ThrowsError() throws {
        let queue = try kQueue.make()

        XCTAssertThrowsError(
            try queue.addEvent(.read, for: .validMock)
        )
    }

    func testAddingAndRemovingEvents() throws {
        var queue = try kQueue.make()
        let (s1, _) = try Socket.makeNonBlockingPair()

        XCTAssertNoThrow(try queue.addEvents(.connection, for: s1.file))
        XCTAssertEqual(queue.existing[s1.file], .connection)

        XCTAssertNoThrow(try queue.removeEvents(.connection, for: s1.file))
        XCTAssertNil(queue.existing[s1.file])
    }

    func testRemovingEventToInvalidDescriptor_ThrowsError() throws {
        let queue = try kQueue.make()

        XCTAssertThrowsError(
            try queue.removeEvent(.read, for: .validMock)
        )
    }

    func testFilterEvents() {
        XCTAssertEqual(
            Socket.Event.read.kqueueFilter,
            Int16(EVFILT_READ)
        )
        XCTAssertEqual(
            Socket.Event.write.kqueueFilter,
            Int16(EVFILT_WRITE)
        )
        XCTAssertEqual(
            Socket.Event.make(from: Int16(EVFILT_READ)),
            .read
        )
        XCTAssertEqual(
            Socket.Event.make(from: Int16(EVFILT_WRITE)),
            .write
        )
        XCTAssertNil(Socket.Event.make(from: 10100))
    }

    func testReadResult_CreatesNotification() {
        XCTAssertEqual(
            EventNotification.make(from: .make(
                ident: 10,
                filter: EVFILT_READ
            )),
            EventNotification(
                file: .init(rawValue: 10),
                events: .read,
                errors: []
            )
        )
    }

    func testReadErrors_CreatesNotification() {
        XCTAssertEqual(
            EventNotification.make(from: .make(
                ident: 10,
                filter: EVFILT_READ,
                flags: EV_ERROR
            )),
            EventNotification(
                file: .init(rawValue: 10),
                events: .read,
                errors: [.error]
            )
        )
    }

    func testErrorsIgnored_WhenReadWithDataAvailable() {
        XCTAssertEqual(
            EventNotification.make(from: .make(
                ident: 10,
                filter: EVFILT_READ,
                flags: EV_ERROR,
                data: 5
            )),
            EventNotification(
                file: .init(rawValue: 10),
                events: .read,
                errors: []
            )
        )
    }

    func testWriteResult_CreatesNotification() {
        XCTAssertEqual(
            EventNotification.make(from: .make(
                ident: 10,
                filter: EVFILT_WRITE
            )),
            EventNotification(
                file: .init(rawValue: 10),
                events: .write,
                errors: []
            )
        )
    }

    func testWriteErrors_CreatesNotification() {
        XCTAssertEqual(
            EventNotification.make(from: .make(
                ident: 10,
                filter: EVFILT_WRITE,
                flags: EV_EOF,
                data: 10
            )),
            EventNotification(
                file: .init(rawValue: 10),
                events: .write,
                errors: [.endOfFile]
            )
        )
    }

    func testInvalidFilter_DoesNotCreateNotification() {
        XCTAssertNil(
            EventNotification.make(from: .make(
                filter: 0
            ))
        )
    }

    func testQueueReturnsEvents() async throws {
        var queue = try kQueue.make()

        let (s1, s2) = try Socket.makeNonBlockingPair()

        try queue.addEvents([.read], for: s2.file)

        let data = Data([10, 20])
        _ = try s1.write(data, from: data.startIndex)

        await AsyncAssertEqual(
            try await queue.getEvents(),
            [.init(file: s2.file, events: [.read], errors: [])]
        )
    }

    func testQueueThrowsErrorIfClosed() async throws {
        var queue = try kQueue.make()
        let (s1, _) = try Socket.makeNonBlockingPair()
        try queue.addEvents([.read], for: s1.file)

        try queue.stop()
        await AsyncAssertThrowsError(try await queue.getEvents())
    }
}

private extension kQueue {

    func readyEvents(for file: Socket.FileDescriptor) throws -> Socket.Events {
        var events = Array(repeating: kevent(), count: 8)
        var timeout = timespec(tv_sec: 0, tv_nsec: 0)
        let count = kevent(self.file.rawValue, nil, 0, &events, Int32(events.count), &timeout)
        guard count >= 0 else { throw SocketError.makeFailed("test kevent") }
        return events.prefix(Int(count)).reduce(into: Socket.Events()) { result, event in
            if event.ident == UInt(file.rawValue), let filter = Socket.Event.make(from: event.filter) {
                result.insert(filter)
            }
        }
    }

    static func make() throws -> Self {
        var queue = kQueue(maxEvents: 20)
        try queue.open()
        return queue
    }

    func getEvents() async throws -> [EventNotification] {
        let queue = UncheckedSendable(wrappedValue: self)
        return try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global().async {
                let result = Result {
                    try queue.wrappedValue.getNotifications()
                }
                continuation.resume(with: result)
            }
        }
    }
}

private extension kevent {
    static func make(ident: UInt = 0,
                     filter: Int32 = EVFILT_READ,
                     flags: Int32 = 0,
                     data: Int = 0) -> Self {
        .init(ident: ident,
              filter: Int16(filter),
              flags: UInt16(flags),
              fflags: 0,
              data: data,
              udata: nil)
    }
}
#endif
