//
//  SocketPool+kQueue.swift
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
import Darwin

public extension AsyncSocketPool where Self == SocketPool<kQueue> {
    static func kQueue(maxEvents limit: Int = 20, logger: some Logging = .disabled) -> SocketPool<kQueue> {
        .init(maxEvents: limit, logger: logger)
    }

    private init(maxEvents limit: Int, logger: some Logging = .disabled) {
        self.init(queue: FlyingSocks.kQueue(maxEvents: limit, logger: logger), logger: logger)
    }
}

public struct kQueue: EventQueue {

    private(set) var file: Socket.FileDescriptor
    private(set) var existing: [Socket.FileDescriptor: Socket.Events]
    private let eventsLimit: Int
    private let logger: any Logging

    public init(maxEvents limit: Int, logger: some Logging = .disabled) {
        self.file = .invalid
        self.existing = [:]
        self.eventsLimit = limit
        self.logger = logger
    }

    public mutating func open() throws {
        existing = [:]
        self.file = try Self.makeQueue()
    }

    public mutating func stop() throws {
        existing = [:]
        guard file != .invalid else {
            throw SocketError.disconnected
        }
        defer { file = .invalid }
        try Self.closeQueue(file: file)
    }

    public mutating func close() throws {
        // stop closes
    }

    public mutating func addEvents(_ events: Socket.Events, for socket: Socket.FileDescriptor) throws {
        for event in events {
            // Closing a socket removes kernel interests, even if its descriptor is reused.
            try addEvent(event, for: socket)
            existing[socket, default: []].insert(event)
        }
    }

    func addEvent(_ event: Socket.Event, for socket: Socket.FileDescriptor) throws {
        var event = Darwin.kevent(
            ident: UInt(socket.rawValue),
            filter: event.kqueueFilter,
            flags: UInt16(EV_ADD | EV_ENABLE),
            fflags: 0,
            data: 0,
            udata: nil
        )
        guard kevent(file.rawValue, &event, 1, nil, 0, nil) != -1 else {
            throw SocketError.makeFailed("kqueue add kevent")
        }
    }

    public mutating func removeEvents(_ events: Socket.Events, for socket: Socket.FileDescriptor) throws {
        try removeEvents(events, for: socket, using: removeEvent)
    }

    mutating func removeEvents(_ events: Socket.Events,
                               for socket: Socket.FileDescriptor,
                               using remove: (Socket.Event, Socket.FileDescriptor) throws -> Void) throws {
        guard var entries = existing[socket] else { return }
        var firstError: (any Error)?
        for event in events where entries.contains(event) {
            do {
                try remove(event, socket)
                entries.remove(event)
            } catch {
                if firstError == nil { firstError = error }
                if let socketError = error as? SocketError,
                   case .failed(_, let code, _) = socketError,
                   code == ENOENT || code == EBADF {
                    entries.remove(event)
                }
            }
        }
        existing[socket] = entries.isEmpty ? nil : entries
        if let firstError { throw firstError }
    }

    func removeEvent(_ event: Socket.Event, for socket: Socket.FileDescriptor) throws {
        var event = Darwin.kevent(
            ident: UInt(socket.rawValue),
            filter: event.kqueueFilter,
            flags: UInt16(EV_DELETE | EV_DISABLE),
            fflags: 0,
            data: 0,
            udata: nil
        )
        guard kevent(file.rawValue, &event, 1, nil, 0, nil) != -1 else {
            throw SocketError.makeFailed("kqueue remove kevent")
        }
    }

    public func getNotifications() throws -> [EventNotification] {
        var events = Array(repeating: kevent(), count: eventsLimit)
        let status = kevent(file.rawValue, nil, 0, &events, Int32(eventsLimit), nil)
        guard status > 0 else {
            throw SocketError.makeFailed("kqueue kevent")
        }

        return events
            .prefix(Int(status))
            .compactMap(EventNotification.make)
    }

    static func makeQueue(file: Int32 = Darwin.kqueue()) throws -> Socket.FileDescriptor {
        let file = Socket.FileDescriptor(rawValue: file)
        guard file != .invalid else {
            throw SocketError.makeFailed("kqueue")
        }
        return file
    }

    static func closeQueue(file: Socket.FileDescriptor) throws {
        guard file != .invalid else { return }
        guard Socket.close(file.rawValue) >= 0 else {
            throw SocketError.makeFailed("kqueue")
        }
    }
}

extension EventNotification {

    static func make(from event: kevent) -> Self? {
        guard let filter = Socket.Event.make(from: event.filter) else {
            return nil
        }
        var notification = EventNotification(
            file: .init(rawValue: Int32(event.ident)),
            events: [filter],
            errors: []
        )

        if filter == .read && event.data > 0 {
            // ignore read errors until there is no data available
            return notification
        }

        if (event.flags & UInt16(EV_EOF)) == EV_EOF {
            notification.errors.insert(.endOfFile)
        }
        if (event.flags & UInt16(EV_ERROR)) == EV_ERROR {
            notification.errors.insert(.error)
        }

        return notification
    }
}

extension Socket.Event {
    var kqueueFilter: Int16 {
        switch self {
        case .read: return Int16(EVFILT_READ)
        case .write: return Int16(EVFILT_WRITE)
        }
    }

    static func make(from filter: Int16) -> Self? {
        switch Int32(filter) {
        case EVFILT_READ: return .read
        case EVFILT_WRITE: return .write
        default: return nil
        }
    }
}
#endif
