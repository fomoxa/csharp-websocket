# Fomoxa.Net.WebSockets

WebSocket transport and listener for [Fomoxa.Net](https://github.com/fomoxa/csharp).
`WebSocketTransport` implements `ITransport` and `WebSocketListener` implements `IListenerTransport`, so a Fomoxa `Connection`, or anything built on one such as fRPC, runs over WebSocket without changes.

The library targets `netstandard2.1` and needs no engine.
It frames RFC 6455 itself on a non-blocking byte stream and does all its work inside the caller's `Send`, `Receive` and `Accept` calls, with no thread or `Task` of its own.

## Scope

| Included | Not included |
|---|---|
| Binary messages up to `FomoxaWire.MaxDataFrameSize`, fragmented or not | Text messages (a text frame is a protocol error) |
| Ping answered with pong, close handshake | Extensions and subprotocols |
| HTTP upgrade on both sides: path check, origin allow-list, version 13 | TLS: end `wss://` at a proxy, or run the client handshake on your own TLS byte stream |
| Handshake size and time limits | HTTP routing beyond one path |

## Using it

Server side:

```csharp
var settings = new WebSocketSettings { Path = "/fomoxa" };
settings.AllowedOrigins.Add("https://game.example");
using var listener = new WebSocketListener(new IPEndPoint(IPAddress.Any, 7777), settings);

AcceptOutcome accepted = listener.Accept();
if (accepted.Status == AcceptStatus.Accepted)
{
    ITransport transport = accepted.Transport!;
}
```

`Accept` never blocks. A loop that has nothing else to do can sleep in `listener.WaitReady(timeout)`, which returns when a connection arrives, a pending handshake can move, or one runs out of time.

Client side, non-blocking:

```csharp
using var connector = new WebSocketConnector("ws://game.example:7777/fomoxa", 0);
while (connector.Poll(out WebSocketTransport? transport) == WebSocketConnectStatus.Pending)
{
}
```

or blocking, for a program that owns its thread:

```csharp
WebSocketTransport transport = WebSocketConnector.Connect("127.0.0.1", 7777, settings);
```

An address is a host name, an IP address or a `ws://` URL. A host or IP uses the port argument and `WebSocketSettings.Path`; a URL carries its own.

`WebSocketClientHandshake` runs the client upgrade on any `IByteStream`. That is how a caller adds TLS: wrap the socket in a stream that encrypts, then hand it over.

## Waiting on the socket

A loop that wants to sleep until the transport has work reads three members:

| Member | Meaning |
|---|---|
| `Socket` | The socket under a `SocketByteStream`, or `null` for another stream |
| `HasPendingInput` | A whole message or frame is already read and not delivered; waiting on the socket would miss it |
| `HasPendingOutput` | Bytes of an accepted send are still queued; wait for the socket to become writable |

## Platform note

`SocketByteStream` and the listener poll the socket for write readiness before every send.
Under Unity's Mono a non-blocking `Socket.Send` can block when the send buffer is full; the poll keeps the call non-blocking.

## Building and testing

```
dotnet build Fomoxa.Net.WebSockets.sln -c Release
dotnet test Fomoxa.Net.WebSockets.sln -c Release
```

The tests run real sockets on loopback, including the .NET `ClientWebSocket` against the listener and the connector against a .NET server WebSocket.

## License

Apache-2.0.
