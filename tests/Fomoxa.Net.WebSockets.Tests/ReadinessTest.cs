using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Fomoxa.Net.Transports;
using NUnit.Framework;

namespace Fomoxa.Net.WebSockets.Tests
{
    public sealed class ReadinessTest
    {
        private static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);

        [Test]
        public void ASendTheSocketCannotTakeIsWouldBlockOnBothEndsAndLeavesOutputPending()
        {
            using var listener = WebSocketTransportTest.Listen(new WebSocketSettings(), out ushort port);
            using var connector = new WebSocketConnector("127.0.0.1", port);
            (WebSocketTransport client, WebSocketTransport server) = Pair(connector, listener);
            using (client)
            using (server)
            {
                long fromClient = Backpressure.FillUntilWouldBlock(client, new byte[16 * 1024]);
                Assert.IsTrue(client.HasPendingOutput);
                Assert.AreEqual(fromClient, Backpressure.Drain(server, client, fromClient));
                Wait(() =>
                {
                    client.Receive(Span<byte>.Empty);
                    return !client.HasPendingOutput;
                });

                long fromServer = Backpressure.FillUntilWouldBlock(server, new byte[16 * 1024]);
                Assert.IsTrue(server.HasPendingOutput);
                Assert.AreEqual(fromServer, Backpressure.Drain(client, server, fromServer));
            }
        }

        [Test]
        public void PendingInputCountsOnlyWholeFramesAlreadyRead()
        {
            using var listener = WebSocketTransportTest.Listen(new WebSocketSettings(), out ushort port);
            using var connector = new WebSocketConnector("127.0.0.1", port);
            (WebSocketTransport client, WebSocketTransport server) = Pair(connector, listener);
            using (client)
            using (server)
            {
                Assert.AreSame(((SocketByteStream)client.Stream).Socket, client.Socket);
                Assert.IsFalse(client.HasPendingInput);
                Assert.AreEqual(TransportSignal.Ok, server.Send(new byte[] { 1, 2, 3 }).Signal);
                Assert.AreEqual(TransportSignal.Ok, server.Send(new byte[] { 4, 5, 6 }).Signal);
                Wait(() => client.Socket.Available >= 10);

                var buffer = new byte[16];
                Assert.AreEqual(TransportSignal.Ok, client.Receive(buffer).Signal);
                Assert.IsTrue(client.HasPendingInput);
                Assert.AreEqual(TransportSignal.Ok, client.Receive(buffer).Signal);
                Assert.AreEqual(4, buffer[0]);
                Assert.IsFalse(client.HasPendingInput);
                Assert.AreEqual(TransportSignal.WouldBlock, client.Receive(buffer).Signal);
            }
        }

        [Test]
        public void AHalfReadFrameIsNotPendingInput()
        {
            using var listener = WebSocketTransportTest.Listen(new WebSocketSettings(), out ushort port);
            using var raw = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            raw.Connect(IPAddress.Loopback, port);
            raw.Send(System.Text.Encoding.ASCII.GetBytes("GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"));
            WebSocketTransport server = null;
            Wait(() =>
            {
                AcceptOutcome outcome = listener.Accept();
                server = (WebSocketTransport)outcome.Transport;
                return server != null;
            });
            using (server)
            {
                byte[] frame = { 0x82, 0x83, 1, 2, 3, 4, 7 ^ 1, 8 ^ 2, 9 ^ 3 };
                raw.Send(frame, 0, 4, SocketFlags.None);
                var buffer = new byte[16];
                Wait(() => server.Receive(buffer).Signal == TransportSignal.WouldBlock && server.Socket.Available == 0);
                Assert.IsFalse(server.HasPendingInput);

                raw.Send(frame, 4, frame.Length - 4, SocketFlags.None);
                ReceiveOutcome outcome = default;
                Wait(() => (outcome = server.Receive(buffer)).Signal != TransportSignal.WouldBlock);
                Assert.AreEqual(TransportSignal.Ok, outcome.Signal);
                CollectionAssert.AreEqual(new byte[] { 7, 8, 9 }, buffer[..3]);
            }
        }

        [Test]
        public void TheListenerWaitEndsOnANewConnectionAndOnTimeout()
        {
            using var listener = WebSocketTransportTest.Listen(new WebSocketSettings(), out ushort port);
            var clock = Stopwatch.StartNew();
            Assert.IsFalse(listener.WaitReady(TimeSpan.FromMilliseconds(50)));
            Assert.GreaterOrEqual(clock.ElapsedMilliseconds, 40);

            using var raw = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            raw.Connect(IPAddress.Loopback, port);
            clock.Restart();
            Assert.IsTrue(listener.WaitReady(TimeSpan.FromSeconds(10)));
            Assert.Less(clock.Elapsed, TimeSpan.FromSeconds(5));
            listener.Accept();

            raw.Send(System.Text.Encoding.ASCII.GetBytes("GET / HTTP/1.1\r\n"));
            Assert.IsTrue(listener.WaitReady(TimeSpan.FromSeconds(10)));
        }

        [Test]
        public void AnInterruptSocketEndsTheListenerWait()
        {
            using var listener = WebSocketTransportTest.Listen(new WebSocketSettings(), out _);
            using var interrupt = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            interrupt.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            interrupt.Connect(interrupt.LocalEndPoint);
            Assert.IsFalse(listener.WaitReady(TimeSpan.FromMilliseconds(20), interrupt));

            interrupt.Send(new byte[] { 1 });
            var clock = Stopwatch.StartNew();
            Assert.IsTrue(listener.WaitReady(TimeSpan.FromSeconds(10), interrupt));
            Assert.Less(clock.Elapsed, TimeSpan.FromSeconds(5));
        }

        [Test]
        public void TheListenerWaitEndsWhenAPendingHandshakeExpires()
        {
            var now = TimeSpan.Zero;
            var settings = new WebSocketSettings { HandshakeTimeout = TimeSpan.FromMilliseconds(100), Clock = () => now };
            using var listener = WebSocketTransportTest.Listen(settings, out ushort port);
            using var raw = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            raw.Connect(IPAddress.Loopback, port);
            Wait(() =>
            {
                listener.Accept();
                return listener.PendingCount == 1;
            });

            now = TimeSpan.FromMilliseconds(100);
            var clock = Stopwatch.StartNew();
            listener.WaitReady(TimeSpan.FromSeconds(10));
            Assert.Less(clock.Elapsed, TimeSpan.FromSeconds(5));
        }

        [Test]
        public void TheBlockingConnectReturnsATransportOrThrows()
        {
            using var listener = WebSocketTransportTest.Listen(new WebSocketSettings(), out ushort port);
            ITransport server = null;
            var stop = false;
            var accepting = new Thread(() =>
            {
                while (!stop && server == null)
                {
                    listener.WaitReady(TimeSpan.FromMilliseconds(100));
                    server = listener.Accept().Transport;
                }
            }) { IsBackground = true };
            accepting.Start();

            using (WebSocketTransport client = WebSocketConnector.Connect("127.0.0.1", port))
            {
                Assert.IsTrue(accepting.Join(Limit));
                using (server)
                {
                    Assert.AreEqual(TransportSignal.Ok, client.Send(new byte[] { 42 }).Signal);
                    var buffer = new byte[4];
                    ReceiveOutcome outcome = default;
                    Wait(() => (outcome = server.Receive(buffer)).Signal != TransportSignal.WouldBlock);
                    Assert.AreEqual(1, outcome.Count);
                }
            }

            stop = true;
            ushort closed = port;
            listener.Dispose();
            Assert.Throws<SocketException>(() => WebSocketConnector.Connect("127.0.0.1", closed, null, TimeSpan.FromSeconds(5)));
        }

        private static (WebSocketTransport Client, WebSocketTransport Server) Pair(WebSocketConnector connector, WebSocketListener listener)
        {
            WebSocketTransport server = null;
            WebSocketTransport client = null;
            Wait(() =>
            {
                AcceptOutcome accepted = listener.Accept();
                if (accepted.Status == AcceptStatus.Accepted)
                {
                    server = (WebSocketTransport)accepted.Transport;
                }

                if (client == null)
                {
                    Assert.AreNotEqual(WebSocketConnectStatus.Failed, connector.Poll(out client));
                }

                return client != null && server != null;
            });
            return (client, server);
        }

        private static void Wait(Func<bool> done)
        {
            var clock = Stopwatch.StartNew();
            while (!done())
            {
                Assert.Less(clock.Elapsed, Limit, "timed out");
                Thread.Sleep(1);
            }
        }
    }

    internal static class Backpressure
    {
        private const long MostBytes = 256L * 1024 * 1024;
        private static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);

        public static long FillUntilWouldBlock(ITransport transport, byte[] chunk)
        {
            long accepted = 0;
            TransportSignal last = TransportSignal.Ok;
            var filling = new Thread(() =>
            {
                while (last == TransportSignal.Ok && accepted < MostBytes)
                {
                    SendOutcome outcome = transport.Send(chunk);
                    last = outcome.Signal;
                    if (last == TransportSignal.Ok)
                    {
                        accepted += chunk.Length;
                    }
                }
            }) { IsBackground = true };
            filling.Start();

            Assert.IsTrue(filling.Join(Limit), "a send blocked the calling thread");
            Assert.AreEqual(TransportSignal.WouldBlock, last);
            return accepted;
        }

        public static long Drain(ITransport receiver, ITransport sender, long expected)
        {
            long read = 0;
            var buffer = new byte[256 * 1024];
            var clock = Stopwatch.StartNew();
            while (read < expected && clock.Elapsed < Limit)
            {
                sender.Receive(Span<byte>.Empty);
                ReceiveOutcome outcome = receiver.Receive(buffer);
                if (outcome.Signal == TransportSignal.Ok)
                {
                    read += outcome.Count;
                }
                else
                {
                    Assert.AreEqual(TransportSignal.WouldBlock, outcome.Signal);
                    Thread.Sleep(1);
                }
            }

            return read;
        }
    }
}
