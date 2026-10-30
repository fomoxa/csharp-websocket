using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Fomoxa.Net.WebSockets
{
    public sealed class WebSocketConnector : IDisposable
    {
        private readonly WebSocketAddress address;
        private readonly WebSocketSettings settings;
        private readonly Task<IPAddress[]>? resolving;
        private IPAddress[]? addresses;
        private int nextAddress;
        private Socket? connecting;
        private WebSocketClientHandshake? handshake;
        private bool finished;

        public WebSocketConnector(string address, ushort port, WebSocketSettings? settings = null)
        {
            this.settings = settings ?? new WebSocketSettings();
            this.address = WebSocketAddress.Parse(address, port, this.settings.Path);
            if (this.address.Secure)
            {
                throw new ArgumentException("this package has no TLS: end TLS at a proxy, or run WebSocketClientHandshake on a TLS byte stream", nameof(address));
            }

            if (IPAddress.TryParse(this.address.Host, out IPAddress? literal))
            {
                addresses = new[] { literal };
            }
            else
            {
                resolving = Dns.GetHostAddressesAsync(this.address.Host);
            }
        }

        public static WebSocketTransport Connect(string address, ushort port, WebSocketSettings? settings = null, TimeSpan? timeout = null)
        {
            TimeSpan limit = timeout ?? (settings ?? new WebSocketSettings()).HandshakeTimeout;
            using var connector = new WebSocketConnector(address, port, settings);
            var clock = Stopwatch.StartNew();
            while (true)
            {
                WebSocketConnectStatus status = connector.Poll(out WebSocketTransport? transport);
                if (status == WebSocketConnectStatus.Connected)
                {
                    return transport!;
                }

                if (status == WebSocketConnectStatus.Failed)
                {
                    throw new SocketException((int)SocketError.ConnectionRefused);
                }

                if (clock.Elapsed > limit)
                {
                    throw new SocketException((int)SocketError.TimedOut);
                }

                Thread.Sleep(1);
            }
        }

        public WebSocketConnectStatus Poll(out WebSocketTransport? transport)
        {
            transport = null;
            if (finished)
            {
                return WebSocketConnectStatus.Failed;
            }

            if (handshake == null)
            {
                WebSocketConnectStatus connected = PollSocket(out Socket? socket);
                if (connected != WebSocketConnectStatus.Connected)
                {
                    return connected;
                }

                handshake = new WebSocketClientHandshake(new SocketByteStream(socket!), address.Target, address.HostHeader, settings);
            }

            WebSocketConnectStatus status = handshake.Poll(out transport);
            if (status != WebSocketConnectStatus.Pending)
            {
                finished = true;
                handshake = null;
            }

            return status;
        }

        public void Dispose()
        {
            finished = true;
            connecting?.Dispose();
            connecting = null;
            handshake?.Dispose();
            handshake = null;
        }

        private WebSocketConnectStatus PollSocket(out Socket? connected)
        {
            connected = null;
            if (addresses == null)
            {
                if (!resolving!.IsCompleted)
                {
                    return WebSocketConnectStatus.Pending;
                }

                if (resolving.Exception != null || resolving.IsCanceled || resolving.Result.Length == 0)
                {
                    return Fail();
                }

                addresses = IPv4First(resolving.Result);
            }

            if (connecting == null)
            {
                return ConnectNext(out connected);
            }

            if (connecting.Poll(0, SelectMode.SelectError))
            {
                return RetryWithNextAddress(out connected);
            }

            if (!connecting.Poll(0, SelectMode.SelectWrite))
            {
                return WebSocketConnectStatus.Pending;
            }

            if ((int)connecting.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Error)! != 0)
            {
                return RetryWithNextAddress(out connected);
            }

            connected = connecting;
            connecting = null;
            return WebSocketConnectStatus.Connected;
        }

        private WebSocketConnectStatus ConnectNext(out Socket? connected)
        {
            connected = null;
            if (nextAddress == addresses!.Length)
            {
                return Fail();
            }

            var remote = new IPEndPoint(addresses[nextAddress++], address.Port);
            try
            {
                connecting = new Socket(remote.AddressFamily, SocketType.Stream, ProtocolType.Tcp) { Blocking = false };
                connecting.Connect(remote);
            }
            catch (SocketException error) when (IsInProgress(error.SocketErrorCode))
            {
                return WebSocketConnectStatus.Pending;
            }
            catch (SocketException)
            {
                return RetryWithNextAddress(out connected);
            }

            connected = connecting;
            connecting = null;
            return WebSocketConnectStatus.Connected;
        }

        private WebSocketConnectStatus RetryWithNextAddress(out Socket? connected)
        {
            connecting?.Dispose();
            connecting = null;
            return ConnectNext(out connected);
        }

        private WebSocketConnectStatus Fail()
        {
            Dispose();
            return WebSocketConnectStatus.Failed;
        }

        private static bool IsInProgress(SocketError error) =>
            error == SocketError.WouldBlock
            || error == SocketError.InProgress
            || error == SocketError.AlreadyInProgress;

        private static IPAddress[] IPv4First(IPAddress[] unordered)
        {
            var ordered = new List<IPAddress>(unordered.Length);
            foreach (IPAddress candidate in unordered)
            {
                if (candidate.AddressFamily == AddressFamily.InterNetwork)
                {
                    ordered.Add(candidate);
                }
            }

            foreach (IPAddress candidate in unordered)
            {
                if (candidate.AddressFamily != AddressFamily.InterNetwork)
                {
                    ordered.Add(candidate);
                }
            }

            return ordered.ToArray();
        }
    }
}
