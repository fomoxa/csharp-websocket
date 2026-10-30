using System;
using System.Net.Sockets;

namespace Fomoxa.Net.WebSockets
{
    public enum WebSocketConnectStatus
    {
        Pending,
        Connected,
        Failed,
    }

    public sealed class WebSocketClientHandshake : IDisposable
    {
        private readonly WebSocketSettings settings;
        private readonly string key;
        private readonly byte[] request;
        private readonly byte[] response;
        private readonly TimeSpan started;
        private IByteStream? stream;
        private int requestSent;
        private int used;
        private bool finished;

        public WebSocketClientHandshake(IByteStream stream, string target, string hostHeader, WebSocketSettings settings)
        {
            this.stream = stream ?? throw new ArgumentNullException(nameof(stream));
            this.settings = settings ?? throw new ArgumentNullException(nameof(settings));
            key = WebSocketHandshake.NewKey();
            request = WebSocketHandshake.Request(target, hostHeader, key);
            response = new byte[settings.MaxHandshakeBytes];
            started = settings.Clock();
        }

        public WebSocketConnectStatus Poll(out WebSocketTransport? transport)
        {
            transport = null;
            if (finished || stream == null)
            {
                return WebSocketConnectStatus.Failed;
            }

            if (settings.Clock() - started > settings.HandshakeTimeout)
            {
                return Fail();
            }

            if (!WriteRequest())
            {
                return finished ? WebSocketConnectStatus.Failed : WebSocketConnectStatus.Pending;
            }

            return ReadResponse(out transport);
        }

        public void Dispose()
        {
            finished = true;
            stream?.Dispose();
            stream = null;
        }

        private bool WriteRequest()
        {
            while (requestSent < request.Length)
            {
                int sent = stream!.Send(new ReadOnlySpan<byte>(request, requestSent, request.Length - requestSent), out SocketError error);
                if (error == SocketError.WouldBlock || (error == SocketError.Success && sent <= 0))
                {
                    return false;
                }

                if (error != SocketError.Success)
                {
                    Fail();
                    return false;
                }

                requestSent += sent;
            }

            return true;
        }

        private WebSocketConnectStatus ReadResponse(out WebSocketTransport? transport)
        {
            transport = null;
            int read = stream!.Receive(new Span<byte>(response, used, response.Length - used), out SocketError error);
            if (error == SocketError.WouldBlock)
            {
                return WebSocketConnectStatus.Pending;
            }

            if (error != SocketError.Success || read == 0)
            {
                return Fail();
            }

            used += read;
            int headerEnd = WebSocketHandshake.HeaderEnd(response, used);
            if (headerEnd < 0)
            {
                return used == response.Length ? Fail() : WebSocketConnectStatus.Pending;
            }

            if (!WebSocketHandshake.AcceptsResponse(response, headerEnd, key))
            {
                return Fail();
            }

            transport = new WebSocketTransport(stream, true, new ReadOnlySpan<byte>(response, headerEnd, used - headerEnd));
            stream = null;
            finished = true;
            return WebSocketConnectStatus.Connected;
        }

        private WebSocketConnectStatus Fail()
        {
            Dispose();
            return WebSocketConnectStatus.Failed;
        }
    }
}
