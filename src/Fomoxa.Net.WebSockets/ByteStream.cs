using System;
using System.Net.Sockets;

namespace Fomoxa.Net.WebSockets
{
    public interface IByteStream : IDisposable
    {
        int Send(ReadOnlySpan<byte> bytes, out SocketError error);

        int Receive(Span<byte> buffer, out SocketError error);
    }

    public sealed class SocketByteStream : IByteStream
    {
        public SocketByteStream(Socket socket)
        {
            Socket = socket ?? throw new ArgumentNullException(nameof(socket));
            socket.Blocking = false;
            socket.NoDelay = true;
        }

        public Socket Socket { get; }

        public int Send(ReadOnlySpan<byte> bytes, out SocketError error)
        {
            if (!Socket.Poll(0, SelectMode.SelectWrite))
            {
                error = SocketError.WouldBlock;
                return 0;
            }

            return Socket.Send(bytes, SocketFlags.None, out error);
        }

        public int Receive(Span<byte> buffer, out SocketError error) => Socket.Receive(buffer, SocketFlags.None, out error);

        public void Dispose() => Socket.Dispose();
    }
}
