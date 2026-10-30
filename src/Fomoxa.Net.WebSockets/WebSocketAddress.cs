using System;
using System.Net;
using System.Net.Sockets;

namespace Fomoxa.Net.WebSockets
{
    public readonly struct WebSocketAddress
    {
        private WebSocketAddress(string host, ushort port, string target, string hostHeader, bool secure)
        {
            Host = host;
            Port = port;
            Target = target;
            HostHeader = hostHeader;
            Secure = secure;
        }

        public string Host { get; }

        public ushort Port { get; }

        public string Target { get; }

        public string HostHeader { get; }

        public bool Secure { get; }

        public static WebSocketAddress Parse(string address, ushort port, string path)
        {
            if (string.IsNullOrEmpty(address))
            {
                throw new ArgumentException("a WebSocket address is a host name, an IP address or a ws:// URL", nameof(address));
            }

            bool secure = address.StartsWith("wss://", StringComparison.OrdinalIgnoreCase);
            if (!secure && !address.StartsWith("ws://", StringComparison.OrdinalIgnoreCase))
            {
                string literalHost = IPAddress.TryParse(address, out IPAddress? literal) && literal.AddressFamily == AddressFamily.InterNetworkV6
                    ? $"[{address}]"
                    : address;
                return new WebSocketAddress(address, port, path, $"{literalHost}:{port}", false);
            }

            if (!Uri.TryCreate(address, UriKind.Absolute, out Uri? uri) || uri.Host.Length == 0 || uri.Fragment.Length > 0)
            {
                throw new ArgumentException($"'{address}' is not a valid {(secure ? "wss" : "ws")}:// URL", nameof(address));
            }

            return new WebSocketAddress(uri.DnsSafeHost, (ushort)uri.Port, uri.PathAndQuery, uri.Authority, secure);
        }

        public static string UrlFor(string address, ushort port, string path)
        {
            if (address != null
                && (address.StartsWith("ws://", StringComparison.OrdinalIgnoreCase) || address.StartsWith("wss://", StringComparison.OrdinalIgnoreCase)))
            {
                if (!Uri.TryCreate(address, UriKind.Absolute, out Uri? uri) || uri.Host.Length == 0 || uri.Fragment.Length > 0)
                {
                    throw new ArgumentException($"'{address}' is not a valid WebSocket URL", nameof(address));
                }

                return address;
            }

            WebSocketAddress parsed = Parse(address!, port, path);
            return $"ws://{parsed.HostHeader}{parsed.Target}";
        }
    }
}
