import http, { Agent as HttpAgent } from "node:http";
import https from "node:https";
import net, { type AddressInfo } from "node:net";
import type { Duplex } from "node:stream";
import type { RuntimeEnv } from "openclaw/plugin-sdk/runtime-env";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import WebSocket, { WebSocketServer } from "ws";
import {
  buildRelayWebSocketOptions,
  buildRelayWebSocketUrl,
  monitorSlackRelaySource,
  parseRelayFrame,
  resolveRelayProxyAgent,
  SlackRelayMalformedFrameError,
  SLACK_RELAY_MAX_PAYLOAD_BYTES,
  type SlackRelayIdentity,
} from "./relay-source.js";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((nextResolve, nextReject) => {
    resolve = nextResolve;
    reject = nextReject;
  });
  return { promise, reject, resolve };
}

function relayFrame(text: string): Buffer {
  return Buffer.from(text, "utf8");
}

describe("Slack relay source", () => {
  it("builds authenticated relay websocket URLs safely", () => {
    expect(
      buildRelayWebSocketUrl({
        url: "https://router.example.com/gateway/ws?existing=1",
        authToken: "secret",
        gatewayId: "pash",
      }),
    ).toBe("wss://router.example.com/gateway/ws?existing=1&gateway_id=pash");

    expect(() =>
      buildRelayWebSocketUrl({
        url: "ws://router.example.com/gateway/ws",
        authToken: "secret",
        gatewayId: "pash",
      }),
    ).toThrow("plaintext ws:// for non-local host");
    expect(() =>
      buildRelayWebSocketUrl({
        url: "https://router.example.com",
        authToken: "secret",
        gatewayId: "pash",
      }),
    ).toThrow("must include its websocket path");

    expect(
      buildRelayWebSocketOptions("secret", "wss://router.example.com/gateway/ws?gateway_id=pash"),
    ).toMatchObject({
      headers: { Authorization: "Bearer secret" },
      maxPayload: SLACK_RELAY_MAX_PAYLOAD_BYTES,
      perMessageDeflate: false,
    });
  });

  it("applies hello identity and acks a routed event only after durable accept", async () => {
    const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    await new Promise<void>((resolve) => {
      server.once("listening", resolve);
    });
    const port = (server.address() as AddressInfo).port;
    const ack = deferred<Record<string, unknown>>();
    const acceptStarted = deferred<void>();
    const acceptDone = deferred<void>();
    const receivedAcks: Array<Record<string, unknown>> = [];
    const requestHeaders = deferred<{ authorization?: string; url?: string }>();
    server.once("connection", (socket, request) => {
      requestHeaders.resolve({
        authorization: request.headers.authorization,
        url: request.url,
      });
      socket.on("message", (data) => {
        const messageText = Array.isArray(data)
          ? Buffer.concat(data).toString("utf8")
          : data instanceof ArrayBuffer
            ? Buffer.from(new Uint8Array(data)).toString("utf8")
            : Buffer.from(data).toString("utf8");
        const frame = JSON.parse(messageText) as Record<string, unknown>;
        receivedAcks.push(frame);
        ack.resolve(frame);
      });
      socket.send(
        JSON.stringify({
          type: "hello",
          gateway_id: "pash",
          slack_identity: {
            username: "Nik Team Claw",
            icon_url: "https://example.com/nik.png",
          },
        }),
      );
      socket.send("not-json");
      socket.send(
        JSON.stringify({
          type: "slack_event",
          delivery_id: "delivery-failed",
          route: { kind: "user_group", key: "T1:S1" },
          payload: {
            event: {
              type: "message",
              channel: "C1",
              user: "U1",
              text: "fail-handler",
              ts: "1.000000",
            },
          },
        }),
      );
      socket.send(
        JSON.stringify({
          type: "slack_event",
          delivery_id: "delivery-1",
          route: { kind: "channel_default", key: "T1:C1" },
          payload: {
            team_id: "T1",
            event_id: "Ev1",
            event: {
              type: "message",
              channel: "C1",
              user: "U1",
              text: "hello",
              ts: "1.000001",
            },
          },
        }),
      );
    });

    const abortController = new AbortController();
    const acceptRelayEvent = vi.fn(
      async (event: { deliveryId: string; message: { text?: string } }) => {
        if (event.message.text === "fail-handler") {
          throw new Error("durable accept failed");
        }
        acceptStarted.resolve();
        await acceptDone.promise;
      },
    );
    const runtimeError = vi.fn();
    const identities: Array<SlackRelayIdentity | undefined> = [];
    const statuses: Array<Record<string, unknown>> = [];
    const monitor = monitorSlackRelaySource({
      config: {
        url: `ws://127.0.0.1:${port}/gateway/ws`,
        authToken: "relay-secret",
        gatewayId: "pash",
      },
      acceptRelayEvent,
      runtime: { error: runtimeError, log: vi.fn() } as unknown as RuntimeEnv,
      abortSignal: abortController.signal,
      identityHealth: { lifecycle: "blocked", lastError: "request_timeout" },
      setIdentity: (identity) => identities.push(identity),
      setStatus: (status) => statuses.push(status),
    });

    await expect(requestHeaders.promise).resolves.toEqual({
      authorization: "Bearer relay-secret",
      url: "/gateway/ws?gateway_id=pash",
    });
    await acceptStarted.promise;
    expect(receivedAcks).toEqual([]);
    acceptDone.resolve();
    await expect(ack.promise).resolves.toEqual({
      type: "ack",
      delivery_id: "delivery-1",
    });
    expect(receivedAcks).toEqual([{ type: "ack", delivery_id: "delivery-1" }]);
    expect(runtimeError).toHaveBeenCalledTimes(2);
    expect(acceptRelayEvent).toHaveBeenCalledWith({
      deliveryId: "delivery-1",
      message: expect.objectContaining({ channel: "C1", text: "hello" }),
    });
    // The failed durable accept must never ack: the router redelivers it.
    expect(receivedAcks).not.toContainEqual({ type: "ack", delivery_id: "delivery-failed" });
    expect(identities).toContainEqual({
      username: "Nik Team Claw",
      iconUrl: "https://example.com/nik.png",
    });
    expect(statuses).toContainEqual({
      relayRoute: { kind: "channel_default", key: "T1:C1" },
    });
    expect(statuses).toContainEqual({
      connected: true,
      lastConnectedAt: expect.any(Number),
      lifecycle: "blocked",
      lastError: "request_timeout",
    });

    abortController.abort();
    await monitor;
    expect(identities.at(-1)).toBeUndefined();
    for (const client of server.clients) {
      client.terminate();
    }
    await new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve();
      });
    });
  });

  describe("parseRelayFrame", () => {
    it("parses valid JSON frames", () => {
      const frame = parseRelayFrame(
        relayFrame(JSON.stringify({ type: "slack_event", data: { text: "hello" } })),
      );
      expect(frame).toEqual({ type: "slack_event", data: { text: "hello" } });
    });

    it("throws SlackRelayMalformedFrameError for malformed JSON", () => {
      expect(() => parseRelayFrame(relayFrame("NOT JSON {{{"))).toThrow(
        SlackRelayMalformedFrameError,
      );
    });

    it("wraps the original SyntaxError as the cause", () => {
      let error: unknown;
      try {
        parseRelayFrame(relayFrame("NOT JSON {{{"));
      } catch (err: unknown) {
        error = err;
      }
      expect(error).toBeInstanceOf(SlackRelayMalformedFrameError);
      expect((error as SlackRelayMalformedFrameError).message).toContain("malformed JSON frame");
      expect((error as SlackRelayMalformedFrameError).cause).toBeDefined();
    });

    it("parses empty object frames", () => {
      expect(parseRelayFrame(relayFrame("{}"))).toEqual({});
    });

    it("parses array frames", () => {
      expect(parseRelayFrame(relayFrame("[1, 2, 3]"))).toEqual([1, 2, 3]);
    });
  });
});

// Self-signed loopback certificate (SAN: 127.0.0.1, localhost; valid to 2126)
// so the proxied and direct wss:// dials below terminate real TLS on 127.0.0.1.
const RELAY_TEST_TLS_CERT = `-----BEGIN CERTIFICATE-----
MIIBpzCCAUygAwIBAgIUezTxOxdfUphW7GSOPN3w6ppcqe0wCgYIKoZIzj0EAwIw
GzEZMBcGA1UEAwwQc2xhY2stcmVsYXkudGVzdDAgFw0yNjA5MDkxNzUzMzBaGA8y
MTI2MDgxNjE3NTMzMFowGzEZMBcGA1UEAwwQc2xhY2stcmVsYXkudGVzdDBZMBMG
ByqGSM49AgEGCCqGSM49AwEHA0IABKYC/MK+pREkCGg+imE4JGALlFu2aVQP7XJN
Ckezs+JewV/OAxB4RzXVcgSgGKP6USQaDBnoBBEy+34QH2zXtJ2jbDBqMB0GA1Ud
DgQWBBSI3WK70K2Wh3wnN+TdlErOoIKmQzAfBgNVHSMEGDAWgBSI3WK70K2Wh3wn
N+TdlErOoIKmQzAaBgNVHREEEzARgglsb2NhbGhvc3SHBH8AAAEwDAYDVR0TBAUw
AwEB/zAKBggqhkjOPQQDAgNJADBGAiEAphAGvWPFTevL7rEy7dBjoTVAk/oT93Mm
qvz6jsUI73ACIQDLLDdqa0x1RevRJ98Y1vQad1mNK9Yk4Oh6k2HkafQ9tg==
-----END CERTIFICATE-----`;
const RELAY_TEST_TLS_KEY = [
  "-----BEGIN PRIVATE KEY-----", // pragma: allowlist secret
  "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgnzhEeRMhzEsaGPOM",
  "xvBVdxlMJ7ANKKYd4P6pIl1KgpWhRANCAASmAvzCvqURJAhoPophOCRgC5RbtmlU",
  "D+1yTQpHs7PiXsFfzgMQeEc11XIEoBij+lEkGgwZ6AQRMvt+EB9s17Sd",
  "-----END PRIVATE KEY-----",
].join("\n");

const PROXY_ENV_KEYS = [
  "HTTPS_PROXY",
  "HTTP_PROXY",
  "ALL_PROXY",
  "NO_PROXY",
  "https_proxy",
  "http_proxy",
  "all_proxy",
  "no_proxy",
] as const;

describe("Slack relay proxy environment", () => {
  const relayUrl = "wss://router.example.com/gateway/ws?gateway_id=pash";

  beforeEach(() => {
    for (const key of PROXY_ENV_KEYS) {
      vi.stubEnv(key, undefined);
    }
  });

  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("dials directly when the environment names no proxy", () => {
    expect(resolveRelayProxyAgent(relayUrl)).toBeUndefined();
    const options = buildRelayWebSocketOptions("secret", relayUrl);
    expect(options).not.toHaveProperty("agent");
    expect(options).toMatchObject({
      headers: { Authorization: "Bearer secret" },
      handshakeTimeout: 30_000,
      maxPayload: SLACK_RELAY_MAX_PAYLOAD_BYTES,
      perMessageDeflate: false,
    });
  });

  it("attaches an env proxy agent to a wss:// dial", () => {
    vi.stubEnv("HTTPS_PROXY", "http://proxy.example.internal:3128");
    const agent = resolveRelayProxyAgent(relayUrl);
    expect(agent).toBeInstanceOf(HttpAgent);
    const options = buildRelayWebSocketOptions("secret", relayUrl);
    expect(options.agent).toBeInstanceOf(HttpAgent);
    expect(options).toMatchObject({
      headers: { Authorization: "Bearer secret" },
      handshakeTimeout: 30_000,
      maxPayload: SLACK_RELAY_MAX_PAYLOAD_BYTES,
      perMessageDeflate: false,
    });
  });

  it("falls back to HTTP_PROXY and lowercase variants for a wss:// dial", () => {
    vi.stubEnv("HTTP_PROXY", "http://proxy.example.internal:3128");
    expect(resolveRelayProxyAgent(relayUrl)).toBeInstanceOf(HttpAgent);
    vi.stubEnv("HTTP_PROXY", undefined);
    vi.stubEnv("https_proxy", "http://proxy.example.internal:3128");
    expect(resolveRelayProxyAgent(relayUrl)).toBeInstanceOf(HttpAgent);
  });

  it("keeps a NO_PROXY match on a direct dial", () => {
    vi.stubEnv("HTTPS_PROXY", "http://proxy.example.internal:3128");
    vi.stubEnv("NO_PROXY", "localhost,.example.com");
    expect(resolveRelayProxyAgent(relayUrl)).toBeUndefined();
    expect(buildRelayWebSocketOptions("secret", relayUrl)).not.toHaveProperty("agent");
    expect(resolveRelayProxyAgent("wss://router.example.net/gateway/ws")).toBeInstanceOf(HttpAgent);
  });

  it("never proxies a plaintext ws:// dial", () => {
    vi.stubEnv("HTTPS_PROXY", "http://proxy.example.internal:3128");
    vi.stubEnv("HTTP_PROXY", "http://proxy.example.internal:3128");
    const localUrl = buildRelayWebSocketUrl({
      url: "ws://127.0.0.1:18080/gateway/ws",
      authToken: "secret",
      gatewayId: "pash",
    });
    expect(resolveRelayProxyAgent(localUrl)).toBeUndefined();
    expect(buildRelayWebSocketOptions("secret", localUrl)).not.toHaveProperty("agent");
  });

  it("dials directly when the proxy URL uses an unsupported protocol", () => {
    vi.stubEnv("HTTPS_PROXY", "socks5://proxy.example.internal:1080");
    expect(resolveRelayProxyAgent(relayUrl)).toBeUndefined();
  });

  it("tunnels the relay upgrade through the env CONNECT proxy and bypasses it on NO_PROXY", async () => {
    const sockets = new Set<Duplex>();
    const track = (socket: Duplex) => {
      sockets.add(socket);
      socket.on("close", () => sockets.delete(socket));
    };
    const tunneledPorts = new Set<number>();
    const upgrades: Array<{ via: "proxy" | "direct"; authorization?: string; url?: string }> = [];
    const connects: string[] = [];

    const relayHttps = https.createServer({ key: RELAY_TEST_TLS_KEY, cert: RELAY_TEST_TLS_CERT });
    relayHttps.on("connection", track);
    const relay = new WebSocketServer({ server: relayHttps, path: "/gateway/ws" });
    relay.on("connection", (socket, request) => {
      upgrades.push({
        via: tunneledPorts.has(request.socket.remotePort ?? -1) ? "proxy" : "direct",
        authorization: request.headers.authorization,
        url: request.url,
      });
      socket.close();
    });

    const proxy = http.createServer((_request, response) => {
      response.writeHead(403).end();
    });
    proxy.on("connection", track);
    proxy.on("connect", (request, clientSocket, head) => {
      track(clientSocket);
      connects.push(request.url ?? "");
      const target = new URL(`http://${request.url}`);
      const targetSocket = net.connect(Number(target.port), target.hostname, () => {
        tunneledPorts.add(targetSocket.localPort ?? -1);
        clientSocket.write("HTTP/1.1 200 Connection Established\r\n\r\n");
        if (head.length > 0) {
          targetSocket.write(head);
        }
        clientSocket.pipe(targetSocket);
        targetSocket.pipe(clientSocket);
      });
      track(targetSocket);
      targetSocket.on("error", () => clientSocket.destroy());
      clientSocket.on("error", () => targetSocket.destroy());
    });

    const listen = (server: http.Server | https.Server) =>
      new Promise<number>((resolve) => {
        server.listen(0, "127.0.0.1", () => resolve((server.address() as AddressInfo).port));
      });
    const relayPort = await listen(relayHttps);
    const proxyPort = await listen(proxy);
    const dial = async () => {
      const url = buildRelayWebSocketUrl({
        url: `https://127.0.0.1:${relayPort}/gateway/ws`,
        authToken: "secret",
        gatewayId: "pash",
      });
      const options = buildRelayWebSocketOptions("secret", url);
      const ws = new WebSocket(url, { ...options, ca: RELAY_TEST_TLS_CERT });
      await new Promise<void>((resolve, reject) => {
        ws.once("open", resolve);
        ws.once("error", reject);
      });
      await new Promise<void>((resolve) => {
        ws.once("close", () => resolve());
      });
      return options;
    };

    try {
      vi.stubEnv("HTTPS_PROXY", `http://127.0.0.1:${proxyPort}`);
      const proxied = await dial();
      expect(connects).toEqual([`127.0.0.1:${relayPort}`]);
      expect(upgrades).toEqual([
        {
          via: "proxy",
          authorization: "Bearer secret",
          url: "/gateway/ws?gateway_id=pash",
        },
      ]);
      expect(proxied.agent).toBeInstanceOf(HttpAgent);

      vi.stubEnv("NO_PROXY", "127.0.0.1");
      const bypassed = await dial();
      expect(bypassed).not.toHaveProperty("agent");
      expect(connects).toHaveLength(1);
      expect(upgrades).toHaveLength(2);
      expect(upgrades[1]).toMatchObject({ via: "direct", authorization: "Bearer secret" });
    } finally {
      for (const socket of sockets) {
        socket.destroy();
      }
      await new Promise<void>((resolve) => {
        relay.close(() => resolve());
      });
      await new Promise<void>((resolve) => {
        relayHttps.close(() => resolve());
      });
      await new Promise<void>((resolve) => {
        proxy.close(() => resolve());
      });
    }
  });
});
