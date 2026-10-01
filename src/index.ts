export interface Env {
  GAME_ROOM: DurableObjectNamespace;
}

const CORS_HEADERS: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...CORS_HEADERS
    }
  });
}

function normalizeRoomCode(raw: string | null): string {
  return (raw || "DEFAULT").trim().toUpperCase();
}

function isValidRoomCode(roomCode: string): boolean {
  return /^[A-Z0-9_-]{1,32}$/.test(roomCode);
}

export default {
  async fetch(request: Request, env: Env, _ctx: ExecutionContext): Promise<Response> {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    const url = new URL(request.url);

    if (url.pathname === "/" || url.pathname === "/health" || url.pathname === "/api/health") {
      return jsonResponse({
        ok: true,
        service: "sjyw-online-worker",
        stage: "websocket-room-skeleton",
        protocol: "lockstep-preview",
        time: new Date().toISOString()
      });
    }

    if (url.pathname === "/ws") {
      const roomCode = normalizeRoomCode(url.searchParams.get("room"));
      if (!isValidRoomCode(roomCode)) {
        return jsonResponse({ ok: false, error: "INVALID_ROOM_CODE" }, 400);
      }

      const id = env.GAME_ROOM.idFromName(roomCode);
      const stub = env.GAME_ROOM.get(id);
      return stub.fetch(request);
    }

    return jsonResponse({ ok: false, error: "NOT_FOUND", path: url.pathname }, 404);
  }
};

export class GameRoom {
  private state: DurableObjectState;
  private seq: number;

  constructor(state: DurableObjectState, _env: Env) {
    this.state = state;
    this.seq = 0;
  }

  async fetch(request: Request): Promise<Response> {
    if (request.headers.get("Upgrade") !== "websocket") {
      return jsonResponse({ ok: false, error: "WEBSOCKET_UPGRADE_REQUIRED" }, 426);
    }

    const url = new URL(request.url);
    const roomCode = normalizeRoomCode(url.searchParams.get("room"));
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    this.state.acceptWebSocket(server);

    const players = this.state.getWebSockets().length;
    this.send(server, {
      type: "welcome",
      room: roomCode,
      players,
      message: "已连接到联机房间骨架。当前版本只验证 WebSocket/DO/部署链路。"
    });

    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): void {
    if (typeof message !== "string") {
      this.send(ws, { type: "error", error: "TEXT_JSON_ONLY" });
      return;
    }

    let data: unknown;
    try {
      data = JSON.parse(message);
    } catch {
      this.send(ws, { type: "error", error: "INVALID_JSON" });
      return;
    }

    const typed = data as { type?: unknown; t?: unknown; playerName?: unknown; payload?: unknown };
    const type = typeof typed.type === "string" ? typed.type : "message";

    if (type === "ping") {
      this.send(ws, { type: "pong", t: typed.t ?? null, players: this.state.getWebSockets().length });
      return;
    }

    this.seq += 1;
    const envelope = {
      type: "broadcast",
      seq: this.seq,
      roomPlayerName: typeof typed.playerName === "string" ? typed.playerName : "anonymous",
      payload: typed.payload ?? null,
      players: this.state.getWebSockets().length
    };

    for (const socket of this.state.getWebSockets()) {
      if (socket !== ws) {
        socket.send(JSON.stringify(envelope));
      }
    }

    this.send(ws, { type: "ack", seq: this.seq, players: envelope.players });
  }

  webSocketClose(_ws: WebSocket, _code: number, _reason: string, _wasClean: boolean): void {
    // 当前骨架不保存房间状态；后续在这里接玩家离开与房间清理。
  }

  webSocketError(ws: WebSocket, _error: unknown): void {
    try {
      ws.close(1011, "SERVER_ERROR");
    } catch {
      // ignore close error
    }
  }

  private send(ws: WebSocket, data: unknown): void {
    try {
      ws.send(JSON.stringify(data));
    } catch {
      // ignore disconnected socket
    }
  }
}
