import type { JobEvent } from './types.js';

const WS_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:3000';

type Listener = (event: JobEvent) => void;

class WebSocketManager {
  private ws: WebSocket | null = null;
  private listeners: Set<Listener> = new Set();
  private reconnectDelay = 1000;
  private maxReconnectDelay = 30000;
  private shouldReconnect = true;

  connect(): void {
    if (this.ws) return;
    this.shouldReconnect = true;

    try {
      this.ws = new WebSocket(WS_URL);

      this.ws.onopen = () => {
        this.reconnectDelay = 1000;
      };

      this.ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data as string) as JobEvent;
          this.listeners.forEach((listener) => listener(data));
        } catch {
          // ignore malformed messages
        }
      };

      this.ws.onclose = () => {
        this.ws = null;
        if (this.shouldReconnect) {
          setTimeout(() => this.connect(), this.reconnectDelay);
          this.reconnectDelay = Math.min(this.reconnectDelay * 2, this.maxReconnectDelay);
        }
      };

      this.ws.onerror = () => {
        this.ws?.close();
      };
    } catch {
      this.ws = null;
      if (this.shouldReconnect) {
        setTimeout(() => this.connect(), this.reconnectDelay);
        this.reconnectDelay = Math.min(this.reconnectDelay * 2, this.maxReconnectDelay);
      }
    }
  }

  disconnect(): void {
    this.shouldReconnect = false;
    this.ws?.close();
    this.ws = null;
  }

  subscribe(callback: Listener): () => void {
    this.listeners.add(callback);
    if (!this.ws) {
      this.connect();
    }
    return () => {
      this.listeners.delete(callback);
    };
  }
}

export const wsManager = new WebSocketManager();
