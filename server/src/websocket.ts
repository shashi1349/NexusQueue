import { WebSocketServer, WebSocket } from 'ws';
import type { Server } from 'http';
import type { Redis } from 'ioredis';
import { redisKeys } from '@nexusqueue/shared';

export interface JobEvent {
  type: string;
  jobId: string;
  jobName: string;
  queueName: string;
  timestamp: number;
  [key: string]: unknown;
}

export class NexusEventBus {
  private wss: WebSocketServer;
  private subscriber: Redis;
  private publisher: Redis;

  constructor(server: Server, subscriber: Redis, publisher: Redis) {
    this.subscriber = subscriber;
    this.publisher = publisher;
    this.wss = new WebSocketServer({ server });

    // Subscribe to events channel
    void this.subscriber.subscribe(redisKeys.events);
    this.subscriber.on('message', (_channel: string, message: string) => {
      // Broadcast to all connected clients
      for (const client of this.wss.clients) {
        if (client.readyState === WebSocket.OPEN) {
          client.send(message);
        }
      }
    });
  }

  async publish(event: JobEvent): Promise<void> {
    await this.publisher.publish(redisKeys.events, JSON.stringify(event));
  }

  close(): void {
    this.wss.close();
  }
}
