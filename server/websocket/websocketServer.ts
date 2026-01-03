import { WebSocketServer, WebSocket } from 'ws';
import { RPAEngine } from '../../src/core/engine/RPAEngine';
import { ETLPipeline } from '../../src/core/pipeline/ETLPipeline';
import type { WebSocketMessage } from '../../src/types/api.types';

const PORT = parseInt(process.env.WEBSOCKET_PORT || '3001');

/**
 * RPA WebSocket Server
 * Broadcasts real-time events from RPA Engine and ETL Pipeline
 */
class RPAWebSocketServer {
  private wss: WebSocketServer;
  private rpaEngine: RPAEngine;
  private etlPipeline: ETLPipeline;
  private clients: Set<WebSocket>;

  constructor(port: number = PORT) {
    this.wss = new WebSocketServer({ port });
    this.rpaEngine = new RPAEngine();
    this.etlPipeline = new ETLPipeline();
    this.clients = new Set();

    this.initialize();
  }

  /**
   * Initialize WebSocket server and event listeners
   */
  private initialize(): void {
    console.log(`[WebSocket] Server starting on port ${PORT}...`);

    // Handle new connections
    this.wss.on('connection', (ws: WebSocket) => {
      console.log('[WebSocket] New client connected');
      this.clients.add(ws);

      // Send welcome message
      this.sendToClient(ws, {
        event: 'system:status' as any,
        data: {
          status: 'healthy',
          message: 'Connected to RPA WebSocket Server',
          timestamp: new Date(),
        },
        timestamp: new Date(),
      });

      // Handle client messages
      ws.on('message', (data: Buffer) => {
        try {
          const message = JSON.parse(data.toString());
          console.log('[WebSocket] Received message:', message);
        } catch (error) {
          console.error('[WebSocket] Failed to parse message:', error);
        }
      });

      // Handle client disconnect
      ws.on('close', () => {
        console.log('[WebSocket] Client disconnected');
        this.clients.delete(ws);
      });

      // Handle errors
      ws.on('error', (error) => {
        console.error('[WebSocket] Client error:', error);
        this.clients.delete(ws);
      });
    });

    // Set up event listeners from backend core services
    this.setupEventListeners();

    console.log(`[WebSocket] Server listening on ws://localhost:${PORT}`);
  }

  /**
   * Set up event listeners for RPA Engine and ETL Pipeline
   */
  private setupEventListeners(): void {
    // ====================
    // Job Events
    // ====================
    this.rpaEngine.on('job:scheduled', (data: any) => {
      console.log('[WebSocket] Job scheduled:', data.jobId);
      this.broadcast({
        event: 'job:scheduled',
        data,
        timestamp: new Date(),
      });
    });

    this.rpaEngine.on('job:started', (data: any) => {
      console.log('[WebSocket] Job started:', data.jobId);
      this.broadcast({
        event: 'job:started',
        data,
        timestamp: new Date(),
      });
    });

    this.rpaEngine.on('job:completed', (data: any) => {
      console.log('[WebSocket] Job completed:', data.jobId);
      this.broadcast({
        event: 'job:completed',
        data,
        timestamp: new Date(),
      });
    });

    this.rpaEngine.on('job:failed', (data: any) => {
      console.log('[WebSocket] Job failed:', data.jobId);
      this.broadcast({
        event: 'job:failed',
        data,
        timestamp: new Date(),
      });
    });

    this.rpaEngine.on('job:paused', (data: any) => {
      console.log('[WebSocket] Job paused:', data.jobId);
      this.broadcast({
        event: 'job:paused',
        data,
        timestamp: new Date(),
      });
    });

    this.rpaEngine.on('job:resumed', (data: any) => {
      console.log('[WebSocket] Job resumed:', data.jobId);
      this.broadcast({
        event: 'job:resumed',
        data,
        timestamp: new Date(),
      });
    });

    this.rpaEngine.on('job:cancelled', (data: any) => {
      console.log('[WebSocket] Job cancelled:', data.jobId);
      this.broadcast({
        event: 'job:cancelled',
        data,
        timestamp: new Date(),
      });
    });

    // ====================
    // Extraction Events
    // ====================
    this.rpaEngine.on('extraction:started', (data: any) => {
      console.log('[WebSocket] Extraction started:', data.jobId);
      this.broadcast({
        event: 'extraction:started',
        data,
        timestamp: new Date(),
      });
    });

    this.rpaEngine.on('extraction:progress', (data: any) => {
      console.log('[WebSocket] Extraction progress:', data.jobId, data.progress);
      this.broadcast({
        event: 'extraction:progress',
        data,
        timestamp: new Date(),
      });
    });

    this.rpaEngine.on('extraction:completed', (data: any) => {
      console.log('[WebSocket] Extraction completed:', data.jobId);
      this.broadcast({
        event: 'extraction:completed',
        data,
        timestamp: new Date(),
      });
    });

    this.rpaEngine.on('extraction:failed', (data: any) => {
      console.log('[WebSocket] Extraction failed:', data.jobId);
      this.broadcast({
        event: 'extraction:failed',
        data,
        timestamp: new Date(),
      });
    });

    // ====================
    // Pipeline Events
    // ====================
    this.etlPipeline.on('pipeline:started', (data: any) => {
      console.log('[WebSocket] Pipeline started:', data.jobId);
      this.broadcast({
        event: 'pipeline:started',
        data,
        timestamp: new Date(),
      });
    });

    this.etlPipeline.on('pipeline:stage-change', (data: any) => {
      console.log('[WebSocket] Pipeline stage change:', data.stage);
      this.broadcast({
        event: 'pipeline:stage-change',
        data,
        timestamp: new Date(),
      });
    });

    this.etlPipeline.on('pipeline:batch-progress', (data: any) => {
      console.log('[WebSocket] Pipeline batch progress:', data.processed, '/', data.total);
      this.broadcast({
        event: 'pipeline:batch-progress',
        data,
        timestamp: new Date(),
      });
    });

    this.etlPipeline.on('pipeline:completed', (data: any) => {
      console.log('[WebSocket] Pipeline completed:', data.jobId);
      this.broadcast({
        event: 'pipeline:completed',
        data,
        timestamp: new Date(),
      });
    });

    this.etlPipeline.on('pipeline:failed', (data: any) => {
      console.log('[WebSocket] Pipeline failed:', data.jobId);
      this.broadcast({
        event: 'pipeline:failed',
        data,
        timestamp: new Date(),
      });
    });

    // ====================
    // Audit Events
    // ====================
    // Note: Audit logger events can be added here if needed
  }

  /**
   * Broadcast message to all connected clients
   */
  private broadcast(message: WebSocketMessage): void {
    const data = JSON.stringify(message);

    this.clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        try {
          client.send(data);
        } catch (error) {
          console.error('[WebSocket] Failed to send to client:', error);
        }
      }
    });
  }

  /**
   * Send message to a specific client
   */
  private sendToClient(client: WebSocket, message: WebSocketMessage): void {
    if (client.readyState === WebSocket.OPEN) {
      try {
        client.send(JSON.stringify(message));
      } catch (error) {
        console.error('[WebSocket] Failed to send to client:', error);
      }
    }
  }

  /**
   * Shutdown the WebSocket server
   */
  public shutdown(): void {
    console.log('[WebSocket] Shutting down server...');

    this.clients.forEach((client) => {
      client.close();
    });

    this.wss.close(() => {
      console.log('[WebSocket] Server closed');
    });
  }
}

// Start the WebSocket server
const server = new RPAWebSocketServer(PORT);

// Handle process termination
process.on('SIGINT', () => {
  console.log('\n[WebSocket] Received SIGINT, shutting down gracefully...');
  server.shutdown();
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('\n[WebSocket] Received SIGTERM, shutting down gracefully...');
  server.shutdown();
  process.exit(0);
});

export default server;
