import express from 'express';
import http from 'http';
import path from 'path';
import { fileURLToPath } from 'url';
import rangeParser from 'range-parser';
import cors from 'cors';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import { WebSocketServer } from 'ws';
import peerflix from 'peerflix';
import { v4 as uuidv4 } from 'uuid';

// --- Configuration ---
const CONFIG = {
    PORT: process.env.PORT || 6543,
    HOST: process.env.HOST || '0.0.0.0',
    MAX_CONCURRENT_STREAMS: process.env.MAX_STREAMS || 50,
    CLEANUP_INTERVAL: 5 * 60 * 1000, // 5 minutes
    STREAM_TIMEOUT: 30 * 60 * 1000,  // 30 minutes
    TEMP_DIR: process.env.TEMP_DIR || '/tmp/torrent-streams'
};

// --- Setup ---
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// --- Security & Middleware ---
app.use(helmet({
    contentSecurityPolicy: false // Allow media streaming
}));

app.use(cors({
    origin: true, // Allow all origins for multi-platform support
    credentials: true
}));

// Rate limiting for API protection
const limiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 100, // Limit each IP to 100 requests per windowMs
    message: { error: 'Too many requests, please try again later.' }
});
app.use('/api/', limiter);

app.use(express.json());

// Serve static files (for web frontend)
app.use(express.static(path.join(__dirname, 'view')));

// --- Global State Management ---
class StreamManager {
    constructor() {
        this.activeStreams = new Map();
        this.clientSessions = new Map();
        this.startCleanupInterval();
    }

    createStream(magnetLink, clientId) {
        const streamId = uuidv4();
        const timestamp = Date.now();
        
        console.log(`🚀 [${streamId}] Creating stream for client ${clientId}`);
        
        const engine = peerflix(magnetLink, {
            connections: 10,
            uploads: 0,
            path: `${CONFIG.TEMP_DIR}/${streamId}`,
            quiet: false,
            tracker: true,
            dht: false,
            webSeeds: true
        });

        const streamData = {
            id: streamId,
            clientId,
            engine,
            magnetLink,
            status: 'initializing',
            createdAt: timestamp,
            lastAccessed: timestamp,
            videoFile: null,
            metadata: {}
        };

        this.activeStreams.set(streamId, streamData);
        
        // Link client to stream
        if (!this.clientSessions.has(clientId)) {
            this.clientSessions.set(clientId, new Set());
        }
        this.clientSessions.get(clientId).add(streamId);

        this.setupEngineEvents(streamData);
        return streamData;
    }

    setupEngineEvents(streamData) {
        const { id, engine } = streamData;

        engine.on('ready', () => {
            console.log(`✅ [${id}] Engine ready`);
            
            const videoFile = engine.files
                .filter(file => {
                    const name = file.name.toLowerCase();
                    return name.match(/\.(mp4|mkv|avi|mov|wmv|flv|webm)$/);
                })
                .sort((a, b) => b.length - a.length)[0];

            if (!videoFile) {
                streamData.status = 'error';
                streamData.error = 'No video file found';
                console.log(`❌ [${id}] No video file found`);
                return;
            }

            streamData.status = 'ready';
            streamData.videoFile = videoFile;
            streamData.metadata = {
                filename: videoFile.name,
                size: videoFile.length,
                duration: null, // Could be extracted with ffprobe
                bitrate: null
            };

            console.log(`✅ [${id}] Video ready: ${videoFile.name} (${(videoFile.length / 1024 / 1024).toFixed(2)} MB)`);
            this.notifyClients(id, 'stream_ready', streamData.metadata);
        });

        engine.on('error', (err) => {
            console.error(`❌ [${id}] Engine error:`, err.message);
            streamData.status = 'error';
            streamData.error = err.message;
            this.notifyClients(id, 'stream_error', { error: err.message });
        });
    }

    getStream(streamId) {
        const stream = this.activeStreams.get(streamId);
        if (stream) {
            stream.lastAccessed = Date.now();
        }
        return stream;
    }

    destroyStream(streamId, reason = 'manual') {
        const stream = this.activeStreams.get(streamId);
        if (!stream) return false;

        console.log(`🗑️ [${streamId}] Destroying stream (${reason})`);
        
        try {
            stream.engine.destroy();
        } catch (e) {
            console.error(`Error destroying engine: ${e.message}`);
        }

        // Remove from client sessions
        if (this.clientSessions.has(stream.clientId)) {
            this.clientSessions.get(stream.clientId).delete(streamId);
            if (this.clientSessions.get(stream.clientId).size === 0) {
                this.clientSessions.delete(stream.clientId);
            }
        }

        this.activeStreams.delete(streamId);
        this.notifyClients(streamId, 'stream_destroyed', { reason });
        return true;
    }

    destroyClientStreams(clientId) {
        const clientStreams = this.clientSessions.get(clientId);
        if (!clientStreams) return 0;

        let destroyed = 0;
        for (const streamId of clientStreams) {
            if (this.destroyStream(streamId, 'client_disconnect')) {
                destroyed++;
            }
        }
        return destroyed;
    }

    notifyClients(streamId, event, data) {
        const message = JSON.stringify({
            type: event,
            streamId,
            data,
            timestamp: Date.now()
        });

        wss.clients.forEach(client => {
            if (client.readyState === client.OPEN) {
                try {
                    client.send(message);
                } catch (e) {
                    console.error('Error sending WebSocket message:', e.message);
                }
            }
        });
    }

    startCleanupInterval() {
        setInterval(() => {
            const now = Date.now();
            let cleaned = 0;

            for (const [streamId, stream] of this.activeStreams) {
                const age = now - stream.lastAccessed;
                if (age > CONFIG.STREAM_TIMEOUT) {
                    this.destroyStream(streamId, 'timeout');
                    cleaned++;
                }
            }

            if (cleaned > 0) {
                console.log(`🧹 Cleaned up ${cleaned} inactive streams`);
            }
        }, CONFIG.CLEANUP_INTERVAL);
    }

    getStats() {
        return {
            activeStreams: this.activeStreams.size,
            connectedClients: this.clientSessions.size,
            totalConnections: wss.clients.size,
            uptime: process.uptime()
        };
    }
}

const streamManager = new StreamManager();

// --- REST API Routes (for all platforms) ---

// Health check
app.get('/api/health', (req, res) => {
    res.json({
        status: 'healthy',
        timestamp: Date.now(),
        stats: streamManager.getStats()
    });
});

// Create new stream
app.post('/api/streams', (req, res) => {
    const { magnetLink, clientId } = req.body;
    
    if (!magnetLink || !clientId) {
        return res.status(400).json({
            error: 'magnetLink and clientId are required'
        });
    }

    try {
        const stream = streamManager.createStream(magnetLink, clientId);
        res.status(201).json({
            streamId: stream.id,
            status: stream.status,
            createdAt: stream.createdAt
        });
    } catch (error) {
        console.error('Error creating stream:', error);
        res.status(500).json({
            error: 'Failed to create stream'
        });
    }
});

// Get stream info
app.get('/api/streams/:streamId', (req, res) => {
    const { streamId } = req.params;
    const stream = streamManager.getStream(streamId);
    
    if (!stream) {
        return res.status(404).json({
            error: 'Stream not found'
        });
    }

    res.json({
        streamId: stream.id,
        status: stream.status,
        metadata: stream.metadata,
        createdAt: stream.createdAt,
        lastAccessed: stream.lastAccessed
    });
});

// Delete stream
app.delete('/api/streams/:streamId', (req, res) => {
    const { streamId } = req.params;
    const destroyed = streamManager.destroyStream(streamId);
    
    if (!destroyed) {
        return res.status(404).json({
            error: 'Stream not found'
        });
    }

    res.json({
        message: 'Stream destroyed successfully'
    });
});

// List all streams (admin endpoint)
app.get('/api/streams', (req, res) => {
    const streams = Array.from(streamManager.activeStreams.values()).map(stream => ({
        streamId: stream.id,
        status: stream.status,
        metadata: stream.metadata,
        createdAt: stream.createdAt,
        lastAccessed: stream.lastAccessed
    }));

    res.json({
        streams,
        total: streams.length
    });
});

// --- Video Streaming Endpoint ---
app.get('/stream/:streamId', (req, res) => {
    const { streamId } = req.params;
    const stream = streamManager.getStream(streamId);

    if (!stream || stream.status !== 'ready' || !stream.videoFile) {
        return res.status(404).json({
            error: 'Stream not ready or not found'
        });
    }

    const videoFile = stream.videoFile;
    const fileSize = videoFile.length;
    const rangeHeader = req.headers.range;

    console.log(`📺 [${streamId}] Streaming request from ${req.ip}`);

    // Handle range requests (crucial for mobile/TV)
    if (rangeHeader) {
        const ranges = rangeParser(fileSize, rangeHeader);

        if (ranges === -1 || ranges === -2) {
            return res.status(416).json({ error: 'Invalid range' });
        }

        const { start, end } = ranges[0];
        const contentLength = end - start + 1;

        res.status(206);
        res.setHeader('Content-Length', contentLength);
        res.setHeader('Content-Range', `bytes ${start}-${end}/${fileSize}`);
        res.setHeader('Accept-Ranges', 'bytes');
        res.setHeader('Content-Type', 'video/mp4');
        
        // CORS headers for cross-platform access
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Access-Control-Allow-Headers', 'Range');

        const streamInstance = videoFile.createReadStream({ start, end });
        streamInstance.on('error', (err) => {
            console.error(`❌ [${streamId}] Stream error:`, err.message);
        });

        streamInstance.pipe(res);
    } else {
        res.status(200);
        res.setHeader('Content-Length', fileSize);
        res.setHeader('Content-Type', 'video/mp4');
        res.setHeader('Accept-Ranges', 'bytes');
        res.setHeader('Access-Control-Allow-Origin', '*');

        const streamInstance = videoFile.createReadStream();
        streamInstance.on('error', (err) => {
            console.error(`❌ [${streamId}] Stream error:`, err.message);
        });

        streamInstance.pipe(res);
    }
});

// --- WebSocket for Real-time Updates ---
wss.on('connection', (ws, req) => {
    const clientId = uuidv4();
    ws.clientId = clientId;
    
    console.log(`🔌 Client connected: ${clientId} from ${req.socket.remoteAddress}`);
    
    ws.send(JSON.stringify({
        type: 'connected',
        clientId,
        timestamp: Date.now()
    }));

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            
            switch (data.type) {
                case 'create_stream':
                    if (data.magnetLink) {
                        const stream = streamManager.createStream(data.magnetLink, clientId);
                        ws.send(JSON.stringify({
                            type: 'stream_created',
                            streamId: stream.id,
                            timestamp: Date.now()
                        }));
                    }
                    break;
                    
                case 'ping':
                    ws.send(JSON.stringify({
                        type: 'pong',
                        timestamp: Date.now()
                    }));
                    break;
            }
        } catch (e) {
            console.error('Invalid WebSocket message:', e.message);
        }
    });

    ws.on('close', () => {
        console.log(`🔌 Client disconnected: ${clientId}`);
        const destroyed = streamManager.destroyClientStreams(clientId);
        if (destroyed > 0) {
            console.log(`🧹 Cleaned up ${destroyed} streams for client ${clientId}`);
        }
    });

    ws.on('error', (err) => {
        console.error('WebSocket error:', err.message);
    });
});

// --- Error Handling ---
app.use((err, req, res, next) => {
    console.error('Unhandled error:', err);
    res.status(500).json({
        error: 'Internal server error',
        timestamp: Date.now()
    });
});

// 404 handler
app.use((req, res) => {
    res.status(404).json({
        error: 'Endpoint not found',
        timestamp: Date.now()
    });
});

// --- Start Server ---
server.listen(CONFIG.PORT, CONFIG.HOST, () => {
    console.log(`🚀 Multi-platform streaming API running at http://${CONFIG.HOST}:${CONFIG.PORT}`);
    console.log(`📱 Ready for Web, Mobile, and TV clients`);
    console.log(`🔧 Max concurrent streams: ${CONFIG.MAX_CONCURRENT_STREAMS}`);
});

// Global error handlers
process.on('uncaughtException', (error) => {
    console.error('🔥 Uncaught Exception:', error.message);
});

process.on('unhandledRejection', (reason) => {
    console.error('🔥 Unhandled Rejection:', reason);
});

console.log('🛡️ Global error handlers installed');
console.log('🌐 API ready for multi-platform access');
