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
import fs from 'fs';

// --- Configuration ---
const CONFIG = {
    PORT: process.env.PORT || 6543,
    HOST: process.env.HOST || '0.0.0.0',
    MAX_CONCURRENT_STREAMS: parseInt(process.env.MAX_STREAMS) || 10, // Reduced from 50
    MAX_STREAMS_PER_CLIENT: 2, // New: limit per client
    CLEANUP_INTERVAL: 2 * 60 * 1000, // 2 minutes (more frequent)
    STREAM_TIMEOUT: 10 * 60 * 1000,  // 10 minutes (shorter timeout)
    TEMP_DIR: process.env.TEMP_DIR || '/tmp/torrent-streams',
    MAX_DISK_USAGE: 5 * 1024 * 1024 * 1024, // 5GB max storage
};

// --- Setup ---
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// --- Smarter Rate Limiting ---
// Different limits for different types of operations

// General API browsing (health, list streams)
const generalApiLimiter = rateLimit({
    windowMs: 1 * 60 * 1000, // 1 minute
    max: 100, // Increased for general API calls
    message: { error: 'API rate limit exceeded. Please slow down.' },
    standardHeaders: true,
    legacyHeaders: false,
});

// Stream creation (most expensive operation)
const streamCreateLimiter = rateLimit({
    windowMs: 5 * 60 * 1000, // 5 minutes
    max: 5, // Only 5 stream creations per 5 minutes per IP
    message: { error: 'Stream creation limit exceeded. Please wait before creating more streams.' },
    standardHeaders: true,
    legacyHeaders: false,
});

// Stream status polling (frequent but lightweight)
const streamStatusLimiter = rateLimit({
    windowMs: 1 * 60 * 1000, // 1 minute
    max: 60, // 60 status checks per minute (1 per second average)
    message: { error: 'Status polling too frequent. Please reduce polling rate.' },
    standardHeaders: true,
    legacyHeaders: false,
    keyGenerator: (req) => {
        // Allow higher limits for status checks of own streams
        return `status-${req.ip}-${req.params.streamId || 'general'}`;
    }
});

// Video streaming (should be unlimited for smooth playback)
const streamingLimiter = rateLimit({
    windowMs: 1 * 60 * 1000, // 1 minute
    max: 1000, // Very high limit for video chunk requests
    message: { error: 'Streaming rate limit exceeded.' },
    standardHeaders: false, // Don't add headers to video responses
    legacyHeaders: false,
    skip: (req) => {
        // Skip rate limiting for range requests (video chunks)
        return req.headers.range !== undefined;
    }
});

// --- Security & Middleware ---
app.use(helmet({
    contentSecurityPolicy: false
}));

app.use(cors({
    origin: true,
    credentials: true
}));

// Apply rate limiting
app.use('/api/health', generalApiLimiter);
app.use('/api/streams', (req, res, next) => {
    if (req.method === 'POST') {
        streamCreateLimiter(req, res, next);
    } else {
        generalApiLimiter(req, res, next);
    }
});

app.use(express.json());
app.use(express.static(path.join(__dirname, 'view')));

// --- Enhanced Stream Manager with Resource Management ---
class StreamManager {
    constructor() {
        this.activeStreams = new Map();
        this.clientSessions = new Map();
        this.diskUsage = 0;
        this.startCleanupInterval();
        this.ensureTempDir();
    }

    ensureTempDir() {
        if (!fs.existsSync(CONFIG.TEMP_DIR)) {
            fs.mkdirSync(CONFIG.TEMP_DIR, { recursive: true });
            console.log(`📁 Created temp directory: ${CONFIG.TEMP_DIR}`);
        }
    }

    // Check if we can create a new stream
    canCreateStream(clientId) {
        const reasons = [];

        // Check global stream limit
        if (this.activeStreams.size >= CONFIG.MAX_CONCURRENT_STREAMS) {
            reasons.push(`Global limit reached (${CONFIG.MAX_CONCURRENT_STREAMS} streams)`);
        }

        // Check per-client limit
        const clientStreams = this.clientSessions.get(clientId);
        if (clientStreams && clientStreams.size >= CONFIG.MAX_STREAMS_PER_CLIENT) {
            reasons.push(`Client limit reached (${CONFIG.MAX_STREAMS_PER_CLIENT} streams per client)`);
        }

        // Check disk usage
        if (this.diskUsage > CONFIG.MAX_DISK_USAGE) {
            reasons.push(`Disk usage limit reached (${(this.diskUsage / 1024 / 1024 / 1024).toFixed(2)}GB)`);
        }

        return {
            allowed: reasons.length === 0,
            reasons: reasons
        };
    }

    createStream(magnetLink, clientId) {
        // Check if stream creation is allowed
        const canCreate = this.canCreateStream(clientId);
        if (!canCreate.allowed) {
            throw new Error(`Cannot create stream: ${canCreate.reasons.join(', ')}`);
        }

        const streamId = uuidv4();
        const timestamp = Date.now();
        const streamPath = `${CONFIG.TEMP_DIR}/${streamId}`;
        
        // Calculate dynamic connection limit based on current load
        const currentLoad = this.activeStreams.size;
        const maxLoad = CONFIG.MAX_CONCURRENT_STREAMS;
        const loadPercentage = currentLoad / maxLoad;
        
        // Dynamic connection scaling:
        // - Low load (0-30%): 50 connections (fast)
        // - Medium load (30-70%): 25 connections (balanced)
        // - High load (70-100%): 10 connections (conservative)
        let connectionLimit;
        if (loadPercentage <= 0.3) {
            connectionLimit = 50; // Fast streaming when server has capacity
        } else if (loadPercentage <= 0.7) {
            connectionLimit = 25; // Balanced performance
        } else {
            connectionLimit = 10; // Conservative when under heavy load
        }
        
        console.log(`🚀 [${streamId}] Creating stream for client ${clientId}`);
        console.log(`📊 Current load: ${currentLoad}/${maxLoad} (${(loadPercentage * 100).toFixed(1)}%) - Using ${connectionLimit} connections`);
        
        const engine = peerflix(magnetLink, {
            connections: connectionLimit, // Dynamic based on server load
            uploads: 0, // Still disable uploads to save bandwidth
            path: streamPath,
            quiet: false,
            tracker: true,
            dht: false,
            webSeeds: true,
            // Additional performance options
            blocklist: false, // Disable IP blocklist for more peers
            verify: false,    // Skip hash verification for faster startup
            // Download strategy for streaming
            strategy: 'rarest' // or 'sequential' for streaming
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
            metadata: {},
            path: streamPath,
            diskUsage: 0
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
                duration: null,
                bitrate: null
            };

            console.log(`✅ [${id}] Video ready: ${videoFile.name} (${(videoFile.length / 1024 / 1024).toFixed(2)} MB)`);
            this.notifyClients(id, 'stream_ready', streamData.metadata);
        });

        // Monitor download progress and disk usage
        engine.on('download', () => {
            this.updateDiskUsage(streamData);
        });

        engine.on('error', (err) => {
            console.error(`❌ [${id}] Engine error:`, err.message);
            streamData.status = 'error';
            streamData.error = err.message;
            this.notifyClients(id, 'stream_error', { error: err.message });
        });
    }

    updateDiskUsage(streamData) {
        try {
            if (fs.existsSync(streamData.path)) {
                const stats = fs.statSync(streamData.path);
                const newUsage = stats.size;
                
                // Update global disk usage
                this.diskUsage = this.diskUsage - streamData.diskUsage + newUsage;
                streamData.diskUsage = newUsage;
                
                // Log significant changes
                if (newUsage > streamData.diskUsage + 10 * 1024 * 1024) { // Every 10MB
                    console.log(`💾 [${streamData.id}] Downloaded: ${(newUsage / 1024 / 1024).toFixed(2)}MB`);
                }
            }
        } catch (error) {
            console.error(`Error checking disk usage for ${streamData.id}:`, error.message);
        }
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
            // Stop the engine
            stream.engine.destroy();
            
            // Clean up downloaded files
            this.cleanupStreamFiles(stream);
            
        } catch (e) {
            console.error(`Error destroying engine: ${e.message}`);
        }

        // Update disk usage
        this.diskUsage -= stream.diskUsage;

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

    cleanupStreamFiles(stream) {
        try {
            if (fs.existsSync(stream.path)) {
                fs.rmSync(stream.path, { recursive: true, force: true });
                console.log(`🧹 [${stream.id}] Cleaned up files: ${stream.path}`);
            }
        } catch (error) {
            console.error(`Error cleaning up files for ${stream.id}:`, error.message);
        }
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

            // Log current resource usage
            console.log(`📊 Resource usage: ${this.activeStreams.size}/${CONFIG.MAX_CONCURRENT_STREAMS} streams, ${(this.diskUsage / 1024 / 1024).toFixed(2)}MB disk`);
            
        }, CONFIG.CLEANUP_INTERVAL);
    }

    getStats() {
        return {
            activeStreams: this.activeStreams.size,
            maxStreams: CONFIG.MAX_CONCURRENT_STREAMS,
            connectedClients: this.clientSessions.size,
            totalConnections: wss.clients.size,
            diskUsageBytes: this.diskUsage,
            diskUsageMB: Math.round(this.diskUsage / 1024 / 1024),
            maxDiskUsageMB: Math.round(CONFIG.MAX_DISK_USAGE / 1024 / 1024),
            uptime: process.uptime()
        };
    }
}

const streamManager = new StreamManager();

// --- REST API Routes ---

// Health check with resource info
app.get('/api/health', (req, res) => {
    res.json({
        status: 'healthy',
        timestamp: Date.now(),
        stats: streamManager.getStats()
    });
});

// Create new stream with limits
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
            createdAt: stream.createdAt,
            limits: {
                maxConcurrentStreams: CONFIG.MAX_CONCURRENT_STREAMS,
                maxStreamsPerClient: CONFIG.MAX_STREAMS_PER_CLIENT,
                streamTimeout: CONFIG.STREAM_TIMEOUT
            }
        });
    } catch (error) {
        console.error('Error creating stream:', error.message);
        res.status(429).json({ // 429 = Too Many Requests
            error: error.message,
            stats: streamManager.getStats()
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
            console.log(`📨 WebSocket message from ${clientId}:`, data.type);
            
            switch (data.type) {
                case 'create_stream':
                    handleCreateStreamWS(data, ws, clientId);
                    break;
                    
                case 'destroy_stream':
                    handleDestroyStreamWS(data, ws, clientId);
                    break;
                    
                case 'ping':
                    ws.send(JSON.stringify({
                        type: 'pong',
                        timestamp: Date.now()
                    }));
                    break;
                    
                default:
                    console.log(`❓ Unknown WebSocket message type: ${data.type}`);
            }
        } catch (e) {
            console.error('Invalid WebSocket message:', e.message);
            ws.send(JSON.stringify({
                type: 'error',
                message: 'Invalid message format'
            }));
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

// WebSocket message handlers
function handleCreateStreamWS(data, ws, clientId) {
    try {
        const { magnetLink } = data;
        
        if (!magnetLink) {
            ws.send(JSON.stringify({
                type: 'error',
                message: 'magnetLink is required'
            }));
            return;
        }

        console.log(`🚀 Creating stream via WebSocket for client ${clientId}`);
        
        // Use the same logic as REST API
        const stream = streamManager.createStream(magnetLink, clientId);
        
        // Send success response
        ws.send(JSON.stringify({
            type: 'stream_created',
            streamId: stream.id,
            status: stream.status,
            timestamp: Date.now()
        }));
        
        console.log(`✅ Stream ${stream.id} created via WebSocket`);
        
    } catch (error) {
        console.error('WebSocket stream creation error:', error.message);
        ws.send(JSON.stringify({
            type: 'error',
            message: error.message
        }));
    }
}

function handleDestroyStreamWS(data, ws, clientId) {
    try {
        const { streamId } = data;
        
        if (!streamId) {
            ws.send(JSON.stringify({
                type: 'error',
                message: 'streamId is required'
            }));
            return;
        }

        console.log(`🗑️ Destroying stream ${streamId} via WebSocket`);
        
        const destroyed = streamManager.destroyStream(streamId, 'websocket_request');
        
        if (destroyed) {
            ws.send(JSON.stringify({
                type: 'stream_destroyed',
                streamId: streamId,
                timestamp: Date.now()
            }));
            console.log(`✅ Stream ${streamId} destroyed via WebSocket`);
        } else {
            ws.send(JSON.stringify({
                type: 'error',
                message: 'Stream not found or could not be destroyed'
            }));
        }
        
    } catch (error) {
        console.error('WebSocket stream destruction error:', error.message);
        ws.send(JSON.stringify({
            type: 'error',
            message: error.message
        }));
    }
}

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
    console.log(`📊 Limits: ${CONFIG.MAX_CONCURRENT_STREAMS} concurrent streams, ${CONFIG.MAX_STREAMS_PER_CLIENT} per client`);
    console.log(`💾 Max disk usage: ${(CONFIG.MAX_DISK_USAGE / 1024 / 1024 / 1024).toFixed(2)}GB`);
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
