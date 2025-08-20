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
    MAX_CONCURRENT_STREAMS: parseInt(process.env.MAX_STREAMS) || 10,
    MAX_STREAMS_PER_CLIENT: 2,
    CLEANUP_INTERVAL: 2 * 60 * 1000, // Check every 2 minutes
    STREAM_INACTIVE_TIMEOUT: 30 * 60 * 1000, // 30 minutes without video access
    CONNECTION_TIMEOUT: 60 * 60 * 1000, // 1 hour for WebSocket connections
    TEMP_DIR: process.env.TEMP_DIR || '/tmp/torrent-streams',
    MAX_DISK_USAGE: 5 * 1024 * 1024 * 1024,
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

// --- Enhanced Stream Manager with Server-Side Control ---
class StreamManager {
    constructor() {
        this.activeStreams = new Map();
        this.clientSessions = new Map();
        this.clientConnections = new Map(); // Track WebSocket connections
        this.diskUsage = 0;
        this.startMaintenanceInterval();
        this.ensureTempDir();
    }

    ensureTempDir() {
        if (!fs.existsSync(CONFIG.TEMP_DIR)) {
            fs.mkdirSync(CONFIG.TEMP_DIR, { recursive: true });
            console.log(`📁 Created temp directory: ${CONFIG.TEMP_DIR}`);
        }
    }

    // Track client connections
    addClientConnection(clientId, ws) {
        this.clientConnections.set(clientId, {
            ws: ws,
            connectedAt: Date.now(),
            lastActivity: Date.now(),
            isActive: true
        });
        console.log(`🔌 [${clientId}] Client connection tracked`);
    }

    // Update client activity
    updateClientActivity(clientId) {
        const connection = this.clientConnections.get(clientId);
        if (connection) {
            connection.lastActivity = Date.now();
            connection.isActive = true;
        }
    }

    // Remove client connection
    removeClientConnection(clientId) {
        this.clientConnections.delete(clientId);
        console.log(`🔌 [${clientId}] Client connection removed`);
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

        // Initialize torrent stats
        streamData.torrentStats = {
            peers: 0,
            seeders: 0,
            leechers: 0,
            downloadSpeed: 0,
            uploadSpeed: 0,
            downloaded: 0,
            uploaded: 0,
            progress: 0,
            ratio: 0,
            eta: 0,
            health: 'unknown'
        };

        engine.on('ready', () => {
            console.log(`✅ [${id}] Engine ready`);
            console.log(`📁 [${id}] Total files in torrent: ${engine.files.length}`);
            console.log(`🌐 [${id}] Torrent info hash: ${engine.torrent.infoHash}`);
            console.log(`📦 [${id}] Total size: ${(engine.torrent.length / 1024 / 1024).toFixed(2)}MB`);
            
            // Log all files for debugging
            engine.files.forEach((file, index) => {
                console.log(`📄 [${id}] File ${index}: ${file.name} (${(file.length / 1024 / 1024).toFixed(2)}MB)`);
            });
            
            // Start torrent monitoring
            this.startTorrentMonitoring(streamData);
            
            const videoFile = this.findBestVideoFile(engine.files, id);

            if (!videoFile) {
                streamData.status = 'error';
                streamData.error = 'No suitable video file found';
                console.log(`❌ [${id}] No video file found`);
                this.notifyClients(id, 'stream_error', { error: 'No suitable video file found' });
                return;
            }

            streamData.status = 'ready';
            streamData.videoFile = videoFile;
            streamData.metadata = {
                filename: videoFile.name,
                size: videoFile.length,
                duration: null,
                bitrate: null,
                container: path.extname(videoFile.name).toLowerCase(),
                isInFolder: videoFile.name.includes('/') || videoFile.name.includes('\\'),
                torrentHash: engine.torrent.infoHash,
                totalTorrentSize: engine.torrent.length
            };

            console.log(`✅ [${id}] Video ready: ${videoFile.name} (${(videoFile.length / 1024 / 1024).toFixed(2)} MB)`);
            console.log(`📦 [${id}] Container: ${streamData.metadata.container}, In folder: ${streamData.metadata.isInFolder}`);
            
            this.notifyClients(id, 'stream_ready', streamData.metadata);
        });

        // Enhanced download monitoring
        engine.on('download', (pieceIndex) => {
            this.updateDiskUsage(streamData);
            this.updateTorrentStats(streamData);
        });

        // Peer connection events
        engine.on('peer', (peer) => {
            console.log(`👥 [${id}] New peer connected: ${peer.remoteAddress}`);
            this.updateTorrentStats(streamData);
        });

        // Upload events
        engine.on('upload', (pieceIndex, offset, length) => {
            this.updateTorrentStats(streamData);
        });

        engine.on('error', (err) => {
            console.error(`❌ [${id}] Engine error:`, err.message);
            streamData.status = 'error';
            streamData.error = err.message;
            this.notifyClients(id, 'stream_error', { error: err.message });
        });
    }

    // Add comprehensive torrent monitoring
    startTorrentMonitoring(streamData) {
        const { id } = streamData;
        
        streamData.monitoringInterval = setInterval(() => {
            this.updateTorrentStats(streamData);
            
            // Send periodic updates to clients
            this.notifyClients(id, 'torrent_stats', streamData.torrentStats);
        }, 2000); // Update every 2 seconds
    }

    updateTorrentStats(streamData) {
        const { engine } = streamData;
        
        if (!engine || !engine.swarm) return;

        const swarm = engine.swarm;
        const torrent = engine.torrent;
        
        // Calculate basic stats
        const downloaded = swarm.downloaded;
        const uploaded = swarm.uploaded;
        const totalLength = torrent.length;
        const progress = totalLength > 0 ? (downloaded / totalLength) * 100 : 0;
        
        // Peer statistics
        const peers = swarm.wires || [];
        const activePeers = peers.filter(peer => !peer.peerChoking).length;
        const seeders = peers.filter(peer => peer.peerPieces && peer.peerPieces.buffer.toString('hex').indexOf('00') === -1).length;
        const leechers = peers.length - seeders;
        
        // Speed calculations (bytes per second)
        const downloadSpeed = swarm.downloadSpeed() || 0;
        const uploadSpeed = swarm.uploadSpeed() || 0;
        
        // ETA calculation
        const remaining = totalLength - downloaded;
        const eta = downloadSpeed > 0 ? remaining / downloadSpeed : 0;
        
        // Health calculation (based on seeders/leechers ratio and download speed)
        let health = 'unknown';
        if (seeders > 10) {
            health = 'excellent';
        } else if (seeders > 5) {
            health = 'good';
        } else if (seeders > 1) {
            health = 'fair';
        } else if (seeders === 1) {
            health = 'poor';
        } else {
            health = 'critical';
        }
        
        // Update stats
        streamData.torrentStats = {
            peers: peers.length,
            activePeers: activePeers,
            seeders: seeders,
            leechers: leechers,
            downloadSpeed: downloadSpeed,
            uploadSpeed: uploadSpeed,
            downloaded: downloaded,
            uploaded: uploaded,
            progress: Math.round(progress * 100) / 100,
            ratio: downloaded > 0 ? uploaded / downloaded : 0,
            eta: eta,
            health: health,
            totalSize: totalLength,
            remaining: remaining,
            // Additional detailed stats
            pieces: {
                total: torrent.pieces ? torrent.pieces.length : 0,
                downloaded: swarm.downloaded ? Math.floor(downloaded / torrent.pieceLength) : 0
            },
            bandwidth: {
                downloadSpeedFormatted: this.formatBytes(downloadSpeed) + '/s',
                uploadSpeedFormatted: this.formatBytes(uploadSpeed) + '/s'
            }
        };

        // Log significant changes
        if (streamData.lastLoggedProgress === undefined || 
            Math.abs(progress - streamData.lastLoggedProgress) >= 5) {
            console.log(`📊 [${streamData.id}] Progress: ${progress.toFixed(1)}%, Peers: ${peers.length} (${seeders}S/${leechers}L), Speed: ${this.formatBytes(downloadSpeed)}/s, Health: ${health}`);
            streamData.lastLoggedProgress = progress;
        }
    }

    // Helper function to format bytes
    formatBytes(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    getStream(streamId) {
        const stream = this.activeStreams.get(streamId);
        if (stream) {
            // Track ALL access activity server-side
            stream.lastAccessed = Date.now();
            stream.accessCount = (stream.accessCount || 0) + 1;
            
            // Update client activity when accessing streams
            this.updateClientActivity(stream.clientId);
            
            // Track video streaming specifically
            if (stream.status === 'ready') {
                stream.lastVideoAccess = Date.now();
                stream.isActivelyStreaming = true;
                console.log(`📺 [${streamId}] Video access tracked server-side`);
            }
        }
        return stream;
    }

    // Comprehensive maintenance with smart cleanup rules
    startMaintenanceInterval() {
        setInterval(() => {
            const now = Date.now();
            
            // 1. Check and cleanup inactive WebSocket connections
            this.cleanupInactiveConnections(now);
            
            // 2. Check and cleanup inactive streams
            this.cleanupInactiveStreams(now);
            
            // 3. Emergency disk space cleanup
            if (this.diskUsage > CONFIG.MAX_DISK_USAGE * 0.95) {
                console.log(`⚠️ Critical disk usage! Emergency cleanup...`);
                this.emergencyDiskCleanup();
            }
            
            // 4. Log status
            this.logSystemStatus();
            
        }, CONFIG.CLEANUP_INTERVAL);
    }

    cleanupInactiveConnections(now) {
        let disconnectedClients = 0;
        
        for (const [clientId, connection] of this.clientConnections) {
            const timeSinceActivity = now - connection.lastActivity;
            
            // Close connections that have been inactive too long
            if (timeSinceActivity > CONFIG.CONNECTION_TIMEOUT) {
                console.log(`⏰ [${clientId}] Connection timeout (${Math.round(timeSinceActivity / 1000 / 60)}min inactive)`);
                
                // Close the WebSocket connection
                if (connection.ws && connection.ws.readyState === connection.ws.OPEN) {
                    connection.ws.close(1000, 'Inactivity timeout');
                }
                
                // Cleanup streams for this client
                this.destroyClientStreams(clientId);
                this.removeClientConnection(clientId);
                disconnectedClients++;
            }
        }
        
        if (disconnectedClients > 0) {
            console.log(`🧹 Cleaned up ${disconnectedClients} inactive client connections`);
        }
    }

    cleanupInactiveStreams(now) {
        let cleanedStreams = 0;
        
        for (const [streamId, stream] of this.activeStreams) {
            const timeSinceAccess = now - stream.lastAccessed;
            const timeSinceVideoAccess = now - (stream.lastVideoAccess || 0);
            
            let shouldDestroy = false;
            let reason = '';
            
            // Check if client connection still exists
            const clientConnection = this.clientConnections.get(stream.clientId);
            if (!clientConnection) {
                shouldDestroy = true;
                reason = 'client_disconnected';
            }
            // Check for video streaming inactivity
            else if (stream.status === 'ready' && stream.isActivelyStreaming) {
                if (timeSinceVideoAccess > CONFIG.STREAM_INACTIVE_TIMEOUT) {
                    shouldDestroy = true;
                    reason = 'video_streaming_inactive';
                }
            }
            // Check for general stream inactivity
            else if (timeSinceAccess > CONFIG.STREAM_INACTIVE_TIMEOUT) {
                shouldDestroy = true;
                reason = 'general_inactivity';
            }
            
            if (shouldDestroy) {
                console.log(`⏰ [${streamId}] Destroying stream: ${reason} (inactive: ${Math.round(timeSinceAccess / 1000 / 60)}min)`);
                this.destroyStream(streamId, reason);
                cleanedStreams++;
            }
        }
        
        if (cleanedStreams > 0) {
            console.log(`🧹 Cleaned up ${cleanedStreams} inactive streams`);
        }
    }

    logSystemStatus() {
        console.log(`📊 System Status:`);
        console.log(`   Active Streams: ${this.activeStreams.size}/${CONFIG.MAX_CONCURRENT_STREAMS}`);
        console.log(`   Connected Clients: ${this.clientConnections.size}`);
        console.log(`   Disk Usage: ${(this.diskUsage / 1024 / 1024).toFixed(2)}MB`);
        
        // Log individual streams
        for (const [streamId, stream] of this.activeStreams) {
            const age = Math.round((Date.now() - stream.createdAt) / 1000 / 60);
            const lastAccess = Math.round((Date.now() - stream.lastAccessed) / 1000 / 60);
            const isStreaming = stream.isActivelyStreaming ? '📺' : '⏸️';
            console.log(`   ${isStreaming} [${streamId}] ${stream.status}, Age: ${age}min, Last: ${lastAccess}min`);
        }
    }

    // Enhanced video streaming tracking
    markVideoStreamingActive(streamId) {
        const stream = this.activeStreams.get(streamId);
        if (stream) {
            stream.lastVideoAccess = Date.now();
            stream.isActivelyStreaming = true;
            this.updateClientActivity(stream.clientId);
            
            // Reset any pending destruction
            if (stream.destructionTimer) {
                clearTimeout(stream.destructionTimer);
                stream.destructionTimer = null;
            }
        }
    }

    // Mark streaming as potentially inactive (after connection ends)
    markVideoStreamingInactive(streamId, delay = 300000) { // 5 minute grace period
        const stream = this.activeStreams.get(streamId);
        if (stream) {
            console.log(`📺 [${streamId}] Video streaming ended, ${delay/1000}s grace period before marking inactive`);
            
            // Clear any existing timer
            if (stream.destructionTimer) {
                clearTimeout(stream.destructionTimer);
            }
            
            // Set a grace period before marking truly inactive
            stream.destructionTimer = setTimeout(() => {
                stream.isActivelyStreaming = false;
                console.log(`⏸️ [${streamId}] Marked as not actively streaming`);
            }, delay);
        }
    }

    // Rest of your existing methods stay the same...
    // createStream, setupEngineEvents, destroyStream, etc.
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

// --- Enhanced Video Streaming Endpoint with Network Optimization ---
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
    const extension = path.extname(videoFile.name).toLowerCase();
    const userAgent = req.headers['user-agent'] || '';
    const isLargeFile = fileSize > 1024 * 1024 * 1024; // > 1GB

    console.log(`📺 [${streamId}] Streaming request: ${videoFile.name} (${extension}) from ${req.ip}`);
    console.log(`📊 [${streamId}] File size: ${(fileSize / 1024 / 1024).toFixed(2)}MB, Large: ${isLargeFile}`);

    // Enhanced content type detection
    let contentType = 'video/mp4'; // Default
    switch (extension) {
        case '.mkv':
            // Special handling for MKV files
            if (userAgent.includes('Chrome')) {
                contentType = 'video/x-matroska';
            } else if (userAgent.includes('Firefox')) {
                contentType = 'video/webm'; // Firefox sometimes prefers this
            } else {
                contentType = 'video/x-matroska';
            }
            break;
        case '.avi':
            contentType = 'video/x-msvideo';
            break;
        case '.mov':
            contentType = 'video/quicktime';
            break;
        case '.wmv':
            contentType = 'video/x-ms-wmv';
            break;
        case '.flv':
            contentType = 'video/x-flv';
            break;
        case '.webm':
            contentType = 'video/webm';
            break;
        case '.m4v':
            contentType = 'video/mp4';
            break;
        case '.ts':
            contentType = 'video/mp2t';
            break;
    }

    // Calculate optimal chunk size based on file size and connection
    let chunkSize = 1024 * 1024; // 1MB default
    if (isLargeFile) {
        chunkSize = 2 * 1024 * 1024; // 2MB for large files
    }
    if (extension === '.mkv') {
        chunkSize = 512 * 1024; // 512KB for MKV (more compatible)
    }

    // Handle range requests with optimizations
    if (rangeHeader) {
        const ranges = rangeParser(fileSize, rangeHeader);

        if (ranges === -1 || ranges === -2) {
            console.log(`❌ [${streamId}] Invalid range header: ${rangeHeader}`);
            return res.status(416).json({ error: 'Invalid range' });
        }

        let { start, end } = ranges[0];
        
        // Optimize end point for better streaming
        if (end === fileSize - 1 && start === 0) {
            // First request - give a good chunk
            end = Math.min(start + chunkSize - 1, fileSize - 1);
        } else if (end - start > chunkSize * 2) {
            // Limit chunk size for better responsiveness
            end = start + chunkSize - 1;
        }

        const contentLength = end - start + 1;

        console.log(`📤 [${streamId}] Serving range: ${start}-${end}/${fileSize} (${(contentLength / 1024).toFixed(2)}KB)`);

        res.status(206);
        res.setHeader('Content-Length', contentLength);
        res.setHeader('Content-Range', `bytes ${start}-${end}/${fileSize}`);
        res.setHeader('Accept-Ranges', 'bytes');
        res.setHeader('Content-Type', contentType);
        
        // Enhanced headers for better streaming
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Access-Control-Allow-Headers', 'Range');
        res.setHeader('Cache-Control', 'public, max-age=3600'); // Cache for 1 hour
        res.setHeader('Connection', 'keep-alive');
        
        // MKV-specific optimizations
        if (extension === '.mkv') {
            res.setHeader('X-Content-Type-Options', 'nosniff');
            res.setHeader('Accept-Encoding', 'identity'); // Disable compression for MKV
        }
        
        // Large file optimizations
        if (isLargeFile) {
            res.setHeader('Transfer-Encoding', 'chunked');
        }

        const streamInstance = videoFile.createReadStream({ 
            start, 
            end,
            highWaterMark: 64 * 1024 // 64KB buffer for smoother streaming
        });
        
        streamInstance.on('error', (err) => {
            console.error(`❌ [${streamId}] Stream error (${start}-${end}):`, err.message);
            if (!res.headersSent) {
                res.status(500).end();
            }
        });

        streamInstance.on('data', (chunk) => {
            // Optional: Track download progress
            stream.downloadProgress = (stream.downloadProgress || 0) + chunk.length;
        });

        streamInstance.pipe(res);
    } else {
        // Non-range request - serve whole file with optimizations
        console.log(`📤 [${streamId}] Serving full file: ${(fileSize / 1024 / 1024).toFixed(2)}MB`);

        res.status(200);
        res.setHeader('Content-Length', fileSize);
        res.setHeader('Content-Type', contentType);
        res.setHeader('Accept-Ranges', 'bytes');
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Cache-Control', 'public, max-age=3600');
        res.setHeader('Connection', 'keep-alive');

        if (extension === '.mkv') {
            res.setHeader('X-Content-Type-Options', 'nosniff');
            res.setHeader('Accept-Encoding', 'identity');
        }

        const streamInstance = videoFile.createReadStream({
            highWaterMark: isLargeFile ? 128 * 1024 : 64 * 1024 // Larger buffer for big files
        });
        
        streamInstance.on('error', (err) => {
            console.error(`❌ [${streamId}] Full stream error:`, err.message);
            if (!res.headersSent) {
                res.status(500).end();
            }
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

    // Track this client connection
    streamManager.addClientConnection(clientId, ws);

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
        streamManager.removeClientConnection(clientId); // Remove connection tracking

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
