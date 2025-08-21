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
import WebTorrent from 'webtorrent';
import { v4 as uuidv4 } from 'uuid';
import fs from 'fs';

// --- Configuration ---
const CONFIG = {
    PORT: process.env.PORT || 6543,
    HOST: process.env.HOST || '0.0.0.0',
    MAX_CONCURRENT_STREAMS: parseInt(process.env.MAX_STREAMS) || 10,
    MAX_STREAMS_PER_CLIENT: 2,
    CLEANUP_INTERVAL: 5 * 60 * 1000,
    TEMP_DIR: process.env.TEMP_DIR || '/tmp/torrent-streams',
    MAX_DISK_USAGE: 5 * 1024 * 1024 * 1024,
    DEFAULT_ENGINE: process.env.TORRENT_ENGINE || 'webtorrent', // 'webtorrent' or 'peerflix'
    DHT_PORT: process.env.DHT_PORT || 6881, // For Docker/Portainer UDP
    TRACKER_PORT: process.env.TRACKER_PORT || 8000 // For Docker/Portainer TCP
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
// CLEAN VERSION
class StreamManager {
    constructor() {
        this.activeStreams = new Map();
        this.clientSessions = new Map();
        this.clientConnections = new Map();
        this.diskUsage = 0;
        this.startupTime = Date.now();
        this.startMonitoringInterval();
        this.ensureTempDir();
    }

    ensureTempDir() {
        if (!fs.existsSync(CONFIG.TEMP_DIR)) {
            fs.mkdirSync(CONFIG.TEMP_DIR, { recursive: true });
            console.log(`📁 Created temp directory: ${CONFIG.TEMP_DIR}`);
        }
    }

    addClientConnection(clientId, ws) {
        this.clientConnections.set(clientId, {
            ws: ws,
            connectedAt: Date.now(),
            lastActivity: Date.now(),
            isActive: true
        });
        console.log(`🔌 [${clientId}] Client connection tracked`);
    }

    updateClientActivity(clientId) {
        const connection = this.clientConnections.get(clientId);
        if (connection) {
            connection.lastActivity = Date.now();
            connection.isActive = true;
        }
    }

    removeClientConnection(clientId) {
        this.clientConnections.delete(clientId);
        console.log(`🔌 [${clientId}] Client connection removed`);
    }

    canCreateStream(clientId) {
        const reasons = [];
        if (this.activeStreams.size >= CONFIG.MAX_CONCURRENT_STREAMS) {
            reasons.push(`Global limit reached (${CONFIG.MAX_CONCURRENT_STREAMS} streams)`);
        }
        const clientStreams = this.clientSessions.get(clientId);
        if (clientStreams && clientStreams.size >= CONFIG.MAX_STREAMS_PER_CLIENT) {
            reasons.push(`Client limit reached (${CONFIG.MAX_STREAMS_PER_CLIENT} streams per client)`);
        }
        if (this.diskUsage > CONFIG.MAX_DISK_USAGE) {
            reasons.push(`Disk usage limit reached (${(this.diskUsage / 1024 / 1024 / 1024).toFixed(2)}GB)`);
        }
        return {
            allowed: reasons.length === 0,
            reasons: reasons
        };
    }

    createStream(magnetLink, clientId, engineType = CONFIG.DEFAULT_ENGINE) {
        const canCreate = this.canCreateStream(clientId);
        if (!canCreate.allowed) {
            throw new Error(`Cannot create stream: ${canCreate.reasons.join(', ')}`);
        }
        const streamId = uuidv4();
        const timestamp = Date.now();
        const streamPath = `${CONFIG.TEMP_DIR}/${streamId}`;
        const currentLoad = this.activeStreams.size;
        const maxLoad = CONFIG.MAX_CONCURRENT_STREAMS;
        const loadPercentage = currentLoad / maxLoad;
        let connectionLimit;
        if (loadPercentage <= 0.3) {
            connectionLimit = 50;
        } else if (loadPercentage <= 0.7) {
            connectionLimit = 25;
        } else {
            connectionLimit = 10;
        }
        console.log(`� [${streamId}] Creating stream for client ${clientId} using ${engineType}`);
        console.log(`📊 Current load: ${currentLoad}/${maxLoad} (${(loadPercentage * 100).toFixed(1)}%) - Using ${connectionLimit} connections`);
        let engine;
        let engineInstance;
        try {
            if (engineType === 'webtorrent') {
                engine = new WebTorrent({
                    maxConns: connectionLimit,
                    tracker: { port: CONFIG.TRACKER_PORT },
                    dht: { port: CONFIG.DHT_PORT },
                    webSeeds: true
                });
                const options = {
                    path: streamPath,
                    destroyStoreOnDestroy: true,
                };
                engineInstance = 'pending';
                const streamData = {
                    id: streamId,
                    clientId,
                    engine,
                    engineInstance,
                    engineType,
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
                if (!this.clientSessions.has(clientId)) {
                    this.clientSessions.set(clientId, new Set());
                }
                this.clientSessions.get(clientId).add(streamId);
                this.setupEngineEvents(streamData);
                engine.add(magnetLink, options, (torrent) => {
                    streamData.engineInstance = torrent;
                    if (streamData.onWebTorrentReady) {
                        streamData.onWebTorrentReady(torrent);
                    }
                });
                return streamData;
            } else {
                engine = peerflix(magnetLink, {
                    connections: connectionLimit,
                    uploads: 0,
                    path: streamPath,
                    quiet: false,
                    tracker: true,
                    dht: false,
                    webSeeds: true,
                    blocklist: false,
                    verify: false,
                    strategy: 'sequential'
                });
                engineInstance = engine;
                const streamData = {
                    id: streamId,
                    clientId,
                    engine,
                    engineInstance,
                    engineType,
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
                if (!this.clientSessions.has(clientId)) {
                    this.clientSessions.set(clientId, new Set());
                }
                this.clientSessions.get(clientId).add(streamId);
                this.setupEngineEvents(streamData);
                return streamData;
            }
        } catch (error) {
            console.error(`❌ [${streamId}] Error creating stream:`, error.message);
            if (engine) {
                try {
                    engine.destroy();
                } catch (destroyError) {
                    console.error(`❌ [${streamId}] Error destroying engine:`, destroyError.message);
                }
            }
            throw new Error(`Failed to create stream: ${error.message}`);
        }
    }

    markVideoStreamingActive(streamId) {
        const stream = this.activeStreams.get(streamId);
        if (stream) {
            stream.lastVideoAccess = Date.now();
            stream.isActivelyStreaming = true;
            this.updateClientActivity(stream.clientId);
            console.log(`📺 [${streamId}] Video streaming active (NO TIMEOUT)`);
        }
    }

    // ... all other methods from your previous class, unchanged ...
    // (setupEngineEvents, startTorrentMonitoring, updateTorrentStats, formatBytes, getStream, startMonitoringInterval, logSystemStatus, emergencyDiskCleanup, destroyClientStreams, getStats, findBestVideoFile, notifyClients, updateDiskUsage, destroyStream, cleanupStreamFiles)

    // Add comprehensive torrent monitoring
    startTorrentMonitoring(streamData) {
        const { id } = streamData;
        
        streamData.monitoringInterval = setInterval(() => {
            this.updateTorrentStats(streamData);
            
            // Send periodic updates to clients
            this.notifyClients(id, 'torrent_stats', streamData.torrentStats);
        }, 2000); // Update every 2 seconds
    }

    // Update the updateTorrentStats method to handle WebTorrent properly

    updateTorrentStats(streamData) {
        const { id, engine, engineType, engineInstance } = streamData;
        
        try {
            // Different handling based on engine type
            if (engineType === 'webtorrent') {
                // For WebTorrent, we need the torrent instance
                const torrent = engineInstance;
                
                // Skip if torrent isn't ready yet
                if (torrent === 'pending' || !torrent) {
                    return;
                }
                
                // Calculate basic stats
                const downloaded = torrent.downloaded || 0;
                const uploaded = torrent.uploaded || 0;
                const totalLength = torrent.length || 1; // Prevent division by zero
                const progress = totalLength > 0 ? (downloaded / totalLength) * 100 : 0;
                
                // WebTorrent specific peer stats
                const wires = torrent.wires || [];
                const numPeers = wires.length || 0;
                
                // WebTorrent doesn't have direct seeders/leechers count
                // We estimate based on which peers have the most pieces
                let seeders = 0;
                let leechers = 0;
                
                wires.forEach(wire => {
                    try {
                        if (wire && wire.remoteHave && wire.remoteHave.length === torrent.pieces.length) {
                            seeders++;
                        } else {
                            leechers++;
                        }
                    } catch (err) {
                        // Skip problematic peers
                    }
                });
                
                // WebTorrent has downloadSpeed/uploadSpeed properties
                const downloadSpeed = torrent.downloadSpeed || 0;
                const uploadSpeed = torrent.uploadSpeed || 0;
                
                // ETA calculation
                const remaining = totalLength - downloaded;
                const eta = downloadSpeed > 0 ? remaining / downloadSpeed : 0;
                
                // Health calculation
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
                    peers: numPeers,
                    activePeers: numPeers, // WebTorrent doesn't distinguish
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
                    pieces: {
                        total: torrent.pieces ? torrent.pieces.length : 0,
                        downloaded: downloaded > 0 ? Math.floor(downloaded / (torrent.pieceLength || 1)) : 0
                    },
                    bandwidth: {
                        downloadSpeedFormatted: this.formatBytes(downloadSpeed) + '/s',
                        uploadSpeedFormatted: this.formatBytes(uploadSpeed) + '/s'
                    }
                };
                
                // Log significant changes
                if (streamData.lastLoggedProgress === undefined || 
                    Math.abs(progress - streamData.lastLoggedProgress) >= 5) {
                    console.log(`📊 [${id}] WebTorrent Progress: ${progress.toFixed(1)}%, Peers: ${numPeers} (${seeders}S/${leechers}L), Speed: ${this.formatBytes(downloadSpeed)}/s, Health: ${health}`);
                    streamData.lastLoggedProgress = progress;
                }
            } else {
                // Original peerflix stats code
                // ... (keep your existing peerflix code here)
            }
        } catch (error) {
            console.error(`❌ [${id}] Error updating ${engineType} stats: ${error.message}`);
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
    startMonitoringInterval() {
        setInterval(() => {
            // ONLY log status and emergency disk cleanup - NO timeout-based cleanup
            
            // 1. Emergency disk space cleanup ONLY (when 95% full)
            if (this.diskUsage > CONFIG.MAX_DISK_USAGE * 0.95) {
                console.log(`⚠️ Critical disk usage! Emergency cleanup...`);
                this.emergencyDiskCleanup();
            }
            
            // 2. Log status (no cleanup)
            this.logSystemStatus();
            
        }, CONFIG.CLEANUP_INTERVAL);
    }

    logSystemStatus() {
        console.log(`📊 System Status (NO TIMEOUTS):`);
        console.log(`   Active Streams: ${this.activeStreams.size}/${CONFIG.MAX_CONCURRENT_STREAMS}`);
        console.log(`   Connected Clients: ${this.clientConnections.size}`);
        console.log(`   Disk Usage: ${(this.diskUsage / 1024 / 1024).toFixed(2)}MB`);
        
        // Log individual streams
        for (const [streamId, stream] of this.activeStreams) {
            const age = Math.round((Date.now() - stream.createdAt) / 1000 / 60);
            const lastAccess = Math.round((Date.now() - stream.lastAccessed) / 1000 / 60);
            const isStreaming = stream.isActivelyStreaming ? '📺' : '⏸️';
            console.log(`   ${isStreaming} [${streamId}] ${stream.status}, Age: ${age}min, Last: ${lastAccess}min (PERMANENT)`);
        }
    }

    emergencyDiskCleanup() {
        const streams = Array.from(this.activeStreams.entries());
        streams.sort(([,a], [,b]) => a.lastAccessed - b.lastAccessed);
        
        let cleaned = 0;
        const targetSize = CONFIG.MAX_DISK_USAGE * 0.8;
        
        for (const [streamId, stream] of streams) {
            if (this.diskUsage <= targetSize) break;
            
            // Only clean streams that haven't been accessed in the last 2 minutes (very short)
            const timeSinceAccess = Date.now() - stream.lastAccessed;
            if (timeSinceAccess > 2 * 60 * 1000) { // Only 2 minutes for emergency
                console.log(`🆘 [${streamId}] Emergency cleanup due to disk space (ONLY reason for cleanup)`);
                this.destroyStream(streamId, 'emergency_disk_cleanup');
                cleaned++;
            }
        }
        
        if (cleaned > 0) {
            console.log(`🧹 Emergency cleanup: removed ${cleaned} streams`);
        }
    }

    // Simplified markVideoStreamingActive (no timeout logic)
    markVideoStreamingActive(streamId) {
        const stream = this.activeStreams.get(streamId);
        if (stream) {
            stream.lastVideoAccess = Date.now();
            stream.isActivelyStreaming = true;
            this.updateClientActivity(stream.clientId);
            
            // NO destruction timer logic
            console.log(`📺 [${streamId}] Video streaming active (NO TIMEOUT)`);
        }
    }

    // Simplified destroyClientStreams - only called on actual disconnect
    destroyClientStreams(clientId) {
        const clientStreams = this.clientSessions.get(clientId);
        if (!clientStreams) return 0;

        let destroyed = 0;
        console.log(`🔌 [${clientId}] Client disconnected - cleaning up their streams`);
        
        for (const streamId of clientStreams) {
            if (this.destroyStream(streamId, 'client_disconnect')) {
                destroyed++;
            }
        }
        
        // Clean up client session
        this.clientSessions.delete(clientId);
        return destroyed;
    }

    // Add missing getStats method
    getStats() {
        const now = Date.now();
        const uptime = now - this.startupTime;
        
        return {
            activeStreams: this.activeStreams.size,
            connectedClients: this.clientConnections.size,
            uptime: uptime, // milliseconds
            diskUsage: this.diskUsage,
            maxStreams: CONFIG.MAX_CONCURRENT_STREAMS,
            timestamp: now
        };
    }

    // Add missing findBestVideoFile method
    findBestVideoFile(files, streamId) {
        if (!files || !Array.isArray(files) || files.length === 0) {
            console.log(`❌ [${streamId}] No files found in torrent`);
            return null;
        }

        console.log(`🔎 [${streamId}] Searching for best video file among ${files.length} files`);

        // Define video file extensions in order of preference
        const videoExtensions = ['.mp4', '.webm', '.mkv', '.mov', '.avi', '.m4v', '.ts', '.flv', '.wmv'];
        
        // First, try to find an exact match for common video formats
        for (const ext of videoExtensions) {
            const exactMatch = files.find(file => 
                file.name && file.name.toLowerCase().endsWith(ext) && 
                file.length > 1024 * 1024 // > 1MB
            );
            
            if (exactMatch) {
                console.log(`✅ [${streamId}] Found exact match for ${ext}: ${exactMatch.name}`);
                
                // Special handling for MKV files
                if (ext === '.mkv') {
                    console.log(`⚠️ [${streamId}] MKV file detected, enabling special handling`);
                    exactMatch.isMkv = true;
                    // Add any MKV-specific properties here if needed
                }
                
                return exactMatch;
            }
        }

        // If no exact match, find the largest file that has a video-like name
        const videoKeywords = ['video', 'movie', 'film', 'episode', 'show', 'clip'];
        const likelyVideos = files.filter(file => 
            file.name && videoKeywords.some(keyword => file.name.toLowerCase().includes(keyword))
        );
        
        if (likelyVideos.length > 0) {
            const largestLikelyVideo = likelyVideos.reduce((largest, file) => 
                file.length > largest.length ? file : largest, likelyVideos[0]
            );
            console.log(`🔍 [${streamId}] Found likely video by name: ${largestLikelyVideo.name}`);
            return largestLikelyVideo;
        }

        // Last resort: just pick the largest file over 10MB
        const largeFiles = files.filter(file => file.length > 10 * 1024 * 1024);
        if (largeFiles.length > 0) {
            const largestFile = largeFiles.reduce((largest, file) => 
                file.length > largest.length ? file : largest, largeFiles[0]
            );
            console.log(`📦 [${streamId}] Using largest file as video: ${largestFile.name}`);
            return largestFile;
        }

        console.log(`❌ [${streamId}] No suitable video file found`);
        return null;
    }

    // Add missing notifyClients method
    notifyClients(streamId, type, data) {
        console.log(`📢 [${streamId}] Notifying clients: ${type}`);
        
        const message = JSON.stringify({
            type: type,
            streamId: streamId,
            data: data,
            timestamp: Date.now()
        });

        // Send to all connected clients
        let notified = 0;
        for (const [clientId, connection] of this.clientConnections) {
            if (connection.ws && connection.ws.readyState === connection.ws.OPEN) {
                try {
                    connection.ws.send(message);
                    notified++;
                } catch (error) {
                    console.error(`❌ Failed to notify client ${clientId}:`, error.message);
                }
            }
        }
        
        console.log(`📢 [${streamId}] Notified ${notified} clients about ${type}`);
    }

    // Add missing updateDiskUsage method
    updateDiskUsage(streamData) {
        if (!streamData.engine || !streamData.engine.swarm) return;

        const oldUsage = streamData.diskUsage || 0;
        const newUsage = streamData.engine.swarm.downloaded || 0;
        
        // Update total disk usage
        this.diskUsage = this.diskUsage - oldUsage + newUsage;
        streamData.diskUsage = newUsage;
        
        // Log significant changes
        if (newUsage - oldUsage > 50 * 1024 * 1024) { // 50MB
            console.log(`💾 [${streamData.id}] Disk usage: ${this.formatBytes(newUsage)} (Total: ${this.formatBytes(this.diskUsage)})`);
        }
    };

    // Add missing destroyStream method
    destroyStream(streamId, reason = 'manual') {
        const stream = this.activeStreams.get(streamId);
        if (!stream) return false;

        console.log(`🗑️ [${streamId}] Destroying stream: ${reason}`);
        
        try {
            // Clear monitoring interval
            if (stream.monitoringInterval) {
                clearInterval(stream.monitoringInterval);
            }
            
            // Stop the engine based on type
            if (stream.engineType === 'webtorrent') {
                // WebTorrent cleanup with proper error handling
                if (stream.engine) {
                    try {
                        if (stream.engineInstance && stream.engineInstance !== 'pending') {
                            // Remove the specific torrent
                            stream.engine.remove(stream.engineInstance, {
                                destroyStore: true // Delete files
                            });
                            console.log(`✅ [${streamId}] WebTorrent torrent removed successfully`);
                        }
                        // Destroy the client
                        stream.engine.destroy(function(err) {
                            if (err) {
                                console.error(`❌ [${streamId}] WebTorrent destroy error:`, err.message);
                            } else {
                                console.log(`✅ [${streamId}] WebTorrent client destroyed successfully`);
                            }
                        });
                    } catch (e) {
                        console.error(`❌ [${streamId}] Error removing WebTorrent:`, e.message);
                    }
                }
            } else {
                // Peerflix cleanup
                if (stream.engine) {
                    try {
                        stream.engine.destroy();
                        console.log(`✅ [${streamId}] Peerflix engine destroyed successfully`);
                    } catch (e) {
                        console.error(`❌ [${streamId}] Error destroying peerflix engine:`, e.message);
                    }
                }
            }
            
            // Clean up downloaded files
            this.cleanupStreamFiles(stream);
            
        } catch (e) {
            console.error(`❌ [${streamId}] Error during stream cleanup: ${e.message}`);
        }

        // Update disk usage
        this.diskUsage -= stream.diskUsage || 0;

        // Remove from client sessions
        if (this.clientSessions.has(stream.clientId)) {
            this.clientSessions.get(stream.clientId).delete(streamId);
            if (this.clientSessions.get(stream.clientId).size === 0) {
                this.clientSessions.delete(stream.clientId);
            }
        }

        // Finally remove the stream from our active streams
        this.activeStreams.delete(streamId);
        this.notifyClients(streamId, 'stream_destroyed', { reason });
        return true;
    };

    // Add missing cleanupStreamFiles method
    cleanupStreamFiles(stream) {
        if (!stream.path || !fs.existsSync(stream.path)) return;

        try {
            // Remove the stream directory
            fs.rmSync(stream.path, { recursive: true, force: true });
            console.log(`🧹 [${stream.id}] Cleaned up files at ${stream.path}`);
        } catch (error) {
            console.error(`❌ [${stream.id}] Failed to cleanup files:`, error.message);
        }
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
    const { magnetLink, clientId, engine = CONFIG.DEFAULT_ENGINE } = req.body;
    
    if (!magnetLink || !clientId) {
        return res.status(400).json({
            error: 'magnetLink and clientId are required'
        });
    }

    // Validate engine type
    const engineType = engine === 'peerflix' ? 'peerflix' : 'webtorrent';

    try {
        const stream = streamManager.createStream(magnetLink, clientId, engineType);
        res.status(201).json({
            streamId: stream.id,
            status: stream.status,
            createdAt: stream.createdAt,
            engineType: engineType,
            noTimeout: true,
            limits: {
                maxConcurrentStreams: CONFIG.MAX_CONCURRENT_STREAMS,
                maxStreamsPerClient: CONFIG.MAX_STREAMS_PER_CLIENT
            }
        });
    } catch (error) {
        console.error('Error creating stream:', error.message);
        res.status(429).json({
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
    const isMkv = extension === '.mkv';

    console.log(`📺 [${streamId}] Streaming request: ${videoFile.name} (${extension}) from ${req.ip}`);
    console.log(`📊 [${streamId}] File size: ${(fileSize / 1024 / 1024).toFixed(2)}MB, Large: ${isLargeFile}, MKV: ${isMkv}`);

    // Enhanced content type detection with more user-agent specific handling
    let contentType = 'video/mp4'; // Default
    switch (extension) {
        case '.mkv':
            // Special handling for MKV files based on user agent
            if (userAgent.includes('Chrome')) {
                contentType = 'video/x-matroska';
            } else if (userAgent.includes('Firefox')) {
                contentType = 'video/webm'; // Firefox sometimes prefers this
            } else if (userAgent.includes('Safari')) {
                contentType = 'video/mp4'; // Safari doesn't handle MKV well
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

    // Optimize chunk size based on file characteristics
    let chunkSize;
    if (isMkv && isLargeFile) {
        chunkSize = 1 * 1024 * 1024; // 1MB for large MKV (more compatible)
    } else if (isMkv) {
        chunkSize = 512 * 1024; // 512KB for regular MKV
    } else if (isLargeFile) {
        chunkSize = 2 * 1024 * 1024; // 2MB for large non-MKV
    } else {
        chunkSize = 1 * 1024 * 1024; // 1MB default
    }

    // Track stream activity
    streamManager.markVideoStreamingActive(streamId);

    // Handle range requests with optimizations
    if (rangeHeader) {
        const ranges = rangeParser(fileSize, rangeHeader);

        if (ranges === -1 || ranges === -2) {
            console.log(`❌ [${streamId}] Invalid range header: ${rangeHeader}`);
            return res.status(416).json({ error: 'Invalid range' });
        }

        let { start, end } = ranges[0];
        
        // Optimize end point for better streaming
        if (end === fileSize - 1 && start === 0 && isLargeFile) {
            // For large files, start with smaller chunk to begin playback quickly
            end = Math.min(start + chunkSize / 2 - 1, fileSize - 1);
        } else if (end - start > chunkSize * 2) {
            // Limit large chunk requests for more responsive streaming
            end = start + chunkSize - 1;
        }

        const contentLength = end - start + 1;

        console.log(`📤 [${streamId}] Serving ${extension} range: ${start}-${end}/${fileSize} (${(contentLength / 1024).toFixed(2)}KB)`);

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
        if (isMkv) {
            res.setHeader('X-Content-Type-Options', 'nosniff');
            res.setHeader('Accept-Encoding', 'identity'); // Disable compression for MKV
        }
        
        // Large file optimizations
        if (isLargeFile) {
            res.setHeader('Transfer-Encoding', 'chunked');
        }

        // Track stream errors to prevent WebSocket disconnections
        let streamHadError = false;
        
        // Create read stream with optimized buffer size for file type
        const streamInstance = videoFile.createReadStream({ 
            start, 
            end,
            highWaterMark: isMkv ? 128 * 1024 : 64 * 1024 // Higher buffer for MKV
        });
        
        streamInstance.on('error', (err) => {
            streamHadError = true;
            console.error(`❌ [${streamId}] Stream error (${start}-${end}):`, err.message);
            if (!res.headersSent) {
                res.status(500).end();
            } else if (!res.finished) {
                res.end();
            }
        });

        streamInstance.on('end', () => {
            if (!streamHadError) {
                console.log(`✅ [${streamId}] Stream chunk completed: ${start}-${end}`);
            }
        });

        streamInstance.pipe(res);
    } else {
        // Non-range request - serve whole file with optimizations
        console.log(`📤 [${streamId}] Serving full ${extension} file: ${(fileSize / 1024 / 1024).toFixed(2)}MB`);

        res.status(200);
        res.setHeader('Content-Length', fileSize);
        res.setHeader('Content-Type', contentType);
        res.setHeader('Accept-Ranges', 'bytes');
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Cache-Control', 'public, max-age=3600');
        res.setHeader('Connection', 'keep-alive');

        if (isMkv) {
            res.setHeader('X-Content-Type-Options', 'nosniff');
            res.setHeader('Accept-Encoding', 'identity');
        }

        const streamInstance = videoFile.createReadStream({
            highWaterMark: isMkv ? 256 * 1024 : 128 * 1024 // Larger buffer for MKV
        });
        
        streamInstance.on('error', (err) => {
            console.error(`❌ [${streamId}] Full stream error:`, err.message);
            if (!res.headersSent) {
                res.status(500).end();
            } else if (!res.finished) {
                res.end();
            }
        });

        // These will help debugging MKV issues
        console.log(`📄 [${streamId}] MKV streaming attempt with mime: ${contentType}`);
        console.log(`📄 [${streamId}] Buffer size: ${isMkv ? 128 * 1024 : 64 * 1024} bytes`);

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
        const { magnetLink, engine = CONFIG.DEFAULT_ENGINE } = data;
        
        if (!magnetLink) {
            ws.send(JSON.stringify({
                type: 'error',
                message: 'magnetLink is required'
            }));
            return;
        }

        // Validate engine type
        const engineType = engine === 'peerflix' ? 'peerflix' : 'webtorrent';
        
        console.log(`🚀 Creating stream via WebSocket for client ${clientId} using ${engineType}`);
        
        // Use the selected engine
        const stream = streamManager.createStream(magnetLink, clientId, engineType);
        
        // Send success response
        ws.send(JSON.stringify({
            type: 'stream_created',
            streamId: stream.id,
            status: stream.status,
            engineType: engineType,
            timestamp: Date.now()
        }));
        
        console.log(`✅ Stream ${stream.id} created via WebSocket using ${engineType}`);
        
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
