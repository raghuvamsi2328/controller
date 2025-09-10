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
import torrentStream from 'torrent-stream'; // Replace Torrenter with torrent-stream
import { spawn } from 'child_process'; // Add for FFmpeg
import { promisify } from 'util';
import { pipeline } from 'stream';

// --- Configuration ---
const CONFIG = {
    PORT: process.env.PORT || 6543,
    HOST: process.env.HOST || '0.0.0.0',
    MAX_CONCURRENT_STREAMS: parseInt(process.env.MAX_STREAMS) || 10,
    MAX_STREAMS_PER_CLIENT: 2,
    CLEANUP_INTERVAL: 5 * 60 * 1000,
    TEMP_DIR: process.env.TEMP_DIR || '/tmp/torrent-streams',
    MAX_DISK_USAGE: 5 * 1024 * 1024 * 1024,
    DEFAULT_ENGINE: process.env.TORRENT_ENGINE || 'webtorrent',
    // Add remuxing options
    ENABLE_REMUXING: process.env.ENABLE_REMUXING !== 'false', // Default true
    FFMPEG_PATH: process.env.FFMPEG_PATH || 'ffmpeg',
    REMUX_QUALITY: process.env.REMUX_QUALITY || 'medium', // low, medium, high
    REMUX_FORMATS: ['mkv', 'avi', 'mov', 'wmv', 'flv', 'webm', 'm4v'], // Formats to remux
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

    createStream(magnetLink, clientId, engineType = CONFIG.DEFAULT_ENGINE) {
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
        
        // Dynamic connection scaling
        let connectionLimit;
        if (loadPercentage <= 0.3) {
            connectionLimit = 50; 
        } else if (loadPercentage <= 0.7) {
            connectionLimit = 25;
        } else {
            connectionLimit = 10;
        }
        
        console.log(`🚀 [${streamId}] Creating stream for client ${clientId} using ${engineType}`);
        console.log(`📊 Current load: ${currentLoad}/${maxLoad} (${(loadPercentage * 100).toFixed(1)}%) - Using ${connectionLimit} connections`);
        
        let engine;
        let engineInstance;

        if (engineType === 'webtorrent') {
            // Create WebTorrent client
            engine = new WebTorrent();
            
            // WebTorrent add options
            const options = {
                path: streamPath,
                announce: [], // Will use default trackers
            };
            
            // Start torrent - WebTorrent style
            engineInstance = 'pending'; // We'll store the torrent instance when ready
            
            // WebTorrent handles adding torrents differently
            engine.add(magnetLink, options, (torrent) => {
                engineInstance = torrent;
                // Signal that the torrent is ready
                if (streamData.onWebTorrentReady) {
                    streamData.onWebTorrentReady(torrent);
                }
            });
        } else if (engineType === 'torrent-stream') {
            // Replace torrenter with torrent-stream
            engine = torrentStream(magnetLink, {
                connections: connectionLimit,
                uploads: 0,
                path: streamPath,
                tracker: true,
                dht: false
            });
            engineInstance = engine; // For torrent-stream, engine is the instance
        } else {
            // Fallback to peerflix
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
            engineInstance = engine; // For peerflix, engine is the instance
        }

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
        
        // Link client to stream
        if (!this.clientSessions.has(clientId)) {
            this.clientSessions.set(clientId, new Set());
        }
        this.clientSessions.get(clientId).add(streamId);

        this.setupEngineEvents(streamData);
        return streamData;
    }

    setupEngineEvents(streamData) {
        const { id, engine, engineType } = streamData;

        // Initialize torrent stats with safe defaults
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
            health: 'initializing'
        };

        if (engineType === 'webtorrent') {
            // WebTorrent specific setup
            
            // We need to handle the ready event differently for WebTorrent
            streamData.onWebTorrentReady = (torrent) => {
                try {
                    console.log(`✅ [${id}] WebTorrent engine ready`);
                    
                    // Basic torrent info
                    const filesCount = torrent.files ? torrent.files.length : 0;
                    const infoHash = torrent.infoHash || 'unknown';
                    const totalSize = torrent.length || 0;
                    
                    console.log(`📁 [${id}] Total files in torrent: ${filesCount}`);
                    console.log(`🌐 [${id}] Torrent info hash: ${infoHash}`);
                    console.log(`📦 [${id}] Total size: ${(totalSize / 1024 / 1024).toFixed(2)}MB`);
                    
                    // Log all files
                    if (torrent.files && Array.isArray(torrent.files)) {
                        torrent.files.forEach((file, index) => {
                            const fileName = file && file.name ? file.name : `File ${index}`;
                            const fileSize = file && file.length ? file.length : 0;
                            console.log(`📄 [${id}] File ${index}: ${fileName} (${(fileSize / 1024 / 1024).toFixed(2)}MB)`);
                        });
                    }
                    
                    // Start torrent monitoring
                    this.startTorrentMonitoring(streamData);
                    
                    // Find best video file (WebTorrent file format is compatible)
                    const videoFile = this.findBestVideoFile(torrent.files || [], id);

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
                        filename: videoFile.name || 'unknown',
                        size: videoFile.length || 0,
                        duration: null,
                        bitrate: null,
                        container: path.extname(videoFile.name || '').toLowerCase(),
                        isInFolder: (videoFile.name || '').includes('/') || (videoFile.name || '').includes('\\'),
                        torrentHash: infoHash,
                        totalTorrentSize: totalSize
                    };

                    console.log(`✅ [${id}] Video ready: ${videoFile.name} (${(videoFile.length / 1024 / 1024).toFixed(2)} MB)`);
                    console.log(`📦 [${id}] Container: ${streamData.metadata.container}, In folder: ${streamData.metadata.isInFolder}`);
                    
                    this.notifyClients(id, 'stream_ready', streamData.metadata);
                } catch (readyError) {
                    console.error(`❌ [${id}] Error in WebTorrent ready handler: ${readyError.message}`);
                    streamData.status = 'error';
                    streamData.error = `WebTorrent ready handler error: ${readyError.message}`;
                    this.notifyClients(id, 'stream_error', { error: streamData.error });
                }
            };

            // WebTorrent events
            streamData.engine.on('error', (err) => {
                console.error(`❌ [${id}] WebTorrent error:`, err.message);
                streamData.status = 'error';
                streamData.error = err.message;
                streamData.torrentStats.health = 'error';
                this.notifyClients(id, 'stream_error', { error: err.message });
            });
            
        } else if (engineType === 'torrent-stream') {
            // Replace torrenter with torrent-stream events
            engine.on('ready', () => {
                try {
                    console.log(`✅ [${id}] Torrent-stream engine ready`);
                    
                    const filesCount = engine.files ? engine.files.length : 0;
                    const infoHash = engine.torrent ? engine.torrent.infoHash : 'unknown';
                    const totalSize = engine.torrent ? engine.torrent.length : 0;

                    console.log(`📁 [${id}] Total files in torrent: ${filesCount}`);
                    console.log(`🌐 [${id}] Torrent info hash: ${infoHash}`);
                    console.log(`📦 [${id}] Total size: ${(totalSize / 1024 / 1024).toFixed(2)}MB`);

                    if (engine.files && Array.isArray(engine.files)) {
                        engine.files.forEach((file, index) => {
                            const fileName = file && file.name ? file.name : `File ${index}`;
                            const fileSize = file && file.length ? file.length : 0;
                            console.log(`📄 [${id}] File ${index}: ${fileName} (${(fileSize / 1024 / 1024).toFixed(2)}MB)`);
                        });
                    }

                    this.startTorrentMonitoring(streamData);

                    const videoFile = this.findBestVideoFile(engine.files || [], id);

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
                        filename: videoFile.name || 'unknown',
                        size: videoFile.length || 0,
                        duration: null,
                        bitrate: null,
                        container: path.extname(videoFile.name || '').toLowerCase(),
                        isInFolder: (videoFile.name || '').includes('/') || (videoFile.name || '').includes('\\'),
                        torrentHash: infoHash,
                        totalTorrentSize: totalSize
                    };

                    console.log(`✅ [${id}] Video ready: ${videoFile.name} (${(videoFile.length / 1024 / 1024).toFixed(2)} MB)`);
                    console.log(`📦 [${id}] Container: ${streamData.metadata.container}, In folder: ${streamData.metadata.isInFolder}`);
                    
                    this.notifyClients(id, 'stream_ready', streamData.metadata);
                } catch (readyError) {
                    console.error(`❌ [${id}] Error in torrent-stream ready handler: ${readyError.message}`);
                    streamData.status = 'error';
                    streamData.error = `Torrent-stream ready handler error: ${readyError.message}`;
                    this.notifyClients(id, 'stream_error', { error: streamData.error });
                }
            });

            // Torrent-stream events
            engine.on('error', (err) => {
                console.error(`❌ [${id}] Torrent-stream error:`, err.message);
                streamData.status = 'error';
                streamData.error = err.message;
                streamData.torrentStats.health = 'error';
                this.notifyClients(id, 'stream_error', { error: err.message });
            });

            // Download monitoring
            engine.on('download', (pieceIndex) => {
                try {
                    this.updateDiskUsage(streamData);
                    this.updateTorrentStats(streamData);
                } catch (downloadError) {
                    console.error(`❌ [${id}] Error in download handler: ${downloadError.message}`);
                }
            });

            // Peer connection events
            engine.on('peer', (peer) => {
                try {
                    const peerAddress = peer && peer.remoteAddress ? peer.remoteAddress : 'unknown';
                    console.log(`👥 [${id}] New peer connected: ${peerAddress}`);
                    this.updateTorrentStats(streamData);
                } catch (peerError) {
                    console.error(`❌ [${id}] Error in peer handler: ${peerError.message}`);
                }
            });
        } else {
            // Original peerflix event handling
            engine.on('ready', () => {
                try {
                    console.log(`✅ [${id}] Peerflix engine ready`);
                    
                    // Safe access to engine properties
                    const filesCount = engine.files ? engine.files.length : 0;
                    const infoHash = engine.torrent ? engine.torrent.infoHash : 'unknown';
                    const totalSize = engine.torrent ? engine.torrent.length : 0;
                    
                    console.log(`📁 [${id}] Total files in torrent: ${filesCount}`);
                    console.log(`🌐 [${id}] Torrent info hash: ${infoHash}`);
                    console.log(`📦 [${id}] Total size: ${(totalSize / 1024 / 1024).toFixed(2)}MB`);
                    
                    // Safely log all files
                    if (engine.files && Array.isArray(engine.files)) {
                        engine.files.forEach((file, index) => {
                            const fileName = file && file.name ? file.name : `File ${index}`;
                            const fileSize = file && file.length ? file.length : 0;
                            console.log(`📄 [${id}] File ${index}: ${fileName} (${(fileSize / 1024 / 1024).toFixed(2)}MB)`);
                        });
                    }
                    
                    // Start torrent monitoring
                    this.startTorrentMonitoring(streamData);
                    
                    const videoFile = this.findBestVideoFile(engine.files || [], id);

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
                        filename: videoFile.name || 'unknown',
                        size: videoFile.length || 0,
                        duration: null,
                        bitrate: null,
                        container: path.extname(videoFile.name || '').toLowerCase(),
                        isInFolder: (videoFile.name || '').includes('/') || (videoFile.name || '').includes('\\'),
                        torrentHash: infoHash,
                        totalTorrentSize: totalSize
                    };

                    console.log(`✅ [${id}] Video ready: ${videoFile.name} (${(videoFile.length / 1024 / 1024).toFixed(2)} MB)`);
                    console.log(`📦 [${id}] Container: ${streamData.metadata.container}, In folder: ${streamData.metadata.isInFolder}`);
                    
                    this.notifyClients(id, 'stream_ready', streamData.metadata);
                    
                } catch (readyError) {
                    console.error(`❌ [${id}] Error in peerflix ready handler: ${readyError.message}`);
                    streamData.status = 'error';
                    streamData.error = `Ready handler error: ${readyError.message}`;
                    this.notifyClients(id, 'stream_error', { error: streamData.error });
                }
            });

            // Enhanced download monitoring with error handling
            engine.on('download', (pieceIndex) => {
                try {
                    this.updateDiskUsage(streamData);
                    this.updateTorrentStats(streamData);
                } catch (downloadError) {
                    console.error(`❌ [${id}] Error in download handler: ${downloadError.message}`);
                }
            });

            // Enhanced peer connection events with error handling
            engine.on('peer', (peer) => {
                try {
                    const peerAddress = peer && peer.remoteAddress ? peer.remoteAddress : 'unknown';
                    console.log(`👥 [${id}] New peer connected: ${peerAddress}`);
                    this.updateTorrentStats(streamData);
                } catch (peerError) {
                    console.error(`❌ [${id}] Error in peer handler: ${peerError.message}`);
                }
            });

            engine.on('error', (err) => {
                console.error(`❌ [${id}] Engine error:`, err.message);
                streamData.status = 'error';
                streamData.error = err.message;
                streamData.torrentStats.health = 'error';
                this.notifyClients(id, 'stream_error', { error: err.message });
            });
        }
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
        const { id, engine, engineType, engineInstance } = streamData;
        
        // Different handling based on engine type
        if (engineType === 'webtorrent') {
            try {
                // For WebTorrent, we need the torrent instance
                const torrent = engineInstance;
                
                // Skip if torrent isn't ready yet
                if (torrent === 'pending' || !torrent) {
                    return;
                }
                
                // Calculate basic stats
                const downloaded = torrent.downloaded || 0;
                const uploaded = torrent.uploaded || 0;
                const totalLength = torrent.length || 0;
                const progress = totalLength > 0 ? (downloaded / totalLength) * 100 : 0;
                
                // WebTorrent specific peer stats
                const peers = torrent.wires || [];
                const numPeers = torrent.numPeers || 0;
                
                // WebTorrent doesn't have direct seeders/leechers count
                // We estimate based on which peers have the most pieces
                let seeders = 0;
                let leechers = 0;
                
                if (Array.isArray(peers)) {
                    peers.forEach(wire => {
                        if (wire && wire.have && wire.have.length === torrent.pieces.length) {
                            seeders++;
                        } else {
                            leechers++;
                        }
                    });
                }
                
                // WebTorrent has downloadSpeed/uploadSpeed properties
                const downloadSpeed = torrent.downloadSpeed || 0;
                const uploadSpeed = torrent.uploadSpeed || 0;
                
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
                        downloaded: torrent.pieces ? torrent.pieces.filter(p => p).length : 0
                    },
                    bandwidth: {
                        downloadSpeedFormatted: this.formatBytes(downloadSpeed) + '/s',
                        uploadSpeedFormatted: this.formatBytes(uploadSpeed) + '/s'
                    }
                };
                
                // Log significant changes
                if (streamData.lastLoggedProgress === undefined || 
                    Math.abs(progress - streamData.lastLoggedProgress) >= 5) {
                    console.log(`📊 [${streamData.id}] Progress: ${progress.toFixed(1)}%, Peers: ${numPeers} (${seeders}S/${leechers}L), Speed: ${this.formatBytes(downloadSpeed)}/s, Health: ${health}`);
                    streamData.lastLoggedProgress = progress;
                }
                
            } catch (error) {
                console.error(`❌ [${id}] Error updating WebTorrent stats: ${error.message}`);
                streamData.torrentStats.health = 'error';
            }
            
        } else if (engineType === 'torrent-stream') {
            try {
                if (!engine || !engine.swarm || !engine.torrent) {
                    return;
                }

                const swarm = engine.swarm;
                const torrent = engine.torrent;
                
                const downloaded = swarm.downloaded || 0;
                const uploaded = swarm.uploaded || 0;
                const totalLength = torrent.length || 0;
                const progress = totalLength > 0 ? (downloaded / totalLength) * 100 : 0;
                const peers = torrent.peers || [];
                const numPeers = peers.length || 0;
                let seeders = 0;
                let leechers = 0;

                peers.forEach(peer => {
                    if (peer && peer.isSeeder) {
                        seeders++;
                    } else {
                        leechers++;
                    }
                });

                const downloadSpeed = torrent.downloadSpeed || 0;
                const uploadSpeed = torrent.uploadSpeed || 0;
                const remaining = totalLength - downloaded;
                const eta = downloadSpeed > 0 ? remaining / downloadSpeed : 0;

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

                streamData.torrentStats = {
                    peers: numPeers,
                    activePeers: numPeers,
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
                        downloaded: torrent.pieces ? torrent.pieces.filter(p => p).length : 0
                    },
                    bandwidth: {
                        downloadSpeedFormatted: this.formatBytes(downloadSpeed) + '/s',
                        uploadSpeedFormatted: this.formatBytes(uploadSpeed) + '/s'
                    }
                };

                // Log significant changes
                if (streamData.lastLoggedProgress === undefined || 
                    Math.abs(progress - streamData.lastLoggedProgress) >= 5) {
                    console.log(`📊 [${streamData.id}] Progress: ${progress.toFixed(1)}%, Peers: ${numPeers} (${seeders}S/${leechers}L), Speed: ${this.formatBytes(downloadSpeed)}/s, Health: ${health}`);
                    streamData.lastLoggedProgress = progress;
                }
            } catch (error) {
                console.error(`❌ [${id}] Error updating torrent-stream stats: ${error.message}`);
                streamData.torrentStats.health = 'error';
            }
        } else {
            // Original peerflix stats code
            // ... [your existing peerflix updateTorrentStats code] ...
            if (!engine || !engine.swarm || !engine.torrent) {
                console.log(`⚠️ [${streamData.id}] Engine/swarm/torrent not ready for stats update`);
                return;
            }

            try {
                const swarm = engine.swarm;
                const torrent = engine.torrent;
                
                // Calculate basic stats with null checks
                const downloaded = swarm.downloaded || 0;
                const uploaded = swarm.uploaded || 0;
                const totalLength = torrent.length || 0;
                const progress = totalLength > 0 ? (downloaded / totalLength) * 100 : 0;
                
                // Enhanced peer statistics with null checking
                const wires = swarm.wires || [];
                const peers = Array.isArray(wires) ? wires : [];
                
                // Safely filter peers
                let activePeers = 0;
                let seeders = 0;
                
                peers.forEach(peer => {
                    try {
                        // Check if peer is active (not choking)
                        if (peer && !peer.peerChoking) {
                            activePeers++;
                        }
                        
                        // Check if peer is a seeder (has all pieces)
                        if (peer && peer.peerPieces && peer.peerPieces.buffer) {
                            // Safer seeder detection
                            const bufferString = peer.peerPieces.buffer.toString('hex');
                            if (bufferString && !bufferString.includes('00')) {
                                seeders++;
                            }
                        }
                    } catch (peerError) {
                        // Skip problematic peers
                        console.log(`⚠️ [${streamData.id}] Skipping problematic peer: ${peerError.message}`);
                    }
                });
                
                const leechers = Math.max(0, peers.length - seeders);
                
                // Speed calculations with null checks
                const downloadSpeed = (swarm.downloadSpeed && typeof swarm.downloadSpeed === 'function') 
                    ? swarm.downloadSpeed() || 0 
                    : 0;
                const uploadSpeed = (swarm.uploadSpeed && typeof swarm.uploadSpeed === 'function') 
                    ? swarm.uploadSpeed() || 0 
                    : 0;
                
                // ETA calculation
                const remaining = Math.max(0, totalLength - downloaded);
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
                    peers: peers.length || 0,
                    activePeers: activePeers || 0,
                    seeders: seeders || 0,
                    leechers: leechers || 0,
                    downloadSpeed: downloadSpeed || 0,
                    uploadSpeed: uploadSpeed || 0,
                    downloaded: downloaded || 0,
                    uploaded: uploaded || 0,
                    progress: Math.round((progress || 0) * 100) / 100,
                    ratio: downloaded > 0 ? (uploaded / downloaded) : 0,
                    eta: eta || 0,
                    health: health,
                    totalSize: totalLength || 0,
                    remaining: remaining || 0,
                    pieces: {
                        total: torrent.pieces ? torrent.pieces.length : 0,
                        downloaded: torrent.pieces ? Math.floor(downloaded / torrent.pieceLength) : 0
                    },
                    bandwidth: {
                        downloadSpeedFormatted: this.formatBytes(downloadSpeed || 0) + '/s',
                        uploadSpeedFormatted: this.formatBytes(uploadSpeed || 0) + '/s'
                    }
                };

                // Log significant changes
                if (streamData.lastLoggedProgress === undefined || 
                    Math.abs(progress - streamData.lastLoggedProgress) >= 5) {
                    console.log(`📊 [${streamData.id}] Progress: ${progress.toFixed(1)}%, Peers: ${peers.length} (${seeders}S/${leechers}L), Speed: ${this.formatBytes(downloadSpeed)}/s, Health: ${health}`);
                    streamData.lastLoggedProgress = progress;
                }
            } catch (error) {
                console.error(`❌ [${streamData.id}] Error updating torrent stats: ${error.message}`);
                streamData.torrentStats.health = 'error';
            }
        }
    }

    // Add the missing methods to the StreamManager class

    // Add the missing startMonitoringInterval method
    startMonitoringInterval() {
        // Clean up old streams and update stats
        setInterval(() => {
            this.cleanupOldStreams();
            this.updateGlobalStats();
        }, CONFIG.CLEANUP_INTERVAL);
        
        console.log(`🔧 Monitoring interval started (cleanup every ${CONFIG.CLEANUP_INTERVAL / 1000}s)`);
    }

    // Add the missing cleanupOldStreams method
    cleanupOldStreams() {
        const now = Date.now();
        const inactiveThreshold = 30 * 60 * 1000; // 30 minutes
        
        for (const [streamId, streamData] of this.activeStreams) {
            const timeSinceLastAccess = now - streamData.lastAccessed;
            
            if (timeSinceLastAccess > inactiveThreshold) {
                console.log(`🧹 [${streamId}] Cleaning up inactive stream (${Math.round(timeSinceLastAccess / 1000 / 60)}min old)`);
                this.destroyStream(streamId);
            }
        }
    }

    // Add the missing updateGlobalStats method
    updateGlobalStats() {
        // Calculate total disk usage
        let totalDiskUsage = 0;
        for (const streamData of this.activeStreams.values()) {
            totalDiskUsage += streamData.diskUsage || 0;
        }
        this.diskUsage = totalDiskUsage;
        
        // Log stats periodically
        if (this.activeStreams.size > 0) {
            console.log(`📊 Global stats: ${this.activeStreams.size} active streams, ${this.formatBytes(this.diskUsage)} disk usage`);
        }
    }

    // Add the missing destroyStream method
    destroyStream(streamId) {
        const streamData = this.activeStreams.get(streamId);
        if (!streamData) {
            return false;
        }

        try {
            console.log(`🗑️ [${streamId}] Destroying stream`);

            // Stop monitoring
            if (streamData.monitoringInterval) {
                clearInterval(streamData.monitoringInterval);
            }

            // Destroy engine
            if (streamData.engine) {
                if (typeof streamData.engine.destroy === 'function') {
                    streamData.engine.destroy();
                } else if (typeof streamData.engine.remove === 'function') {
                    streamData.engine.remove();
                }
            }

            // Clean up client session
            if (streamData.clientId && this.clientSessions.has(streamData.clientId)) {
                this.clientSessions.get(streamData.clientId).delete(streamId);
                if (this.clientSessions.get(streamData.clientId).size === 0) {
                    this.clientSessions.delete(streamData.clientId);
                }
            }

            // Remove from active streams
            this.activeStreams.delete(streamId);

            // Notify clients
            this.notifyClients(streamId, 'stream_destroyed', { streamId });

            return true;
        } catch (error) {
            console.error(`❌ [${streamId}] Error destroying stream: ${error.message}`);
            return false;
        }
    }

    // Add the missing getStream method
    getStream(streamId) {
        const stream = this.activeStreams.get(streamId);
        if (stream) {
            stream.lastAccessed = Date.now();
        }
        return stream;
    }

    // Add the missing getStats method
    getStats() {
        const uptime = Date.now() - this.startupTime;
        
        return {
            activeStreams: this.activeStreams.size,
            connectedClients: this.clientConnections.size,
            totalDiskUsage: this.diskUsage,
            uptime: uptime,
            uptimeFormatted: this.formatUptime(uptime),
            limits: {
                maxConcurrentStreams: CONFIG.MAX_CONCURRENT_STREAMS,
                maxStreamsPerClient: CONFIG.MAX_STREAMS_PER_CLIENT,
                maxDiskUsage: CONFIG.MAX_DISK_USAGE
            },
            load: {
                percentage: (this.activeStreams.size / CONFIG.MAX_CONCURRENT_STREAMS) * 100,
                status: this.activeStreams.size === 0 ? 'idle' : 
                       this.activeStreams.size < CONFIG.MAX_CONCURRENT_STREAMS * 0.5 ? 'light' :
                       this.activeStreams.size < CONFIG.MAX_CONCURRENT_STREAMS * 0.8 ? 'moderate' : 'heavy'
            }
        };
    }

    // Add the missing notifyClients method
    notifyClients(streamId, eventType, data) {
        const streamData = this.activeStreams.get(streamId);
        if (!streamData) return;

        const clientConnection = this.clientConnections.get(streamData.clientId);
        if (clientConnection && clientConnection.ws && clientConnection.ws.readyState === 1) {
            try {
                clientConnection.ws.send(JSON.stringify({
                    type: eventType,
                    streamId: streamId,
                    data: data,
                    timestamp: Date.now()
                }));
            } catch (error) {
                console.error(`❌ Error notifying client ${streamData.clientId}: ${error.message}`);
            }
        }
    }

    // Add the missing updateDiskUsage method
    updateDiskUsage(streamData) {
        try {
            if (streamData.path && fs.existsSync(streamData.path)) {
                const stats = fs.statSync(streamData.path);
                streamData.diskUsage = stats.size || 0;
            }
        } catch (error) {
            console.error(`❌ [${streamData.id}] Error updating disk usage: ${error.message}`);
        }
    }

    // Add the missing findBestVideoFile method
    findBestVideoFile(files, streamId) {
        if (!files || !Array.isArray(files) || files.length === 0) {
            console.log(`❌ [${streamId}] No files array provided`);
            return null;
        }

        // Video extensions in order of preference
        const videoExtensions = ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v'];
        
        // Filter video files
        const videoFiles = files.filter(file => {
            if (!file || !file.name) return false;
            const ext = path.extname(file.name).toLowerCase();
            return videoExtensions.includes(ext);
        });

        if (videoFiles.length === 0) {
            console.log(`❌ [${streamId}] No video files found`);
            return null;
        }

        // Sort by size (largest first) and then by preferred format
        videoFiles.sort((a, b) => {
            const aExt = path.extname(a.name).toLowerCase();
            const bExt = path.extname(b.name).toLowerCase();
            
            // Prefer mp4
            if (aExt === '.mp4' && bExt !== '.mp4') return -1;
            if (bExt === '.mp4' && aExt !== '.mp4') return 1;
            
            // Then by size
            return (b.length || 0) - (a.length || 0);
        });

        const selectedFile = videoFiles[0];
        console.log(`✅ [${streamId}] Selected video file: ${selectedFile.name} (${(selectedFile.length / 1024 / 1024).toFixed(2)}MB)`);
        
        return selectedFile;
    }

    // Add the missing formatUptime method
    formatUptime(ms) {
        const seconds = Math.floor(ms / 1000);
        const minutes = Math.floor(seconds / 60);
        const hours = Math.floor(minutes / 60);
        const days = Math.floor(hours / 24);

        if (days > 0) return `${days}d ${hours % 24}h ${minutes % 60}m`;
        if (hours > 0) return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
        if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
        return `${seconds}s`;
    }

    // Add remuxing helper methods
    needsRemuxing(videoFile) {
        if (!CONFIG.ENABLE_REMUXING) return false;
        
        const extension = path.extname(videoFile.name || '').toLowerCase().slice(1);
        const needsRemux = CONFIG.REMUX_FORMATS.includes(extension);
        
        console.log(`🔍 File: ${videoFile.name}, Extension: ${extension}, Needs remux: ${needsRemux}`);
        return needsRemux;
    }

    async checkFFmpegAvailable() {
        return new Promise((resolve) => {
            const ffmpeg = spawn(CONFIG.FFMPEG_PATH, ['-version']);
            
            ffmpeg.on('close', (code) => {
                resolve(code === 0);
            });
            
            ffmpeg.on('error', () => {
                resolve(false);
            });
            
            // Timeout after 5 seconds
            setTimeout(() => {
                ffmpeg.kill();
                resolve(false);
            }, 5000);
        });
    }

    createRemuxingStream(videoFile, quality = CONFIG.REMUX_QUALITY) {
        console.log(`🔄 Starting remuxing for: ${videoFile.name} (quality: ${quality})`);
        
        const args = [
            '-i', 'pipe:0', // Read from stdin
            '-f', 'mp4',    // Output format
            '-movflags', 'frag_keyframe+empty_moov+faststart', // Streaming optimizations
            '-avoid_negative_ts', 'make_zero',
        ];

        // Quality settings
        switch (quality) {
            case 'low':
                args.push(
                    '-c:v', 'libx264',
                    '-preset', 'ultrafast',
                    '-crf', '28',
                    '-c:a', 'aac',
                    '-b:a', '128k',
                    '-ac', '2'
                );
                break;
            case 'high':
                args.push(
                    '-c:v', 'libx264',
                    '-preset', 'fast',
                    '-crf', '20',
                    '-c:a', 'aac',
                    '-b:a', '256k',
                    '-ac', '2'
                );
                break;
            default: // medium
                args.push(
                    '-c:v', 'libx264',
                    '-preset', 'veryfast',
                    '-crf', '23',
                    '-c:a', 'aac',
                    '-b:a', '192k',
                    '-ac', '2'
                );
        }

        args.push(
            '-max_muxing_queue_size', '1024',
            '-fflags', '+genpts',
            'pipe:1' // Output to stdout
        );

        console.log(`🔧 FFmpeg command: ${CONFIG.FFMPEG_PATH} ${args.join(' ')}`);
        
        const ffmpeg = spawn(CONFIG.FFMPEG_PATH, args, {
            stdio: ['pipe', 'pipe', 'pipe']
        });

        // Handle FFmpeg errors
        ffmpeg.stderr.on('data', (data) => {
            const message = data.toString();
            if (message.includes('error') || message.includes('Error')) {
                console.error(`❌ FFmpeg error: ${message}`);
            }
        });

        ffmpeg.on('error', (error) => {
            console.error(`❌ FFmpeg spawn error: ${error.message}`);
        });

        ffmpeg.on('close', (code) => {
            if (code !== 0) {
                console.error(`❌ FFmpeg exited with code ${code}`);
            } else {
                console.log(`✅ FFmpeg completed successfully`);
            }
        });

        // Create input stream from video file
        const inputStream = videoFile.createReadStream();
        
        // Pipe input to FFmpeg
        inputStream.pipe(ffmpeg.stdin);
        
        // Handle input stream errors
        inputStream.on('error', (error) => {
            console.error(`❌ Input stream error: ${error.message}`);
            ffmpeg.kill();
        });

        // Return FFmpeg stdout as the remuxed stream
        return ffmpeg.stdout;
    }

    createRangeRemuxingStream(videoFile, start, end, quality = CONFIG.REMUX_QUALITY) {
        console.log(`🔄 Starting range remuxing for: ${videoFile.name} (${start}-${end}, quality: ${quality})`);
        
        const args = [
            '-i', 'pipe:0',
            '-ss', Math.max(0, start - 1000000).toString(),
            '-f', 'mp4',
            '-movflags', 'frag_keyframe+empty_moov+faststart',
            '-avoid_negative_ts', 'make_zero',
        ];

        // Add quality settings
        switch (quality) {
            case 'low':
                args.push(
                    '-c:v', 'libx264',
                    '-preset', 'ultrafast',
                    '-crf', '28',
                    '-c:a', 'aac',
                    '-b:a', '128k'
                );
                break;
            case 'high':
                args.push(
                    '-c:v', 'libx264',
                    '-preset', 'fast',
                    '-crf', '20',
                    '-c:a', 'aac',
                    '-b:a', '256k'
                );
                break;
            default:
                args.push(
                    '-c:v', 'libx264',
                    '-preset', 'veryfast',
                    '-crf', '23',
                    '-c:a', 'aac',
                    '-b:a', '192k'
                );
        }

        args.push('pipe:1');

        const ffmpeg = spawn(CONFIG.FFMPEG_PATH, args, {
            stdio: ['pipe', 'pipe', 'pipe']
        });

        // Handle errors
        ffmpeg.stderr.on('data', (data) => {
            const message = data.toString();
            if (message.includes('error') || message.includes('Error')) {
                console.error(`❌ FFmpeg range error: ${message}`);
            }
        });

        // Create ranged input stream
        const inputStream = videoFile.createReadStream({ start, end });
        inputStream.pipe(ffmpeg.stdin);

        inputStream.on('error', (error) => {
            console.error(`❌ Range input stream error: ${error.message}`);
            ffmpeg.kill();
        });

        return ffmpeg.stdout;
    }

    // Add the missing formatBytes method
    formatBytes(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    // Add missing destroyClientStreams method (referenced in WebSocket close handler)
    destroyClientStreams(clientId) {
        const clientStreams = this.clientSessions.get(clientId);
        if (clientStreams) {
            console.log(`🧹 [${clientId}] Destroying ${clientStreams.size} client streams`);
            
            for (const streamId of clientStreams) {
                this.destroyStream(streamId);
            }
            
            this.clientSessions.delete(clientId);
        }
    }

    // Add missing handleStreamAction method (referenced in WebSocket message handler)
    handleStreamAction(data, clientId) {
        const { streamId, action, payload } = data;
        
        if (!streamId) {
            console.warn(`⚠️ [${clientId}] Stream action missing streamId`);
            return;
        }
        
        const stream = this.getStream(streamId);
        if (!stream) {
            console.warn(`⚠️ [${clientId}] Stream action for non-existent stream: ${streamId}`);
            return;
        }
        
        // Verify client owns this stream
        if (stream.clientId !== clientId) {
            console.warn(`⚠️ [${clientId}] Unauthorized stream action for: ${streamId}`);
            return;
        }
        
        console.log(`🎬 [${streamId}] Stream action: ${action} by ${clientId}`);
        
        switch (action) {
            case 'play':
                // Update last accessed time
                stream.lastAccessed = Date.now();
                this.notifyClients(streamId, 'stream_action_result', { 
                    action: 'play', 
                    success: true 
                });
                break;
                
            case 'pause':
                stream.lastAccessed = Date.now();
                this.notifyClients(streamId, 'stream_action_result', { 
                    action: 'pause', 
                    success: true 
                });
                break;
                
            case 'seek':
                const { time } = payload || {};
                if (typeof time === 'number') {
                    stream.lastAccessed = Date.now();
                    console.log(`⏯️ [${streamId}] Seek to: ${time}s`);
                    this.notifyClients(streamId, 'stream_action_result', { 
                        action: 'seek', 
                        time: time, 
                        success: true 
                    });
                } else {
                    console.warn(`⚠️ [${streamId}] Invalid seek time: ${time}`);
                }
                break;
                
            case 'destroy':
                this.destroyStream(streamId);
                break;
                
            default:
                console.warn(`⚠️ [${streamId}] Unknown stream action: ${action}`);
        }
    }

    // ...existing methods like ensureTempDir, addClientConnection, etc.
}

// Create the stream manager instance
const streamManager = new StreamManager();

// --- REST API Routes --- (These should be outside the class)

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

    // Validate engine type (replace 'torrenter' with 'torrent-stream')
    const engineType = ['webtorrent', 'torrent-stream', 'peerflix'].includes(engine) ? engine : 'webtorrent';

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
        clientId: stream.clientId,
        status: stream.status,
        createdAt: stream.createdAt,
        lastAccessed: stream.lastAccessed,
        videoFile: stream.videoFile,
        metadata: stream.metadata,
        diskUsage: stream.diskUsage,
        torrentStats: stream.torrentStats
    });
});

// Stream video file (with range support and remuxing)
app.get('/api/streams/:streamId/video', streamingLimiter, async (req, res) => {
    const { streamId } = req.params;
    const stream = streamManager.getStream(streamId);
    
    if (!stream || stream.status !== 'ready' || !stream.videoFile) {
        return res.status(404).json({
            error: 'Stream not found or not ready'
        });
    }

    const videoFile = stream.videoFile;
    const fileSize = videoFile.length;
    const range = req.headers.range;
    let needsRemux = streamManager.needsRemuxing(videoFile);
    const quality = req.query.quality || CONFIG.REMUX_QUALITY;

    // Check if FFmpeg is available when remuxing is needed
    if (needsRemux && CONFIG.ENABLE_REMUXING) {
        const ffmpegAvailable = await streamManager.checkFFmpegAvailable();
        if (!ffmpegAvailable) {
            console.error(`❌ FFmpeg not available, serving original file`);
            needsRemux = false;
        }
    }

    console.log(`📤 [${streamId}] Serving ${needsRemux ? 'remuxed' : 'original'} video: ${videoFile.name}`);

    if (needsRemux && CONFIG.ENABLE_REMUXING) {
        // Remuxing mode
        res.setHeader('Content-Type', 'video/mp4');
        res.setHeader('Accept-Ranges', 'bytes');
        res.setHeader('Cache-Control', 'no-cache');
        res.setHeader('Connection', 'keep-alive');
        res.setHeader('X-Remuxed', 'true');
        res.setHeader('X-Original-Format', path.extname(videoFile.name));

        if (range) {
            // Range request with remuxing
            const ranges = rangeParser(fileSize, range);
            if (ranges === -1) {
                return res.status(416).send('Requested range not satisfiable');
            }

            const { start, end } = ranges[0];
            
            res.writeHead(206, {
                'Content-Type': 'video/mp4',
                'Accept-Ranges': 'bytes',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Transfer-Encoding': 'chunked',
                'X-Remuxed': 'true'
            });

            try {
                const remuxStream = streamManager.createRangeRemuxingStream(videoFile, start, end, quality);
                remuxStream.on('error', (err) => {
                    console.error(`❌ [${streamId}] Remux range stream error:`, err.message);
                    if (!res.headersSent) {
                        res.status(500).end();
                    } else {
                        res.end();
                    }
                });
                remuxStream.pipe(res);
            } catch (error) {
                console.error(`❌ [${streamId}] Remux range error:`, error.message);
                res.status(500).end();
            }
        } else {
            // Full file remuxing
            res.writeHead(200, {
                'Content-Type': 'video/mp4',
                'Accept-Ranges': 'bytes',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Transfer-Encoding': 'chunked',
                'X-Remuxed': 'true',
                'X-Original-Format': path.extname(videoFile.name)
            });

            try {
                const remuxStream = streamManager.createRemuxingStream(videoFile, quality);
                remuxStream.on('error', (err) => {
                    console.error(`❌ [${streamId}] Remux stream error:`, err.message);
                    if (!res.headersSent) {
                        res.status(500).end();
                    } else {
                        res.end();
                    }
                });
                remuxStream.pipe(res);
            } catch (error) {
                console.error(`❌ [${streamId}] Remux error:`, error.message);
                res.status(500).end();
            }
        }
    } else {
        // Original file serving
        const extension = path.extname(videoFile.name).toLowerCase();
        
        // Set appropriate content type
        switch (extension) {
            case '.mp4':
                res.setHeader('Content-Type', 'video/mp4');
                break;
            case '.mkv':
                res.setHeader('Content-Type', 'video/x-matroska');
                break;
            case '.avi':
                res.setHeader('Content-Type', 'video/x-msvideo');
                break;
            case '.mov':
                res.setHeader('Content-Type', 'video/quicktime');
                break;
            case '.webm':
                res.setHeader('Content-Type', 'video/webm');
                break;
            default:
                res.setHeader('Content-Type', 'video/mp4');
        }

        if (range) {
            // Range request
            const ranges = rangeParser(fileSize, range);
            if (ranges === -1) {
                return res.status(416).send('Requested range not satisfiable');
            }

            const { start, end } = ranges[0];
            const chunkSize = end - start + 1;

            res.writeHead(206, {
                'Content-Range': `bytes ${start}-${end}/${fileSize}`,
                'Content-Length': chunkSize,
                'Accept-Ranges': 'bytes',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive'
            });

            const streamInstance = videoFile.createReadStream({
                start,
                end,
                highWaterMark: 64 * 1024
            });
            
            streamInstance.on('error', (err) => {
                console.error(`❌ [${streamId}] Stream error (${start}-${end}):`, err.message);
                if (!res.headersSent) {
                    res.status(500).end();
                }
            });

            streamInstance.pipe(res);
        } else {
            // Full file
            res.status(200);
            res.setHeader('Content-Length', fileSize);
            res.setHeader('Accept-Ranges', 'bytes');
            res.setHeader('Access-Control-Allow-Origin', '*');
            res.setHeader('Cache-Control', 'public, max-age=3600');
            res.setHeader('Connection', 'keep-alive');

            const streamInstance = videoFile.createReadStream({
                highWaterMark: fileSize > 100 * 1024 * 1024 ? 128 * 1024 : 64 * 1024
            });
            
            streamInstance.on('error', (err) => {
                console.error(`❌ [${streamId}] Full stream error:`, err.message);
                if (!res.headersSent) {
                    res.status(500).end();
                }
            });

            streamInstance.pipe(res);
        }
    }
});

// Add remuxing status endpoint
app.get('/api/remux/status', async (req, res) => {
    const ffmpegAvailable = await streamManager.checkFFmpegAvailable();
    
    res.json({
        remuxingEnabled: CONFIG.ENABLE_REMUXING,
        ffmpegAvailable: ffmpegAvailable,
        ffmpegPath: CONFIG.FFMPEG_PATH,
        supportedFormats: CONFIG.REMUX_FORMATS,
        defaultQuality: CONFIG.REMUX_QUALITY,
        availableQualities: ['low', 'medium', 'high']
    });
});

// Add stream info endpoint with remuxing details
app.get('/api/streams/:streamId/info', (req, res) => {
    const { streamId } = req.params;
    const stream = streamManager.getStream(streamId);
    
    if (!stream || stream.status !== 'ready' || !stream.videoFile) {
        return res.status(404).json({
            error: 'Stream not found or not ready'
        });
    }

    const videoFile = stream.videoFile;
    const needsRemux = streamManager.needsRemuxing(videoFile);
    const extension = path.extname(videoFile.name).toLowerCase();

    res.json({
        streamId: stream.id,
        filename: videoFile.name,
        size: videoFile.length,
        originalFormat: extension,
        needsRemuxing: needsRemux,
        remuxingEnabled: CONFIG.ENABLE_REMUXING,
        availableQualities: ['low', 'medium', 'high'],
        directPlaySupported: !needsRemux,
        metadata: stream.metadata
    });
});

// --- Error Handling Middleware ---
app.use((err, req, res, next) => {
    console.error(`❌ [${req.ip}] Error:`, err.message);

    // Generic error response
    res.status(500).json({
        error: 'Internal Server Error',
        details: err.message
    });
});

// --- WebSocket Events ---
wss.on('connection', (ws, req) => {
    const clientId = uuidv4();
    console.log(`🌐 New WebSocket connection: ${clientId}`);
    
    // Track this client connection
    streamManager.addClientConnection(clientId, ws);

    // Send welcome message
    ws.send(JSON.stringify({ type: 'welcome', clientId }));

    // --- Client Message Handling ---
    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            
            // Update client activity
            streamManager.updateClientActivity(clientId);
            
            // Handle different message types
            switch (data.type) {
                case 'ping':
                    // Simple ping/pong for keep-alive
                    ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
                    break;
                case 'stream_action':
                    // Handle stream actions (play, pause, seek, etc.)
                    streamManager.handleStreamAction(data, clientId);
                    break;
                case 'get_stats':
                    // Send current stats to client
                    ws.send(JSON.stringify({ 
                        type: 'stats', 
                        data: streamManager.getStats() 
                    }));
                    break;
                default:
                    console.warn(`⚠️ [${clientId}] Unknown message type: ${data.type}`);
            }
        } catch (e) {
            console.error(`❌ [${clientId}] Message handling error: ${e.message}`);
            ws.send(JSON.stringify({ 
                type: 'error', 
                message: 'Invalid message format' 
            }));
        }
    });

    // Handle client disconnect
    ws.on('close', () => {
        console.log(`🔌 Client disconnected: ${clientId}`);
        streamManager.removeClientConnection(clientId);
        
        // Optionally, destroy all streams for this client on disconnect
        // Uncomment the line below if you want to clean up streams when client disconnects
        // streamManager.destroyClientStreams(clientId);
    });

    // Handle WebSocket errors
    ws.on('error', (error) => {
        console.error(`❌ [${clientId}] WebSocket error: ${error.message}`);
        streamManager.removeClientConnection(clientId);
    });
});

// --- Start Server ---
server.listen(CONFIG.PORT, CONFIG.HOST, () => {
    console.log(`🚀 Server running at http://${CONFIG.HOST}:${CONFIG.PORT}/`);
});