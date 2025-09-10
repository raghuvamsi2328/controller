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
import Torrenter from 'torrenter';
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
        } else if (engineType === 'torrenter') {
            // Torrenter engine setup
            engine = new Torrenter();
            engineInstance = 'pending';
            engine.add(magnetLink, { path: streamPath }, (torrent) => {
                engineInstance = torrent;
                if (streamData.onTorrenterReady) {
                    streamData.onTorrenterReady(torrent);
                }
            });
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
            
        } else if (engineType === 'torrenter') {
            // Torrenter specific setup
            streamData.onTorrenterReady = (torrent) => {
                try {
                    console.log(`✅ [${id}] Torrenter engine ready`);
                    const filesCount = torrent.files ? torrent.files.length : 0;
                    const infoHash = torrent.infoHash || 'unknown';
                    const totalSize = torrent.length || 0;

                    console.log(`📁 [${id}] Total files in torrent: ${filesCount}`);
                    console.log(`🌐 [${id}] Torrent info hash: ${infoHash}`);
                    console.log(`📦 [${id}] Total size: ${(totalSize / 1024 / 1024).toFixed(2)}MB`);

                    if (torrent.files && Array.isArray(torrent.files)) {
                        torrent.files.forEach((file, index) => {
                            const fileName = file && file.name ? file.name : `File ${index}`;
                            const fileSize = file && file.length ? file.length : 0;
                            console.log(`📄 [${id}] File ${index}: ${fileName} (${(fileSize / 1024 / 1024).toFixed(2)}MB)`);
                        });
                    }

                    this.startTorrentMonitoring(streamData);

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
                    console.error(`❌ [${id}] Error in Torrenter ready handler: ${readyError.message}`);
                    streamData.status = 'error';
                    streamData.error = `Torrenter ready handler error: ${readyError.message}`;
                    this.notifyClients(id, 'stream_error', { error: streamData.error });
                }
            };

            engine.on('error', (err) => {
                console.error(`❌ [${id}] Torrenter error:`, err.message);
                streamData.status = 'error';
                streamData.error = err.message;
                streamData.torrentStats.health = 'error';
                this.notifyClients(id, 'stream_error', { error: err.message });
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
            
        } else if (engineType === 'torrenter') {
            try {
                const torrent = engineInstance;
                if (torrent === 'pending' || !torrent) {
                    return;
                }
                const downloaded = torrent.downloaded || 0;
                const uploaded = torrent.uploaded || 0;
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
                console.error(`❌ [${id}] Error updating Torrenter stats: ${error.message}`);
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

    // Helper function to format bytes
    formatBytes(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }
} // Close the StreamManager class here

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

    // Validate engine type
    const engineType = ['webtorrent', 'torrenter', 'peerflix'].includes(engine) ? engine : 'webtorrent';

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
            
            // Handle different message types
            switch (data.type) {
                case 'ping':
                    // Simple ping/pong for keep-alive
                    ws.send(JSON.stringify({ type: 'pong' }));
                    break;
                case 'stream_action':
                    // Handle stream actions (play, pause, seek, etc.)
                    this.handleStreamAction(data, clientId);
                    break;
                default:
                    console.warn(`⚠️ [${clientId}] Unknown message type: ${data.type}`);
            }
        } catch (e) {
            console.error(`❌ [${clientId}] Message handling error: ${e.message}`);
        }
    });

    // Handle client disconnect
    ws.on('close', () => {
        console.log(`🔌 Client disconnected: ${clientId}`);
        streamManager.removeClientConnection(clientId);
        
        // Optionally, destroy all streams for this client on disconnect
        // streamManager.destroyClientStreams(clientId);
    });
});

// --- Start Server ---
server.listen(CONFIG.PORT, CONFIG.HOST, () => {
    console.log(`🚀 Server running at http://${CONFIG.HOST}:${CONFIG.PORT}/`);
});