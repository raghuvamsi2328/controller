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
        console.log(`🔍 [${streamId}] Searching for best video file among ${files.length} files`);
        
        // Define video extensions with priority (higher score = better)
        const videoExtensions = {
            '.mp4': 10,   // Best compatibility
            '.mkv': 9,    // High quality, good support
            '.avi': 7,    // Good compatibility
            '.mov': 6,    // Apple format
            '.wmv': 5,    // Windows format
            '.flv': 4,    // Flash video
            '.webm': 8,   // Web format
            '.m4v': 9,    // iTunes format
            '.mpg': 3,    // Older format
            '.mpeg': 3,   // Older format
            '.3gp': 2,    // Mobile format
            '.ts': 6      // Transport stream
        };

        // Filter video files
        const videoFiles = files.filter(file => {
            const extension = path.extname(file.name).toLowerCase();
            return videoExtensions.hasOwnProperty(extension);
        });

        if (videoFiles.length === 0) {
            console.log(`❌ [${streamId}] No video files found`);
            return null;
        }

        console.log(`📹 [${streamId}] Found ${videoFiles.length} video files`);

        // Score each video file
        const scoredFiles = videoFiles.map(file => {
            const extension = path.extname(file.name).toLowerCase();
            const basename = path.basename(file.name).toLowerCase();
            const dirname = path.dirname(file.name).toLowerCase();
            
            let score = 0;
            
            // Base score from extension
            score += videoExtensions[extension] || 0;
            
            // File size scoring (prefer larger files, but not tiny or extremely large)
            const sizeMB = file.length / (1024 * 1024);
            if (sizeMB > 100 && sizeMB < 20000) { // Between 100MB and 20GB
                score += Math.min(10, sizeMB / 1000); // Up to 10 points for size
            } else if (sizeMB <= 100) {
                score -= 5; // Penalize very small files (likely samples/trailers)
            }
            
            // Prefer files not in sample/trailer folders
            if (dirname.includes('sample') || dirname.includes('trailer') || dirname.includes('preview')) {
                score -= 15;
                console.log(`⚠️ [${streamId}] Penalizing sample/trailer: ${file.name}`);
            }
            
            // Penalize sample/trailer files by name
            if (basename.includes('sample') || basename.includes('trailer') || basename.includes('preview')) {
                score -= 10;
                console.log(`⚠️ [${streamId}] Penalizing sample/trailer by name: ${file.name}`);
            }
            
            // Prefer main movie folders
            if (dirname === '.' || dirname === '' || !dirname.includes('/')) {
                score += 5; // Bonus for root-level files
            }
            
            // Bonus for common movie indicators
            if (basename.includes('1080p') || basename.includes('720p') || basename.includes('4k')) {
                score += 3;
            }
            
            // Bonus for main feature indicators
            if (basename.includes('feature') || basename.includes('main')) {
                score += 5;
            }

            return {
                file,
                score,
                extension,
                sizeMB: sizeMB.toFixed(2),
                path: file.name
            };
        });

        // Sort by score (highest first)
        scoredFiles.sort((a, b) => b.score - a.score);

        // Log scoring results
        console.log(`🏆 [${streamId}] Video file scoring results:`);
        scoredFiles.forEach((item, index) => {
            console.log(`  ${index + 1}. ${item.path} (${item.extension}, ${item.sizeMB}MB, score: ${item.score})`);
        });

        const bestFile = scoredFiles[0].file;
        console.log(`✅ [${streamId}] Selected best video: ${bestFile.name}`);
        
        return bestFile;
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
    }

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
                // WebTorrent cleanup
                if (stream.engine) {
                    if (stream.engineInstance && stream.engineInstance !== 'pending') {
                        // Remove the specific torrent
                        stream.engine.remove(stream.engineInstance, {removeTorrent: true});
                    }
                    // Destroy the client
                    stream.engine.destroy();
                }
            } else {
                // Peerflix cleanup
                if (stream.engine) {
                    stream.engine.destroy();
                }
            }
            
            // Clean up downloaded files
            this.cleanupStreamFiles(stream);
            
        } catch (e) {
            console.error(`❌ [${streamId}] Error destroying engine: ${e.message}`);
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

        this.activeStreams.delete(streamId);
        this.notifyClients(streamId, 'stream_destroyed', { reason });
        return true;
    }

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

    // --- rest of your existing methods stay the same ...
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
            res.setHeader('Content-Disposition', 'inline');  // Add this
            // Consider additional headers if needed
        }
        
        // Large file optimizations
        if (isLargeFile) {
            res.setHeader('Transfer-Encoding', 'chunked');
        }

        const streamInstance = videoFile.createReadStream({ 
            start, 
            end,
            highWaterMark: isLargeFile ? 256 * 1024 : 128 * 1024 // Increased buffers
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
            res.setHeader('Content-Disposition', 'inline');  // Add this
            // Consider additional headers if needed
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
