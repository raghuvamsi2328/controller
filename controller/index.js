import WebTorrent from 'webtorrent';
import { WebSocketServer } from 'ws';
import ffmpeg from 'fluent-ffmpeg';
import express from 'express';
import http from 'http';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

// --- Setup ---
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const client = new WebTorrent();

const HLS_DIR = path.join(__dirname, 'hls');
const PORT = 3001;

// Ensure HLS directory exists and is clean
if (fs.existsSync(HLS_DIR)) {
    fs.rmSync(HLS_DIR, { recursive: true, force: true });
}
fs.mkdirSync(HLS_DIR);

// Serve the HLS files
app.use('/hls', express.static(HLS_DIR));

// --- State ---
let ffmpegProcess = null;
const defaultMagnetLink = 'magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c'; // Big Buck Bunny

// --- Functions ---
function clearPreviousStream() {
    console.log('🗑️ Clearing previous stream...');
    // Stop any running ffmpeg process
    if (ffmpegProcess) {
        ffmpegProcess.kill('SIGKILL');
        ffmpegProcess = null;
        console.log('🔪 Killed previous FFmpeg process.');
    }

    // Remove all torrents
    client.torrents.forEach(torrent => {
        console.log('Removing torrent:', torrent.name || 'Unknown');
        client.remove(torrent, { destroyStore: true });
    });

    // Clean HLS directory
    fs.readdirSync(HLS_DIR).forEach(file => {
        fs.unlinkSync(path.join(HLS_DIR, file));
    });
    console.log('🧹 Cleaned HLS directory.');
}

function startStream(magnetLink) {
    clearPreviousStream();
    console.log('Starting torrent for:', magnetLink);

    const torrent = client.add(magnetLink, { destroyStoreOnDestroy: true });

    torrent.on('ready', () => {
        console.log('✅ Torrent ready:', torrent.name);
        const videoFile = torrent.files.find(file => 
            file.name.endsWith('.mp4') || file.name.endsWith('.mkv')
        );

        if (!videoFile) {
            console.error('❌ No video file found in torrent.');
            broadcast({ type: 'error', message: 'No MP4 or MKV file found in the torrent.' });
            return;
        }

        console.log(`🎬 Streaming file: ${videoFile.name}`);
        const sourceStream = videoFile.createReadStream();
        const playlistPath = path.join(HLS_DIR, 'playlist.m3u8');

        ffmpegProcess = ffmpeg(sourceStream)
            .videoCodec('libx264')
            .audioCodec('aac')
            .addOptions([
                '-hls_time 10',          // 10-second segments
                '-hls_list_size 6',      // Keep 6 segments in the playlist
                '-hls_flags delete_segments', // Delete old segments
                '-preset ultrafast',
                '-tune zerolatency'
            ])
            .on('start', (commandLine) => {
                console.log('🚀 FFmpeg started:', commandLine);
                // Give FFmpeg a moment to create the first files
                setTimeout(() => {
                    broadcast({ 
                        type: 'streamReady', 
                        url: '/hls/playlist.m3u8' 
                    });
                }, 5000);
            })
            .on('error', (err) => {
                console.error('❌ FFmpeg error:', err.message);
                broadcast({ type: 'error', message: 'Failed to transcode video.' });
            })
            .on('end', () => {
                console.log('✅ FFmpeg processing finished.');
            })
            .save(playlistPath);
    });

    torrent.on('error', err => console.error('Torrent error:', err));
}

// --- WebSocket Logic ---
function broadcast(data) {
    wss.clients.forEach(ws => {
        if (ws.readyState === 1) { // WebSocket.OPEN
            ws.send(JSON.stringify(data));
        }
    });
}

wss.on('connection', ws => {
    console.log('New client connected.');
    ws.send(JSON.stringify({ type: 'connected' }));

    ws.on('message', message => {
        try {
            const data = JSON.parse(message);
            console.log('Received message:', data.type);
            if (data.type === 'setMagnetLink' && data.magnetLink) {
                startStream(data.magnetLink);
            } else if (data.type === 'startDefault') {
                startStream(defaultMagnetLink);
            }
        } catch (e) {
            console.error('Failed to parse message:', e);
        }
    });

    ws.on('close', () => console.log('Client disconnected.'));
});

// --- Start Server ---
server.listen(PORT, () => {
    console.log(`🚀 Server running at http://localhost:${PORT}`);
    console.log('🎬 Waiting for client to provide a magnet link...');
});
