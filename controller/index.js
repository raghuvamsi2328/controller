import WebTorrent from 'webtorrent';
import { WebSocketServer } from 'ws';
import ffmpeg from 'fluent-ffmpeg';
import ffmpegInstaller from '@ffmpeg-installer/ffmpeg';
import express from 'express';
import http from 'http';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import os from 'os'; // <-- ADD THIS LINE

// Tell fluent-ffmpeg where to find the binary
ffmpeg.setFfmpegPath(ffmpegInstaller.path);

// --- Setup ---
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const client = new WebTorrent();

// CHANGE THIS LINE
const HLS_DIR = path.join(os.tmpdir(), 'webtorrent-streamer-hls');
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
    if (ffmpegProcess) {
        ffmpegProcess.kill('SIGKILL');
        ffmpegProcess = null;
        console.log('🔪 Killed previous FFmpeg process.');
    }
    client.torrents.forEach(torrent => {
        console.log('Removing torrent:', torrent.name || 'Unknown');
        client.remove(torrent, { destroyStore: true });
    });
    fs.readdirSync(HLS_DIR).forEach(file => {
        fs.unlinkSync(path.join(HLS_DIR, file));
    });
    console.log('🧹 Cleaned HLS directory.');
}

function waitForPlaylist(playlistPath, callback, timeout = 30000) {
    console.log('⏳ Waiting for playlist to be created...');
    const interval = 200;
    let elapsedTime = 0;
    const check = setInterval(() => {
        if (fs.existsSync(playlistPath)) {
            clearInterval(check);
            console.log('✅ Playlist found! Notifying client.');
            callback(null);
        } else {
            elapsedTime += interval;
            if (elapsedTime >= timeout) {
                clearInterval(check);
                const timeoutError = new Error('Timed out waiting for playlist file.');
                console.error(`❌ ${timeoutError.message}`);
                callback(timeoutError);
            }
        }
    }, interval);
}

function startStream(magnetLink) {
    clearPreviousStream();
    console.log('Starting torrent for:', magnetLink);

    const torrent = client.add(magnetLink, { destroyStoreOnDestroy: true });

    torrent.on('error', (err) => {
        console.error('❌ Top-level torrent error:', err.message);
        broadcast({ type: 'error', message: 'Invalid magnet link or torrent error.' });
    });

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

        // --- NEW DIAGNOSTIC LOGGING ---
        // Log all major events on the source stream to see if it's closing unexpectedly.
        sourceStream.on('error', (err) => {
            console.error('❌ Torrent stream read error:', err.message);
            broadcast({ type: 'error', message: 'Failed to read from torrent stream.' });
            if (ffmpegProcess) ffmpegProcess.kill('SIGKILL');
        });
        sourceStream.on('end', () => console.log('ℹ️ Source stream: "end" event fired.'));
        sourceStream.on('close', () => console.log('ℹ️ Source stream: "close" event fired.'));
        // --- END NEW DIAGNOSTIC LOGGING ---

        ffmpegProcess = ffmpeg(sourceStream)
            .videoCodec('libx264')
            .audioCodec('aac')
            .addOptions([
                '-hls_time 10',
                '-hls_list_size 6',
                '-hls_flags delete_segments',
                '-preset ultrafast',
                '-tune zerolatency'
            ])
            .on('start', (commandLine) => {
                console.log('🚀 FFmpeg started.');
                waitForPlaylist(playlistPath, (err) => {
                    if (err) {
                        broadcast({ type: 'error', message: 'Stream failed to start in time.' });
                        return;
                    }
                    broadcast({ 
                        type: 'streamReady', 
                        url: '/hls/playlist.m3u8' 
                    });
                });
            })
            .on('error', (err, stdout, stderr) => {
                if (!err.message.includes('SIGKILL')) {
                    console.error('❌ FFmpeg process error:', err.message);
                    broadcast({ type: 'error', message: 'Failed to transcode video.' });
                }
            })
            .on('end', () => {
                console.log('✅ FFmpeg processing finished.');
            })
            .save(playlistPath);
    });
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

// --- NEW & IMPROVED GLOBAL ERROR HANDLERS ---
process.on('uncaughtException', (err, origin) => {
    console.error('🔥🔥🔥 UNCAUGHT EXCEPTION! 🔥🔥🔥');
    console.error(`Caught exception: ${err}\n` + `Exception origin: ${origin}`);
    console.error(err.stack);
    broadcast({ type: 'error', message: 'A fatal server error occurred (uncaughtException).' });
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('🔥🔥🔥 UNHANDLED REJECTION! 🔥🔥🔥');
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    broadcast({ type: 'error', message: 'A fatal server error occurred (unhandledRejection).' });
});

process.on('exit', (code) => {
    console.log(`👋 Process is exiting with code: ${code}`);
});
// --- END NEW HANDLERS ---
