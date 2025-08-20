import WebTorrent from 'webtorrent';
import { WebSocketServer } from 'ws';
import express from 'express';
import http from 'http';
import path from 'path';
import { fileURLToPath } from 'url';
import rangeParser from 'range-parser';
import cors from 'cors';
import crypto from 'crypto';

// Simple UUID generator that works in all Node.js versions
function generateSessionId() {
    return 'session_' + Math.random().toString(36).substr(2, 9) + '_' + Date.now().toString(36);
}

// --- Setup ---
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const client = new WebTorrent();

const PORT = 6543; // Changed from 3001

// Use CORS to allow requests from other domains (e.g., a separate frontend)
app.use(cors());

// Serve the HTML file from the 'view' folder
// THE FIX: Remove the '..' to point to the correct directory inside the container.
app.use(express.static(path.join(__dirname, 'view')));

// --- State ---
// Replace the single torrent state with a Map to hold multiple torrents.
// The key will be a unique sessionId.
const activeTorrents = new Map();
const defaultMagnetLink = 'magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c';

// --- Functions ---
// This function is no longer needed as we now support multiple streams.
// function clearPreviousStream() { ... }

function startStream(magnetLink, ws) {
    try {
        console.log(`🔍 [DEBUG] startStream called with magnet: ${magnetLink.substring(0, 80)}...`);
        
        // If this WebSocket connection already has a stream, destroy it before creating a new one.
        if (ws.sessionId) {
            const oldTorrent = activeTorrents.get(ws.sessionId);
            if (oldTorrent) {
                console.log(`🗑️ Clearing previous stream for session: ${ws.sessionId}`);
                client.remove(oldTorrent, { destroyStore: true });
                activeTorrents.delete(ws.sessionId);
            }
        }

        const sessionId = generateSessionId();
        ws.sessionId = sessionId;
        console.log(`🔍 [DEBUG] Generated session ID: ${sessionId}`);

        console.log(`🚀 [${sessionId}] Starting torrent for:`, magnetLink);
        
        // Add error handling for the torrent creation itself
        let torrent;
        try {
            console.log(`🔍 [DEBUG] About to call client.add...`);
            torrent = client.add(magnetLink, { 
                destroyStoreOnDestroy: true,
                // Add these options to make it more robust in Docker
                downloadLimit: -1,
                uploadLimit: -1,
                maxConns: 55,
                tracker: { announce: [] }
            });
            console.log(`🔍 [DEBUG] client.add completed successfully`);
        } catch (addError) {
            console.error(`❌ [${sessionId}] Failed to add torrent:`, addError.message);
            ws.send(JSON.stringify({ type: 'error', message: 'Failed to add torrent. Invalid magnet link.' }));
            return;
        }

        activeTorrents.set(sessionId, torrent);
        console.log(`🔍 [DEBUG] Torrent added to activeTorrents map`);

        // Set a timeout to detect if the torrent never becomes ready
        const readyTimeout = setTimeout(() => {
            console.error(`❌ [${sessionId}] Torrent ready timeout after 30 seconds`);
            ws.send(JSON.stringify({ type: 'error', message: 'Torrent took too long to become ready. Try a different magnet link.' }));
            client.remove(torrent, { destroyStore: true });
            activeTorrents.delete(sessionId);
        }, 30000);

        torrent.on('ready', () => {
            try {
                clearTimeout(readyTimeout);
                console.log(`✅ [${sessionId}] Torrent ready: ${torrent.name}`);
                console.log(`🔍 [DEBUG] Torrent has ${torrent.files.length} files`);
                
                const videoFile = torrent.files.find(file => {
                    const name = file.name.toLowerCase();
                    return name.endsWith('.mp4') || name.endsWith('.mkv') || name.endsWith('.avi');
                });

                if (!videoFile) {
                    console.log(`❌ [${sessionId}] No video file found in torrent`);
                    console.log(`🔍 [DEBUG] Available files:`, torrent.files.map(f => f.name));
                    ws.send(JSON.stringify({ type: 'error', message: 'No video file found.' }));
                    return;
                }
                
                console.log(`✅ [${sessionId}] File ready: ${videoFile.name}`);
                ws.send(JSON.stringify({
                    type: 'streamReady',
                    url: `/stream/${sessionId}?filename=${encodeURIComponent(videoFile.name)}`
                }));
            } catch (readyError) {
                console.error(`❌ [${sessionId}] Error in ready handler:`, readyError.message);
                ws.send(JSON.stringify({ type: 'error', message: 'Error processing torrent files.' }));
            }
        });

        torrent.on('error', (err) => {
            clearTimeout(readyTimeout);
            console.error(`❌ [${sessionId}] Torrent error:`, err.message);
            ws.send(JSON.stringify({ type: 'error', message: 'Invalid magnet link or torrent error.' }));
            activeTorrents.delete(sessionId);
        });

        // Add more event listeners for debugging
        torrent.on('download', () => {
            console.log(`🔍 [${sessionId}] Download progress: ${(torrent.progress * 100).toFixed(1)}%`);
        });

        torrent.on('wire', (wire) => {
            console.log(`🔍 [${sessionId}] Connected to peer`);
        });

    } catch (error) {
        console.error('❌ Critical error in startStream:', error.message);
        console.error('Stack trace:', error.stack);
        ws.send(JSON.stringify({ type: 'error', message: 'Server error occurred.' }));
    }
}

// --- HTTP Streaming Endpoint ---
// The route now includes the sessionId parameter.
app.get('/stream/:sessionId', (req, res) => {
    const { sessionId } = req.params;
    const torrent = activeTorrents.get(sessionId); // Look up the specific torrent for this session.

    if (!torrent || !torrent.ready) {
        return res.status(404).send('Stream not found or not ready. Please select a torrent first.');
    }

    const { filename } = req.query;
    const file = torrent.files.find(f => f.name === filename);

    if (!file) {
        return res.status(404).send('File not found in torrent.');
    }

    const fileSize = file.length;
    const rangeHeader = req.headers.range;

    let stream;

    if (rangeHeader) {
        const ranges = rangeParser(fileSize, rangeHeader);

        if (ranges === -1 || ranges === -2) {
            res.status(416).send('Malformed Range header');
            return;
        }

        const { start, end } = ranges[0];
        const contentLength = end - start + 1;

        res.status(206); // Partial Content
        res.setHeader('Content-Length', contentLength);
        res.setHeader('Content-Range', `bytes ${start}-${end}/${fileSize}`);
        res.setHeader('Accept-Ranges', 'bytes');
        res.setHeader('Content-Type', 'video/mp4');

        stream = file.createReadStream({ start, end });
    } else {
        res.status(200);
        res.setHeader('Content-Length', fileSize);
        res.setHeader('Content-Type', 'video/mp4');
        stream = file.createReadStream();
    }

    // ** THE FIX IS HERE **
    // Add listeners to gracefully handle stream lifecycle events.

    // When the client closes the connection (e.g., closes the tab), destroy the torrent stream.
    res.on('close', () => {
        if (!stream.destroyed) {
            console.log('Client closed connection, destroying stream.');
            stream.destroy();
        }
    });

    // When the stream has an error (e.g., the torrent was destroyed by a new request),
    // log it and end the response. This prevents the 'afterdestroy' crash.
    stream.on('error', (err) => {
        console.error('Stream error:', err.message);
        // We can't send headers anymore, just end the connection.
        if (!res.headersSent) {
            res.status(500).send('Stream error');
        } else {
            res.end();
        }
    });

    // Pipe the data to the response.
    stream.pipe(res);
});


// --- WebSocket Logic ---
wss.on('connection', ws => {
    console.log('New client connected.');
    ws.send(JSON.stringify({ type: 'connected' }));

    ws.on('message', message => {
        try {
            const data = JSON.parse(message);
            if (data.type === 'setMagnetLink' && data.magnetLink) {
                startStream(data.magnetLink, ws);
            } else if (data.type === 'startDefault') {
                startStream(defaultMagnetLink, ws);
            }
        } catch (e) {
            console.error('Failed to parse message:', e);
        }
    });

    // When a client disconnects, clean up their associated torrent.
    ws.on('close', () => {
        console.log('Client disconnected.');
        if (ws.sessionId) {
            const torrent = activeTorrents.get(ws.sessionId);
            if (torrent) {
                console.log(`🧹 Cleaning up torrent for session: ${ws.sessionId}`);
                client.remove(torrent, { destroyStore: true });
                activeTorrents.delete(ws.sessionId);
            }
        }
    });
});

// --- Start Server ---
server.listen(PORT, () => {
    console.log(`🚀 Server running at http://localhost:${PORT}`);
});
