import WebTorrent from 'webtorrent';
import { WebSocketServer } from 'ws';
import express from 'express';
import http from 'http';
import path from 'path';
import { fileURLToPath } from 'url';
import rangeParser from 'range-parser';
import cors from 'cors';
import crypto from 'crypto'; // <-- ADD THIS LINE

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
app.use(express.static(path.join(__dirname, '..', 'view')));

// --- State ---
// Replace the single torrent state with a Map to hold multiple torrents.
// The key will be a unique sessionId.
const activeTorrents = new Map();
const defaultMagnetLink = 'magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c';

// --- Functions ---
// This function is no longer needed as we now support multiple streams.
// function clearPreviousStream() { ... }

function startStream(magnetLink, ws) {
    // If this WebSocket connection already has a stream, destroy it before creating a new one.
    if (ws.sessionId) {
        const oldTorrent = activeTorrents.get(ws.sessionId);
        if (oldTorrent) {
            console.log(`🗑️ Clearing previous stream for session: ${ws.sessionId}`);
            client.remove(oldTorrent, { destroyStore: true });
            activeTorrents.delete(ws.sessionId);
        }
    }

    const sessionId = crypto.randomUUID(); // Generate a unique ID for this stream session.
    ws.sessionId = sessionId; // Associate the session ID with the WebSocket connection.

    console.log(`🚀 [${sessionId}] Starting torrent for:`, magnetLink);
    const torrent = client.add(magnetLink, { destroyStoreOnDestroy: true });
    activeTorrents.set(sessionId, torrent); // Add the new torrent to our collection.

    torrent.on('ready', () => {
        const videoFile = torrent.files.find(file => 
            file.name.endsWith('.mp4') || file.name.endsWith('.mkv')
        );

        if (!videoFile) {
            ws.send(JSON.stringify({ type: 'error', message: 'No video file found.' }));
            return;
        }
        
        console.log(`✅ [${sessionId}] File ready: ${videoFile.name}`);
        // The URL now includes the unique sessionId.
        ws.send(JSON.stringify({
            type: 'streamReady',
            url: `/stream/${sessionId}?filename=${encodeURIComponent(videoFile.name)}`
        }));
    });

    torrent.on('error', (err) => {
        console.error(`❌ [${sessionId}] Torrent error:`, err.message);
        ws.send(JSON.stringify({ type: 'error', message: 'Invalid magnet link or torrent error.' }));
        activeTorrents.delete(sessionId); // Clean up on error.
    });
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
