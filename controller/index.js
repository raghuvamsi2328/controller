import WebTorrent from 'webtorrent';
import { WebSocketServer } from 'ws';
import express from 'express';
import http from 'http';
import path from 'path';
import { fileURLToPath } from 'url';
import rangeParser from 'range-parser';
import cors from 'cors';

// --- Setup ---
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const client = new WebTorrent();

const PORT = 3001;

// Use CORS to allow requests from other domains (e.g., a separate frontend)
app.use(cors());

// Serve the HTML file from the 'view' folder
app.use(express.static(path.join(__dirname, '..', 'view')));

// --- State ---
let activeTorrent = null;
const defaultMagnetLink = 'magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c';

// --- Functions ---
function clearPreviousStream() {
    console.log('🗑️ Clearing previous stream...');
    if (activeTorrent) {
        client.remove(activeTorrent, { destroyStore: true });
        activeTorrent = null;
    }
}

function startStream(magnetLink, ws) {
    clearPreviousStream();
    console.log('Starting torrent for:', magnetLink);

    activeTorrent = client.add(magnetLink, { destroyStoreOnDestroy: true });

    activeTorrent.on('ready', () => {
        const videoFile = activeTorrent.files.find(file => 
            file.name.endsWith('.mp4') || file.name.endsWith('.mkv')
        );

        if (!videoFile) {
            ws.send(JSON.stringify({ type: 'error', message: 'No video file found.' }));
            return;
        }
        
        console.log(`✅ File ready: ${videoFile.name}`);
        ws.send(JSON.stringify({
            type: 'streamReady',
            url: `/stream?filename=${encodeURIComponent(videoFile.name)}`
        }));
    });

    activeTorrent.on('error', (err) => {
        console.error('❌ Torrent error:', err.message);
        ws.send(JSON.stringify({ type: 'error', message: 'Invalid magnet link or torrent error.' }));
    });
}

// --- HTTP Streaming Endpoint ---
app.get('/stream', (req, res) => {
    if (!activeTorrent || !activeTorrent.ready) {
        return res.status(404).send('No active stream. Please select a torrent first.');
    }

    const { filename } = req.query;
    const file = activeTorrent.files.find(f => f.name === filename);

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

    ws.on('close', () => console.log('Client disconnected.'));
});

// --- Start Server ---
server.listen(PORT, () => {
    console.log(`🚀 Server running at http://localhost:${PORT}`);
});
