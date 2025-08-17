
import WebTorrent from 'webtorrent';
import { WebSocketServer } from 'ws';

// Replace with your own magnet URI or torrent hash
// Using Ubuntu 22.04 Desktop ISO as a test torrent (always has seeders)
const torrentId = 'magnet:?xt=urn:btih:7969590D4697BDF90F680729D15560C9F95B160E';

const client = new WebTorrent();
const wss = new WebSocketServer({ port: 3000 });

let bufferChunks = [];

// Start downloading the torrent immediately
console.log('Starting torrent download...');
console.log('Torrent ID:', torrentId);

const torrent = client.add(torrentId);

torrent.on('metadata', () => {
  console.log('Torrent metadata loaded:', torrent.name);
  console.log('Number of files:', torrent.files.length);
  console.log('Total size:', torrent.length, 'bytes');
  const file = torrent.files[0]; // Streaming first file only for simplicity
  const stream = file.createReadStream();

  stream.on('data', chunk => {
    bufferChunks.push(chunk); // Buffer for new clients
    // Send chunk to all connected clients
    wss.clients.forEach(ws => {
      if (ws.readyState === 1) { // WebSocket.OPEN
        ws.send(chunk);
      }
    });
    console.log(`Sent chunk of size: ${chunk.length} bytes`);
  });

  stream.on('end', () => {
    wss.clients.forEach(ws => {
      if (ws.readyState === 1) { // WebSocket.OPEN
        ws.send('done');
      }
    });
    console.log('Streaming complete.');
  });
});

torrent.on('error', err => {
  console.error('Torrent error:', err);
});

torrent.on('warning', err => {
  console.warn('Torrent warning:', err);
});

// Log torrent progress every 5 seconds
setInterval(() => {
  if (torrent.progress > 0) {
    console.log(`Progress: ${(torrent.progress * 100).toFixed(1)}%, Download speed: ${torrent.downloadSpeed} bytes/s, Peers: ${torrent.numPeers}`);
  } else {
    console.log(`Looking for peers... Current peers: ${torrent.numPeers}`);
  }
}, 5000);

// Add error handling and progress tracking
client.on('error', err => {
  console.error('WebTorrent client error:', err);
});

// WebSocket server setup
console.log('WebSocket server starting on port 8000...');

wss.on('listening', () => {
  console.log('WebSocket server is listening on port 8000');
});

wss.on('connection', ws => {
  console.log('New browser client connected');
  bufferChunks.forEach(chunk => {
    if (ws.readyState === 1) { // WebSocket.OPEN
      ws.send(chunk);
    }
  });
});
