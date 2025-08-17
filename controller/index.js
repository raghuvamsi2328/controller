
import WebTorrent from 'webtorrent';
import { WebSocketServer } from 'ws';

// Replace with your own magnet URI or torrent hash
// Using Ubuntu 22.04 Desktop ISO as a test torrent (always has seeders)
const torrentId = 'magnet:?xt=urn:btih:7969590D4697BDF90F680729D15560C9F95B160E';

const client = new WebTorrent();
const wss = new WebSocketServer({ port: 3001 });

let bufferChunks = [];

// Start downloading the torrent immediately
console.log('Starting torrent download...');
console.log('Torrent ID:', torrentId);

const torrent = client.add(torrentId);

torrent.on('metadata', () => {
  console.log('Torrent metadata loaded:', torrent.name);
  console.log('Number of files:', torrent.files.length);
  console.log('Total size:', torrent.length, 'bytes');
  
  // Filter only MP4 and MKV files
  const videoFiles = torrent.files.filter(file => {
    const extension = file.name.toLowerCase().split('.').pop();
    return extension === 'mp4' || extension === 'mkv';
  });
  
  console.log('Video files found:', videoFiles.length);
  videoFiles.forEach((file, index) => {
    console.log(`${index + 1}. ${file.name} (${file.length} bytes)`);
  });
  
  if (videoFiles.length === 0) {
    console.log('No MP4 or MKV files found in this torrent');
    // Notify WebSocket clients that no video files were found
    wss.clients.forEach(ws => {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify({
          type: 'error',
          message: 'No MP4 or MKV files found in this torrent'
        }));
      }
    });
    return;
  }
  
  // Stream the first video file found (you can modify this to stream multiple files)
  const file = videoFiles[0];
  console.log(`Starting to stream: ${file.name}`);
  
  // Send file info to WebSocket clients
  wss.clients.forEach(ws => {
    if (ws.readyState === 1) {
      ws.send(JSON.stringify({
        type: 'fileInfo',
        name: file.name,
        size: file.length,
        totalFiles: videoFiles.length
      }));
    }
  });
  
  const stream = file.createReadStream();

  stream.on('data', chunk => {
    bufferChunks.push(chunk); // Buffer for new clients
    // Send chunk to all connected clients
    wss.clients.forEach(ws => {
      if (ws.readyState === 1) { // WebSocket.OPEN
        // Send raw binary data for better performance
        ws.send(chunk);
      }
    });
    console.log(`Sent chunk of size: ${chunk.length} bytes (${file.name})`);
  });

  stream.on('end', () => {
    wss.clients.forEach(ws => {
      if (ws.readyState === 1) { // WebSocket.OPEN
        ws.send(JSON.stringify({
          type: 'streamEnd',
          message: `Streaming complete for ${file.name}`
        }));
      }
    });
    console.log(`Streaming complete for: ${file.name}`);
  });

  stream.on('error', (err) => {
    console.error(`Stream error for ${file.name}:`, err);
    wss.clients.forEach(ws => {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify({
          type: 'streamError',
          message: `Stream error: ${err.message}`
        }));
      }
    });
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
console.log('WebSocket server starting on port 3001...');

wss.on('listening', () => {
  console.log('WebSocket server is listening on port 3001');
});

wss.on('connection', ws => {
  console.log('New browser client connected');
  
  // Send connection acknowledgment
  ws.send(JSON.stringify({
    type: 'connected',
    message: 'Connected to WebTorrent streaming server'
  }));
  
  // Send all buffered chunks to new client (raw binary data)
  bufferChunks.forEach(chunk => {
    if (ws.readyState === 1) { // WebSocket.OPEN
      ws.send(chunk);
    }
  });
  
  // Handle client disconnection
  ws.on('close', () => {
    console.log('Browser client disconnected');
  });
  
  ws.on('error', (err) => {
    console.error('WebSocket client error:', err);
  });
});
