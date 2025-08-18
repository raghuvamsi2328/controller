import WebTorrent from 'webtorrent';
import { WebSocketServer } from 'ws';
import ffmpeg from 'fluent-ffmpeg';
import { PassThrough } from 'stream';

// Replace with your own magnet URI or torrent hash
// Using Ubuntu 22.04 Desktop ISO as a test torrent (always has seeders)
// Hard-coded magnet link for testing (replace with the torrent you want to stream)
// Current torrent: Superman (2025) - H.265 codec (not browser compatible)
// For testing, try a smaller H.264 video torrent
// Add this torrent magnet link
// Option 1: Superman Returns sample
// const magnetLink = 'magnet:?xt=urn:btih:7969590D4697BDF90F680729D15560C9F95B160E';

// Option 2: Big Buck Bunny (more peers available) - Default fallback
const defaultMagnetLink = 'magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c'

let currentMagnetLink = null; // Will be set by client

console.log('🎬 Auto-transcoding enabled with FFmpeg');
console.log('🎬 H.265/HEVC videos will be converted to H.264 in real-time');
console.log('🎬 Any video format will now work with browser playback!');

const client = new WebTorrent();
const wss = new WebSocketServer({ port: 3001 });

let bufferChunks = [];
let isStreaming = false;
let currentFileInfo = null;

// Function to clear all WebTorrent data and restart
function clearCacheAndRestart(newMagnetLink = null) {
  console.log('🗑️ Clearing WebTorrent cache and restarting...');
  
  // Stop current streaming
  isStreaming = false;
  bufferChunks = [];
  currentFileInfo = null;
  
  // Update magnet link if provided
  if (newMagnetLink) {
    currentMagnetLink = newMagnetLink;
    console.log('📎 New magnet link set:', newMagnetLink);
  }
  
  // Remove all existing torrents
  client.torrents.forEach(torrent => {
    console.log('Removing torrent:', torrent.name || 'Unknown');
    client.remove(torrent, { destroyStore: true });
  });
  
  // Wait a bit then restart
  setTimeout(() => {
    console.log('🔄 Restarting torrent download...');
    startTorrentDownload();
  }, 1000);
}

// Function to start torrent download
function startTorrentDownload(magnetLink = null) {
  const linkToUse = magnetLink || currentMagnetLink || defaultMagnetLink;
  currentMagnetLink = linkToUse;
  
  console.log('Starting torrent download...');
  console.log('Torrent ID:', linkToUse);

  const torrent = client.add(linkToUse, { 
    destroyStoreOnDestroy: true,
    storeCacheSlots: 0 // Disable caching
  });

  // Force streaming to start even if torrent is already downloaded
  function startStreaming() {
    console.log('Torrent metadata loaded:', torrent.name);
    console.log('Number of files:', torrent.files.length);
    console.log('Total size:', torrent.length, 'bytes');
    console.log('Progress:', (torrent.progress * 100).toFixed(1) + '%');
    
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
    
    // Stream the first video file found
    const file = videoFiles[0];
    console.log(`Starting to stream: ${file.name}`);
    
    // Check if transcoding is needed
    const requiresTranscoding = needsTranscoding(file.name);
    console.log(`📹 Video analysis: ${file.name}`);
    console.log(`🔍 Requires transcoding: ${requiresTranscoding ? 'YES' : 'NO'}`);
    
    if (requiresTranscoding) {
      console.log('🔄 This video will be transcoded to H.264 for browser compatibility');
    } else {
      console.log('✅ This video should be browser-compatible as-is');
    }
    
    // Send file info to WebSocket clients
    currentFileInfo = {
      type: 'fileInfo',
      name: file.name,
      size: file.length,
      totalFiles: videoFiles.length,
      transcoding: requiresTranscoding,
      codec: 'video/mp4; codecs="avc1.4d4028, mp4a.40.2"'
    };
    
    wss.clients.forEach(ws => {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify(currentFileInfo));
      }
    });
    
    // Clear previous buffer and reset streaming state
    bufferChunks = [];
    isStreaming = true;
    
    console.log('🎬 Creating read stream for file...');
    const originalStream = file.createReadStream();
    
    // Choose stream based on transcoding needs
    const stream = requiresTranscoding ? 
      createTranscodingStream(originalStream, file.name) : 
      originalStream;

    stream.on('data', chunk => {
      if (!isStreaming) return;
      
      // Limit buffer size to prevent memory issues (keep last 50 chunks)
      bufferChunks.push(chunk);
      if (bufferChunks.length > 50) {
        bufferChunks.shift(); // Remove oldest chunk
      }
      
      // Send chunk to all connected clients
      wss.clients.forEach(ws => {
        if (ws.readyState === 1) { // WebSocket.OPEN
          try {
            ws.send(chunk);
          } catch (error) {
            console.error('Error sending chunk to client:', error.message);
          }
        }
      });
      console.log(`Sent chunk of size: ${chunk.length} bytes (${file.name})`);
    });

    stream.on('end', () => {
      isStreaming = false;
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
      isStreaming = false;
      wss.clients.forEach(ws => {
        if (ws.readyState === 1) {
          ws.send(JSON.stringify({
            type: 'streamError',
            message: `Stream error: ${err.message}`
          }));
        }
      });
    });
  }

  torrent.on('metadata', startStreaming);

  // Also try to start streaming if torrent is already ready
  torrent.on('ready', () => {
    console.log('Torrent ready event fired');
    if (!isStreaming) {
      console.log('Starting streaming from ready event...');
      startStreaming();
    }
  });
  
  torrent.on('error', err => {
    console.error('Torrent error:', err);
  });

  torrent.on('warning', err => {
    console.warn('Torrent warning:', err);
  });

  // Log torrent progress every 5 seconds
  const progressInterval = setInterval(() => {
    if (torrent.destroyed) {
      clearInterval(progressInterval);
      return;
    }
    
    if (torrent.progress > 0) {
      console.log(`Progress: ${(torrent.progress * 100).toFixed(1)}%, Download speed: ${torrent.downloadSpeed} bytes/s, Peers: ${torrent.numPeers}`);
    } else {
      console.log(`Looking for peers... Current peers: ${torrent.numPeers}`);
    }
  }, 5000);
}

// Function to check if video needs transcoding
function needsTranscoding(filename) {
  const extension = filename.toLowerCase().split('.').pop();
  const name = filename.toLowerCase();
  
  // Check for H.265/HEVC indicators
  const hasHEVC = name.includes('x265') || 
                  name.includes('hevc') || 
                  name.includes('h265') ||
                  name.includes('265');
  
  // Check for non-MP4 formats that might need transcoding
  const needsFormatConversion = extension === 'mkv' || 
                               extension === 'avi' || 
                               extension === 'mov' ||
                               extension === 'webm';
  
  return hasHEVC || needsFormatConversion;
}

// Function to create FFmpeg transcoding stream
function createTranscodingStream(inputStream, filename) {
  console.log(`🔄 Transcoding ${filename} to browser-compatible H.264 MP4...`);
  
  const outputStream = new PassThrough();
  
  const ffmpegCommand = ffmpeg(inputStream)
    // Let FFmpeg auto-detect the input format from the stream
    .videoCodec('libx264') // Convert to H.264
    .audioCodec('aac') // Convert audio to AAC
    .format('mp4') // Output format
    .addOptions([
      '-preset ultrafast', // Fastest encoding for real-time
      '-tune zerolatency', // Optimize for low-latency streaming
      '-movflags frag_keyframe+empty_moov', // This is the crucial part for MediaSource
      '-f mp4' // Explicitly set the container format
    ]);
  
  // Handle transcoding progress
  ffmpegCommand.on('start', (commandLine) => {
    console.log('🎬 FFmpeg started:', commandLine);
  });
  
  ffmpegCommand.on('progress', (progress) => {
    if (progress.percent) {
      console.log(`🔄 Transcoding progress: ${Math.round(progress.percent)}%`);
    }
  });
  
  ffmpegCommand.on('error', (err) => {
    console.error('❌ FFmpeg transcoding error:', err.message);
    // Try fallback without specific input format
    console.log('🔄 Retrying transcoding with auto-detection...');
    createFallbackTranscodingStream(inputStream, filename, outputStream);
  });
  
  ffmpegCommand.on('end', () => {
    console.log('✅ Transcoding completed successfully');
    outputStream.end();
  });
  
  // Pipe to output stream
  ffmpegCommand.pipe(outputStream, { end: true });
  
  return outputStream;
}

// Fallback transcoding with minimal options
function createFallbackTranscodingStream(inputStream, filename, outputStream) {
  const fallbackCommand = ffmpeg(inputStream)
    .videoCodec('libx264')
    .audioCodec('aac')
    .format('mp4')
    .addOptions([
      '-preset ultrafast',
      '-movflags frag_keyframe+empty_moov'
    ]);
  
  fallbackCommand.on('error', (err) => {
    console.error('❌ Fallback transcoding also failed:', err.message);
    console.log('⚠️ Streaming original video without transcoding...');
    // If transcoding fails, stream original
    inputStream.pipe(outputStream);
  });
  
  fallbackCommand.pipe(outputStream, { end: true });
}

// Start downloading the default torrent only if no magnet link is provided by client
// startTorrentDownload(); // Commented out - will start when client sends magnet link

console.log('🎬 Server ready - waiting for magnet link from client...');

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
  
  // Send current file info if available
  if (currentFileInfo) {
    ws.send(JSON.stringify(currentFileInfo));
  }
  
  // Send buffered chunks to new client (only if currently streaming)
  if (isStreaming && bufferChunks.length > 0) {
    console.log(`Sending ${bufferChunks.length} buffered chunks to new client`);
    bufferChunks.forEach(chunk => {
      if (ws.readyState === 1) { // WebSocket.OPEN
        try {
          ws.send(chunk);
        } catch (error) {
          console.error('Error sending buffered chunk to new client:', error.message);
        }
      }
    });
  }
  
  // Handle messages from client
  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      
      if (data.type === 'restart') {
        console.log('🔄 Client requested restart');
        clearCacheAndRestart();
        ws.send(JSON.stringify({
          type: 'info',
          message: 'Restarting stream...'
        }));
      } 
      else if (data.type === 'setMagnetLink') {
        console.log('🧲 Client sent new magnet link');
        console.log('Link:', data.magnetLink);
        
        // Validate magnet link
        if (!data.magnetLink || (!data.magnetLink.startsWith('magnet:') && !data.magnetLink.length === 40)) {
          ws.send(JSON.stringify({
            type: 'error',
            message: 'Invalid magnet link or hash provided'
          }));
          return;
        }
        
        // Convert hash to magnet link if needed
        let magnetLink = data.magnetLink;
        if (data.magnetLink.length === 40 && !data.magnetLink.startsWith('magnet:')) {
          magnetLink = `magnet:?xt=urn:btih:${data.magnetLink}`;
          console.log('🔗 Converted hash to magnet link:', magnetLink);
        }
        
        ws.send(JSON.stringify({
          type: 'info',
          message: 'Starting stream with new torrent...'
        }));
        
        clearCacheAndRestart(magnetLink);
      }
      else if (data.type === 'startDefault') {
        console.log('🎬 Client requested default torrent');
        ws.send(JSON.stringify({
          type: 'info',
          message: 'Starting default stream...'
        }));
        clearCacheAndRestart(defaultMagnetLink);
      }
      
    } catch (error) {
      console.error('Error parsing client message:', error.message);
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

// Add console commands for manual control
process.stdin.setEncoding('utf8');
process.stdin.on('readable', () => {
  const chunk = process.stdin.read();
  if (chunk !== null) {
    const command = chunk.trim();
    if (command === 'restart' || command === 'r') {
      clearCacheAndRestart();
    } else if (command === 'status' || command === 's') {
      console.log(`Streaming: ${isStreaming}, Buffered chunks: ${bufferChunks.length}, Connected clients: ${wss.clients.size}`);
    } else if (command === 'help' || command === 'h') {
      console.log('Commands: restart/r = restart stream, status/s = show status, help/h = show help');
    }
  }
});

console.log('💡 Type "restart" or "r" to restart streaming, "status" or "s" for status, "help" or "h" for help');
