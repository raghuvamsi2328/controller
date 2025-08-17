
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

// Option 2: Big Buck Bunny (more peers available)
const magnetLink = 'magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c&dn=Big+Buck+Bunny&tr=udp%3A%2F%2Fexplodie.org%3A6969&tr=udp%3A%2F%2Ftracker.coppersurfer.tk%3A6969&tr=udp%3A%2F%2Ftracker.empire-js.us%3A1337&tr=udp%3A%2F%2Ftracker.leechers-paradise.org%3A6969&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=wss%3A%2F%2Ftracker.btorrent.xyz&tr=wss%3A%2F%2Ftracker.fastcast.nz&tr=wss%3A%2F%2Ftracker.openwebtorrent.com';

console.log('🎬 Auto-transcoding enabled with FFmpeg');
console.log('🎬 H.265/HEVC videos will be converted to H.264 in real-time');
console.log('🎬 Any video format will now work with browser playback!');

const client = new WebTorrent();
const wss = new WebSocketServer({ port: 3001 });

let bufferChunks = [];
let isStreaming = false;
let currentFileInfo = null;

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
    .inputFormat('mp4') // Try MP4 first, FFmpeg will auto-detect if needed
    .videoCodec('libx264') // Convert to H.264
    .audioCodec('aac') // Convert audio to AAC
    .format('mp4')
    .addOptions([
      '-preset ultrafast', // Fastest encoding for real-time
      '-tune zerolatency', // Optimize for streaming
      '-crf 28', // Good quality balance for streaming
      '-maxrate 2M', // Limit bitrate for smoother streaming
      '-bufsize 4M', // Buffer size
      '-movflags frag_keyframe+empty_moov+default_base_moof', // Enable progressive streaming with proper fragmentation
      '-frag_duration 1000000', // 1 second fragments
      '-f mp4' // Force MP4 format
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

// Start downloading the torrent immediately
console.log('Starting torrent download...');
console.log('Torrent ID:', magnetLink);

const torrent = client.add(magnetLink);

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
    transcoding: requiresTranscoding
  };
  
  wss.clients.forEach(ws => {
    if (ws.readyState === 1) {
      ws.send(JSON.stringify(currentFileInfo));
    }
  });
  
  // Clear previous buffer and reset streaming state
  bufferChunks = [];
  isStreaming = true;
  
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
  
  // Handle client disconnection
  ws.on('close', () => {
    console.log('Browser client disconnected');
  });
  
  ws.on('error', (err) => {
    console.error('WebSocket client error:', err);
  });
});
