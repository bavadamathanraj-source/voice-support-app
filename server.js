const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Serve static web pages from the current directory
app.use(express.static('.'));

wss.on('connection', (ws, req) => {
    console.log('A user connected from IP:', req.socket.remoteAddress);

    ws.on('message', async (message) => {
        try {
            // Handle message data whether it comes as Buffer, Blob, or String
            const messageString = Buffer.isBuffer(message) ? message.toString('utf8') : String(message);
            const data = JSON.parse(messageString);
            
            console.log('Received signal type:', data.type);

            // Broadcast the exact message string to all other connected clients (Customer <-> Agent)
            wss.clients.forEach((client) => {
                if (client !== ws && client.readyState === client.OPEN) {
                    client.send(messageString);
                }
            });
        } catch (err) {
            console.error('Failed to parse incoming message:', err);
        }
    });

    ws.on('close', () => {
        console.log('A user disconnected.');
    });

    ws.on('error', (error) => {
        console.error('WebSocket error encountered:', error);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Signaling server running on port ${PORT}`);
});