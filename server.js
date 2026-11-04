const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Serve static web pages from a folder named 'public'
app.use(express.static('public'));

wss.on('connection', (ws) => {
    console.log('A user connected!');

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            console.log('Received signal:', data.type);

            // Handle call decline explicitly if needed
            if (data.type === 'decline-call') {
                console.log('Call declined by agent.');
            }

            // Pass signals between Customer and Agent (broadcast to other connected clients)
            wss.clients.forEach((client) => {
                if (client !== ws && client.readyState === client.OPEN) {
                    client.send(message); // Forward the exact message string received
                }
            });
        } catch (err) {
            console.error('Failed to parse incoming message:', err);
        }
    });

    ws.on('close', () => console.log('A user disconnected.'));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});