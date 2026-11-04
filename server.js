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
        const data = JSON.parse(message);
        console.log('Received signal:', data.type);
        // Add this inside your ws.on('message', ...) handler:
  if (data.type === 'decline-call') {
    console.log('Call declined by agent.');
    
    // Send a message to the customer that the call was declined
    if (customerSocket && customerSocket.readyState === WebSocket.OPEN) {
      customerSocket.send(JSON.stringify({ type: 'call-declined' }));
    }
  }

        // Pass signals between Customer and Agent
        wss.clients.forEach((client) => {
            if (client !== ws && client.readyState === ws.OPEN) {
                client.send(JSON.stringify(data));
            }
        });
    });

    ws.on('close', () => console.log('A user disconnected.'));
});

// Change this:
// server.listen(3000, ...)

// To this:
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
