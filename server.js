const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static('public'));

const agents = new Map();     // ws -> { id, status: 'IDLE' | 'BUSY', currentCustomer: null }
const customers = new Map();  // ws -> { id, phoneNumber, status: 'WAITING' | 'IN_CALL' }
const waitingQueue = [];      // Customer sockets in queue

wss.on('connection', (ws) => {
    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);

            switch (data.type) {
                case 'REGISTER_AGENT':
                    agents.set(ws, { id: data.agentId, status: 'IDLE', currentCustomer: null });
                    broadcastStats();
                    checkQueue();
                    break;

                case 'REGISTER_CUSTOMER':
                    customers.set(ws, { id: ws, phoneNumber: data.phoneNumber, status: 'WAITING' });
                    attemptRoute(ws);
                    break;

                case 'offer':
                case 'answer':
                case 'candidate':
                case 'AGENT_READY_FOR_CALL':
                    wss.clients.forEach(client => {
                        if (client !== ws && client.readyState === 1) {
                            client.send(message);
                        }
                    });
                    break;

                case 'HANGUP':
                case 'DECLINE':
                    handleDisconnect(ws);
                    break;
            }
        } catch (err) {
            console.error('Signal parsing error:', err);
        }
    });

    ws.on('close', () => {
        handleDisconnect(ws);
        agents.delete(ws);
        customers.delete(ws);
        broadcastStats();
    });
});

function attemptRoute(customerWs) {
    let freeAgentWs = null;
    for (let [agentWs, agentData] of agents.entries()) {
        if (agentData.status === 'IDLE') {
            freeAgentWs = agentWs;
            break;
        }
    }

    if (freeAgentWs) {
        const agentData = agents.get(freeAgentWs);
        const customerData = customers.get(customerWs);

        agentData.status = 'BUSY';
        agentData.currentCustomer = customerWs;
        customerData.status = 'IN_CALL';

        const qIdx = waitingQueue.indexOf(customerWs);
        if (qIdx > -1) waitingQueue.splice(qIdx, 1);

        freeAgentWs.send(JSON.stringify({
            type: 'INCOMING_CALL',
            phoneNumber: customerData.phoneNumber
        }));

        customerWs.send(JSON.stringify({
            type: 'CALL_CONNECTED',
            agentId: agentData.id
        }));
    } else {
        if (!waitingQueue.includes(customerWs)) {
            waitingQueue.push(customerWs);
        }
        customerWs.send(JSON.stringify({
            type: 'QUEUED',
            position: waitingQueue.indexOf(customerWs) + 1
        }));
    }
    broadcastStats();
}

function checkQueue() {
    if (waitingQueue.length > 0) {
        for (let [agentWs, agentData] of agents.entries()) {
            if (agentData.status === 'IDLE' && waitingQueue.length > 0) {
                const nextCustomer = waitingQueue.shift();
                attemptRoute(nextCustomer);
                break;
            }
        }
    }
}

function handleDisconnect(ws) {
    if (agents.has(ws)) {
        const agentData = agents.get(ws);
        if (agentData.currentCustomer) {
            const custWs = agentData.currentCustomer;
            const custData = customers.get(custWs);
            if (custData) {
                custData.status = 'WAITING';
                waitingQueue.unshift(custWs);
            }
            if (custWs.readyState === 1) {
                custWs.send(JSON.stringify({ type: 'HANGUP' }));
            }
        }
        agentData.status = 'IDLE';
        agentData.currentCustomer = null;
    } else if (customers.has(ws)) {
        const qIdx = waitingQueue.indexOf(ws);
        if (qIdx > -1) waitingQueue.splice(qIdx, 1);

        for (let [agentWs, agentData] of agents.entries()) {
            if (agentData.currentCustomer === ws) {
                agentData.status = 'IDLE';
                agentData.currentCustomer = null;
                if (agentWs.readyState === 1) {
                    agentWs.send(JSON.stringify({ type: 'HANGUP' }));
                }
                break;
            }
        }
    }
    broadcastStats();
    checkQueue();
}

function broadcastStats() {
    const stats = {
        type: 'QUEUE_STATS',
        waitingCount: waitingQueue.length,
        activeAgents: Array.from(agents.values()).filter(a => a.status === 'BUSY').length,
        idleAgents: Array.from(agents.values()).filter(a => a.status === 'IDLE').length,
        totalAgents: agents.size
    };

    wss.clients.forEach(client => {
        if (client.readyState === 1) {
            client.send(JSON.stringify(stats));
        }
    });
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server listening on port ${PORT}`));