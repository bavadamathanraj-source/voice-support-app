const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static('public'));

// Maps using String IDs instead of Raw Sockets
const agents = new Map();     // socketId -> { id, agentName, status: 'IDLE'|'BUSY', currentCustomerSocketId, ws }
const customers = new Map();  // socketId -> { id, phoneNumber, status: 'WAITING'|'IN_CALL', ws }
const waitingQueue = [];      // Array of customer socketId strings

wss.on('connection', (ws) => {
    const socketId = crypto.randomUUID();
    ws.socketId = socketId;

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);

            switch (data.type) {
                case 'REGISTER_AGENT':
                    agents.set(socketId, {
                        id: socketId,
                        agentName: data.agentId,
                        status: 'IDLE',
                        currentCustomerSocketId: null,
                        ws: ws
                    });
                    console.log(`Agent registered: ${data.agentId} [ID: ${socketId}]`);
                    broadcastQueueStats();
                    checkAndDispatchQueue();
                    break;

                case 'REGISTER_CUSTOMER':
                    customers.set(socketId, {
                        id: socketId,
                        phoneNumber: data.phoneNumber,
                        status: 'WAITING',
                        ws: ws
                    });
                    console.log(`Customer registered: ${data.phoneNumber} [ID: ${socketId}]`);
                    attemptRouteCustomer(socketId);
                    break;

                // WebRTC Signaling Protocol (Forwarded directly via Target Socket IDs)
                case 'OFFER':
                case 'ANSWER':
                case 'CANDIDATE':
                    if (data.targetId) {
                        const targetAgent = agents.get(data.targetId);
                        const targetCustomer = customers.get(data.targetId);
                        const targetWs = targetAgent ? targetAgent.ws : (targetCustomer ? targetCustomer.ws : null);

                        if (targetWs && targetWs.readyState === ws.OPEN) {
                            targetWs.send(JSON.stringify({
                                ...data,
                                senderId: socketId
                            }));
                        }
                    }
                    break;

                case 'DECLINE_CALL':
                    handleAgentDecline(socketId);
                    break;

                case 'HANGUP':
                    handleCallTermination(socketId);
                    break;
            }
        } catch (err) {
            console.error('Error parsing message:', err);
        }
    });

    ws.on('close', () => {
        if (agents.has(socketId)) {
            agents.delete(socketId);
        } else if (customers.has(socketId)) {
            const queueIndex = waitingQueue.indexOf(socketId);
            if (queueIndex > -1) waitingQueue.splice(queueIndex, 1);
            handleCallTermination(socketId);
            customers.delete(socketId);
        }
        broadcastQueueStats();
        updateQueuePositions();
    });
});

// Routing Engine
function attemptRouteCustomer(customerSocketId) {
    const customer = customers.get(customerSocketId);
    if (!customer || customer.status === 'IN_CALL') return;

    // Find first free IDLE agent
    let availableAgent = null;
    for (let agent of agents.values()) {
        if (agent.status === 'IDLE') {
            availableAgent = agent;
            break;
        }
    }

    if (availableAgent) {
        // Connect Agent & Customer
        availableAgent.status = 'BUSY';
        availableAgent.currentCustomerSocketId = customerSocketId;
        customer.status = 'IN_CALL';

        // Remove from queue if present
        const qIdx = waitingQueue.indexOf(customerSocketId);
        if (qIdx > -1) waitingQueue.splice(qIdx, 1);

        // Notify both parties
        availableAgent.ws.send(JSON.stringify({
            type: 'INCOMING_CALL',
            customerSocketId: customerSocketId,
            phoneNumber: customer.phoneNumber
        }));

        customer.ws.send(JSON.stringify({
            type: 'CALL_CONNECTED',
            agentSocketId: availableAgent.id,
            agentName: availableAgent.agentName
        }));

        console.log(`Routed Customer ${customer.phoneNumber} -> Agent ${availableAgent.agentName}`);
    } else {
        // Add to FIFO Queue if not already queued
        if (!waitingQueue.includes(customerSocketId)) {
            waitingQueue.push(customerSocketId);
        }
        updateQueuePositions();
    }
    broadcastQueueStats();
}

function checkAndDispatchQueue() {
    if (waitingQueue.length === 0) return;
    const nextCustomerSocketId = waitingQueue[0];
    attemptRouteCustomer(nextCustomerSocketId);
}

function handleAgentDecline(agentSocketId) {
    const agent = agents.get(agentSocketId);
    if (!agent || !agent.currentCustomerSocketId) return;

    const customerSocketId = agent.currentCustomerSocketId;
    const customer = customers.get(customerSocketId);

    // Reset Agent back to IDLE
    agent.status = 'IDLE';
    agent.currentCustomerSocketId = null;

    if (customer) {
        customer.status = 'WAITING';
        // Put customer back at front of queue
        if (!waitingQueue.includes(customerSocketId)) {
            waitingQueue.unshift(customerSocketId);
        }
        customer.ws.send(JSON.stringify({
            type: 'CALL_DECLINED_REQUEUED',
            message: 'Agent declined call. Re-queueing for next available agent...'
        }));
    }

    broadcastQueueStats();
    // Immediately check if another IDLE agent can take this customer
    checkAndDispatchQueue();
}

function handleCallTermination(socketId) {
    let partnerSocketId = null;

    if (agents.has(socketId)) {
        const agent = agents.get(socketId);
        partnerSocketId = agent.currentCustomerSocketId;
        agent.status = 'IDLE';
        agent.currentCustomerSocketId = null;
    } else if (customers.has(socketId)) {
        for (let agent of agents.values()) {
            if (agent.currentCustomerSocketId === socketId) {
                partnerSocketId = agent.id;
                agent.status = 'IDLE';
                agent.currentCustomerSocketId = null;
                break;
            }
        }
    }

    if (partnerSocketId) {
        const targetAgent = agents.get(partnerSocketId);
        const targetCustomer = customers.get(partnerSocketId);
        const partnerWs = targetAgent ? targetAgent.ws : (targetCustomer ? targetCustomer.ws : null);

        if (partnerWs && partnerWs.readyState === 1) {
            partnerWs.send(JSON.stringify({ type: 'HANGUP' }));
        }
    }

    broadcastQueueStats();
    updateQueuePositions();
    checkAndDispatchQueue();
}

function updateQueuePositions() {
    waitingQueue.forEach((customerSocketId, index) => {
        const customer = customers.get(customerSocketId);
        if (customer && customer.ws.readyState === 1) {
            customer.ws.send(JSON.stringify({
                type: 'QUEUED',
                position: index + 1
            }));
        }
    });
}

function broadcastQueueStats() {
    const stats = {
        type: 'QUEUE_STATS',
        waitingCount: waitingQueue.length,
        activeAgents: Array.from(agents.values()).filter(a => a.status === 'BUSY').length,
        idleAgents: Array.from(agents.values()).filter(a => a.status === 'IDLE').length
    };

    wss.clients.forEach(client => {
        if (client.readyState === 1) {
            client.send(JSON.stringify(stats));
        }
    });
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Enterprise Call Center Engine listening on port ${PORT}`);
});