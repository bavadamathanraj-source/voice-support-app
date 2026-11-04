const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static('public'));

// Data Structures for MNC-Grade Call Center State
const agents = new Map();     // ws -> { id, status: 'IDLE' | 'BUSY', currentCustomerWs }
const customers = new Map();  // ws -> { id, phoneNumber, status: 'WAITING' | 'IN_CALL' }
const waitingQueue = [];      // Array of customer ws objects waiting for an agent

wss.on('connection', (ws, req) => {
    console.log('New connection established.');

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);

            switch (data.type) {
                case 'REGISTER_AGENT':
                    agents.set(ws, { id: data.agentId, status: 'IDLE', currentCustomerWs: null });
                    console.log(`Agent registered: ${data.agentId} (Status: IDLE)`);
                    broadcastQueueStats();
                    break;

                case 'REGISTER_CUSTOMER':
                    customers.set(ws, { id: ws, phoneNumber: data.phoneNumber, status: 'WAITING' });
                    attemptRouteCustomer(ws);
                    break;

                // WebRTC Signaling Forwarder (Scoped strictly between assigned pair)
                case 'OFFER':
                case 'ANSWER':
                case 'CANDIDATE':
                    if (data.targetWsId) {
                        // Forward directly to the specific paired peer
                        wss.clients.forEach(client => {
                            if (client === data.targetWsId && client.readyState === client.OPEN) {
                                client.send(message);
                            }
                        });
                    }
                    break;

                case 'AGENT_TOGGLE_STATUS':
                    const agent = agents.get(ws);
                    if (agent && agent.status !== 'BUSY') {
                        agent.status = data.status; // 'IDLE' or 'OFFLINE'
                        console.log(`Agent ${agent.id} status changed to ${agent.status}`);
                    }
                    break;

                case 'HANGUP':
                    handleCallTermination(ws);
                    break;
            }
        } catch (err) {
            console.error('Error handling message:', err);
        }
    });

    ws.on('close', () => {
        if (agents.has(ws)) {
            console.log(`Agent disconnected: ${agents.get(ws).id}`);
            agents.delete(ws);
        } else if (customers.has(ws)) {
            console.log('Customer disconnected.');
            const queueIndex = waitingQueue.indexOf(ws);
            if (queueIndex > -1) waitingQueue.splice(queueIndex, 1);
            handleCallTermination(ws);
            customers.delete(ws);
        }
        broadcastQueueStats();
    });
});

// Smart Routing Engine (FIFO + Availability Check)
function attemptRouteCustomer(customerWs) {
    // Find the first available IDLE agent
    let availableAgentWs = null;
    for (let [agentWs, agentData] of agents.entries()) {
        if (agentData.status === 'IDLE') {
            availableAgentWs = agentWs;
            break;
        }
    }

    if (availableAgentWs) {
        // Route immediately
        const agentData = agents.get(availableAgentWs);
        agentData.status = 'BUSY';
        agentData.currentCustomerWs = customerWs;

        const customerData = customers.get(customerWs);
        customerData.status = 'IN_CALL';

        // Notify Agent and Customer to connect via WebRTC
        availableAgentWs.send(JSON.stringify({
            type: 'INCOMING_CALL',
            customerWsId: customerWs,
            phoneNumber: customerData.phoneNumber
        }));

        customerWs.send(JSON.stringify({
            type: 'CALL_CONNECTED',
            agentId: agentData.id
        }));

        console.log(`Routed customer (${customerData.phoneNumber}) to agent (${agentData.id})`);
    } else {
        // All agents busy -> Push to FIFO Queue
        if (!waitingQueue.includes(customerWs)) {
            waitingQueue.push(customerWs);
            console.log(`All agents busy. Customer added to queue. Position: ${waitingQueue.length}`);
        }
        customerWs.send(JSON.stringify({
            type: 'QUEUED',
            position: waitingQueue.indexOf(customerWs) + 1
        }));
    }
    broadcastQueueStats();
}

function handleCallTermination(ws) {
    // Check if it was an agent ending the call
    for (let [agentWs, agentData] of agents.entries()) {
        if (agentWs === ws || agentData.currentCustomerWs === ws) {
            const partnerWs = (agentWs === ws) ? agentData.currentCustomerWs : agentWs;
            
            // Free the agent
            agentData.status = 'IDLE';
            agentData.currentCustomerWs = null;

            if (partnerWs && partnerWs.readyState === partnerWs.OPEN) {
                partnerWs.send(JSON.stringify({ type: 'HANGUP' }));
            }

            console.log(`Call ended. Agent ${agentData.id} is now IDLE.`);

            // Pull next customer from FIFO queue if any waiting
            if (waitingQueue.length > 0) {
                const nextCustomer = waitingQueue.shift();
                attemptRouteCustomer(nextCustomer);
            }
        }
    }
    broadcastQueueStats();
}

function broadcastQueueStats() {
    const stats = {
        type: 'QUEUE_STATS',
        waitingCount: waitingQueue.length,
        activeAgents: Array.from(agents.values()).filter(a => a.status === 'BUSY').length,
        idleAgents: Array.from(agents.values()).filter(a => a.status === 'IDLE').length
    };

    wss.clients.forEach(client => {
        if (client.readyState === client.OPEN) {
            client.send(JSON.stringify(stats));
        }
    });
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Enterprise Call Center Signaling Server running on port ${PORT}`);
});