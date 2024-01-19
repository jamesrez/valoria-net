const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const querystring = require('node:querystring'); 
const defaultPort = 3000;
const isLocal = process.env.PORT ? false : true;

function simpleHash(str){
  var hash = 0;
  if (str.length == 0) {
      return hash;
  }
  for (var i = 0; i < str.length; i++) {
      var char = str.charCodeAt(i);
      hash = ((hash<<10)-hash)+char;
      hash = hash & hash; // Convert to 32bit integer
  }
  return Math.abs(hash);
}

function jumpConsistentHash(key, numBuckets) {
  let keyBigInt = BigInt(simpleHash(key));
  let b = -1n;
  let j = 0n;
  while (j < numBuckets) {
      b = j;
      keyBigInt = (keyBigInt * 2862933555777941757n) % (2n ** 64n) + 1n;
      j = BigInt(Math.floor((Number(b) + 1) * Number(1n << 31n) / Number((keyBigInt >> 33n) + 1n)));
  }
  return (Number(b));
}

class Node {
  constructor(port){
    this.app = express();
    this.server = http.createServer(this.app); 
    this.connectErrorCount = 0;
    this.bootstrap = isLocal ? ["http://localhost:3000"] : ["https://valoria-net.onrender.com"]
    this.conns = {};
    this.totalNodes = 1;
    this.syncing = false;
    this.port = port; // Store the port for the WebSocket server
    this.url = process.env.URL || this.determineServerUrl(port);
    this.nodes = [this.url]
    this.groups = {}
    this.connectingTo = {};
    const self = this;
    this.server.listen(port, () => {
      console.log("Valoria Node Running on Port " + port);
      self.wss = new WebSocket.Server({ server: this.server });
      self.wss.on("connection", (ws, req) => {
        const query = querystring.parse(req.url);
        if(query["/?connectCheck"] && self.connectingTo[query["/?connectCheck"]]){
          ws.send(JSON.stringify({event: "URL Confirm", url: self.url, pass: true}))
        } else {
          const url = query["/?url"];
          const wsCheck = new WebSocket(`${url}?connectCheck=${self.url}`)
          wsCheck.on('open', () => {
            wsCheck.on("message", (message) => {
              const data = JSON.parse(message);
              if(data.event == "URL Confirm" && data.pass && url == data.url){
                self.conns[url] = {ws, url, alive: 3}
                ws.on('message', (message) => self.handleWSMsg(self.conns[url], message))
                ws.send(JSON.stringify({event: "Connected to node", url: self.url}))
              }
            })       
          });
        }
      })
      this.connectWithBoostrap()
    });
    this.app.get("/", (req, res) => {
      res.send({
        url: this.url,
        totalNodes: this.totalNodes,
        connections: Object.keys(this.conns),
        groupId: this.groupId,
        group: this.groups[this.groupId],
        groups: this.groups,
      })
    })
  }

  determineServerUrl(port){
    const networkInterfaces = require('os').networkInterfaces();
    const addresses = networkInterfaces['eth0'] || networkInterfaces['wlan0'] || networkInterfaces['lo'] || [];
    for (const address of addresses) {
      if (address.family === 'IPv4') {
        return `http://${address.address}:${port}`;
      }
    }
    return `http://localhost:${port}`;
  }

  handleWSMsg(conn, message, res=null){
    const data = JSON.parse(message);
    if(!conn) return;
    const ws = conn.ws;
    const url = conn.url;
    if(data.event == "Connected to node" && this.connectingTo[data.url]){
      if(res) res(conn);
    }
    if (data.event == 'Alive') {
      this.conns[url].alive += this.conns[url].alive < 3 ? 1 : 0;
    }
    if(data.event == "Request All Nodes"){
      ws.send(JSON.stringify({ event: 'Receive All Nodes', nodes: this.nodes, total: this.totalNodes, from: this.url }));
    }
    if(data.event == "Receive All Nodes"){
      for(let i=0;i<data.nodes.length;i++){
        if(!this.nodes.includes(data.nodes[i])){
          this.nodes.push(data.nodes[i])
          this.totalNodes += 1;
        }
      }
      if(!this.syncing){
        this.startSync();
      }
    }
    if(data.event == "Sync Group Nodes"){
      for(let i=0;i<data.nodes.length;i++){
        if(!this.nodes.includes(data.nodes[i])){
          this.nodes.push(data.nodes[i])
          this.totalNodes += 1;
        }
      }
      ws.send(JSON.stringify({ event: 'Receive All Nodes', nodes: this.nodes, total: this.totalNodes, from: this.url }));
    }
    if (data.event === 'Node Disconnected') {
      this.removeNode(data.url);
    }
  }

  async connectToNode(url){
    const self = this;
    if(url == self.url) return;
    return new Promise(async (res, rej) => {
      try {
        if(self.conns[url]) return res(self.conns[url]);
        self.connectingTo[url] = true;
        const ws = new WebSocket(`${url}?url=${self.url}`);
        ws.on('open', () => {
          if(!self.conns[url]){
            console.log(`${self.url} connected to ${url}`);
          }
          self.conns[url] = {ws, url, alive: 3};
          ws.on("message", (message) => self.handleWSMsg(self.conns[url], message, res))       
        });
        ws.on('error', (error) => {
          console.error(`${self.url}: Error connecting to ${url}: ${error.message}`);
          rej(error)
        });
      } catch(e){
        rej(e)
      }
    })
  }

  async connectWithBoostrap(){
    if (this.bootstrap.includes(this.url)){
      this.bootIncluded = true;
      this.bootstrap.splice(this.bootstrap.indexOf(this.url), 1);
    } 
    const url = this.bootstrap[this.bootstrap.length * Math.random() << 0];
    try {
      const ws = (await this.connectToNode(url)).ws;
      ws.send(JSON.stringify({event: "Request All Nodes", from: this.url}))
    } catch(e){
      this.connectErrorCount += 1;
      this.bootstrap.splice(this.bootstrap.indexOf(url), 1);
      if(this.bootIncluded){
        this.bootstrap.push(this.url)
        if(this.bootstrap.length == 1){
          console.log("This server is the first bootstrap node that's online")
          if(!this.syncing){
            this.startSync()
          }
          return;
        }
      } 
      if(this.connectErrorCount > 100){
        console.log(this.url + ": Giving up, couldn't connect to the bootstrap nodes, reset your server and try again if you want.")
      } else {
        this.connectWithBoostrap()
      }
    }
  }

  removeNode(url) {
    const index = this.nodes.indexOf(url);
    if (index > -1) {
      console.log(this.url + " removing node " + url)
      delete this.conns[url];
      this.nodes.splice(index, 1);
      this.totalNodes -= 1;
      const disconnectionMessage = JSON.stringify({
        event: 'Node Disconnected',
        url
      });
      Object.values(this.conns).forEach(conn => {
        conn.ws.send(disconnectionMessage);
      });
    }
  }

  startSync(){
    const self = this;
    this.syncing = true;
    self.sync = setInterval(async () => {
      // Tell all the nodes in your group what nodes you have
      const group = [];
      const groupTotal = self.getGroupTotal();
      this.groupId = jumpConsistentHash(self.url, groupTotal);
      for(let i=0;i<self.nodes.length;i++){
        if(self.nodes[i] !== self.url && jumpConsistentHash(self.nodes[i], groupTotal) == this.groupId){
          group.push(self.nodes[i]);
          const ws = (await self.connectToNode(self.nodes[i])).ws;
          ws.send(JSON.stringify({event: "Sync Group Nodes", from: self.url, nodes: this.nodes, totalNodes: this.totalNodes }))
        }
      }

      //Construct map of all groups and their nodes
      const groups = {}
      for(let j=0;j<self.nodes.length;j++){
        const groupIndex = jumpConsistentHash(self.nodes[j], self.getGroupTotal());
        if(self.nodes[j] == self.url) self.groupId = groupIndex;
        if(!groups[groupIndex]) groups[groupIndex] = []
        groups[groupIndex].push(self.nodes[j]);
        groups[groupIndex].sort()
      }
      self.groups = groups

      //REMOVE ALL CONNECTIONS OUTSIDE OF NEARBY GROUPS, AND CHECK ALIVE NODES
      let conns = Object.keys(self.conns);
      for(let i=0;i<conns.length;i++){
        if(  
          !groups[self.groupId]?.includes(conns[i]) &&
          (groups[self.groupId - 1] && !groups[self.groupId - 1]?.includes(conns[i])) &&
          (groups[self.groupId + 1] && !groups[self.groupId + 1]?.includes(conns[i]))
        ){
          self.conns[conns[i]]?.ws?.close()
          delete self.conns[conns[i]];
        }
      }

      const nearbyNodes = [
        ...(self.groups[self.groupId] ? self.groups[self.groupId] : []),
      ]
      if(self.groups[self.groupId - 1]?.length > 0){
        nearbyNodes.push(self.groups[self.groupId - 1][self.groups[self.groupId - 1].length * Math.random << 0])
      }
      if(self.groups[self.groupId + 1]?.length > 0){
        nearbyNodes.push(self.groups[self.groupId + 1][self.groups[self.groupId + 1].length * Math.random << 0])
      }
      for(let i=0;i<nearbyNodes.length;i++){
        await self.connectToNode(nearbyNodes[i])
      }

      for(let i=0;i<self.groups[self.groupId].length;i++){
        const url = self.groups[self.groupId][i];
        if(!self.conns[url]) continue;
        self.conns[url].ws.send(JSON.stringify({
          event: "Alive",
        }))
        if(self.conns[url].alive > 0){
          self.conns[url].alive -= 1;
        } else {
          self.removeNode(url)
        }
      }

      if(self.nodes.length == 1){
        self.stopSync();
        await this.connectWithBoostrap()
      }

    }, 1000)
  }

  stopSync = () => {
    clearInterval(this.sync);
    this.syncing = false;
  }

  getGroupTotal = () => {
    const nodeToGroupRatio = 5; // for every 5 nodes there should be 1 group
    return Math.floor(this.totalNodes / nodeToGroupRatio) + ((this.totalNodes % nodeToGroupRatio == 0) ? 0 : 1)
  }

}

if(isLocal){

  // Local example. Create 100 nodes.
  let nodes = []
  for(let i=0;i<100;i++){
    nodes.push(new Node(defaultPort + i));
  }

  setInterval(() => {

    //Get a random node and log it's state
    const i = nodes.length * Math.random() << 0;
    const node = nodes[i];
    console.log(`${node.url} - Total Nodes: ${node.totalNodes}, Total Groups: ${Object.keys(node.groups).length}`)

  }, 1000)

  setInterval(() => {
    //Remove a random node for simulation purposes
    const i = nodes.length * Math.random() << 0;
    const node = nodes[i];
    console.log(`${node.url} HAS DISCONNECTED`)
    node.wss.clients.forEach(function each(ws) {
      return ws.terminate();
    });
    node.stopSync();
    nodes.splice(i, 1)
  }, 5000)


} else {
  // Production
  const node = new Node(process.env.PORT);
}
