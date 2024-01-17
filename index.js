const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const crypto = require('crypto');
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
    this.bootstrap = isLocal ? ["http://localhost:3000", "http://localhost:3001"] : ["http://10.104.73.83:10000"]
    this.id = uuidv4();
    this.conns = {};
    this.totalNodes = 1;
    this.syncing = false;
    this.port = port; // Store the port for the WebSocket server
    this.url = this.determineServerUrl(port);
    this.nodes = [this.url]
    this.groups = [[this.url]];
    const self = this;
    this.server.listen(port, () => {
      console.log("Valoria Node Running on Port " + port);
      self.wss = new WebSocket.Server({ server: this.server });
      self.wss.on("connection", (ws, req) => {
        const query = querystring.parse(req.url);
        const url = query["/?url"];
        self.conns[url] = {ws, url}
        ws.on('message', (message) => self.handleWSMsg(self.conns[url], message))
      })
      this.connectWithBoostrap()
    });
    this.app.get("/", (req, res) => {
      res.send({
        url: this.url,
        totalNodes: this.totalNodes,
        groups: this.groups
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

  handleWSMsg(conn, message){
    const data = JSON.parse(message);
    const ws = conn.ws;
    const url = conn.url;
    if (data.event === 'ping') {
      ws.send(JSON.stringify({ type: 'pong', id: data.id }));
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
      const groupTotal = this.getGroupTotal();
      const groupId = jumpConsistentHash(this.url, groupTotal);
      // if(groupId == jumpConsistentHash(url, groupTotal)){
        for(let i=0;i<data.nodes.length;i++){
          if(!this.nodes.includes(data.nodes[i])){
            this.nodes.push(data.nodes[i])
            this.totalNodes += 1;
          }
        }
        ws.send(JSON.stringify({ event: 'Receive All Nodes', nodes: this.nodes, total: this.totalNodes, from: this.url }));
      // }
    }
  }

  async connectToNode(url){
    const self = this;
    return new Promise(async (res, rej) => {
      if(self.conns[url]) return res(self.conns[url]);
      const ws = new WebSocket(`${url}?url=${this.url}`);
      ws.on('open', () => {
        console.log(`${self.url} connected to ${url}`);
        self.conns[url] = {ws, url};
        ws.on("message", (message) => self.handleWSMsg(self.conns[url], message))       
        res(self.conns[url]);
      });
      ws.on('error', (error) => {
        console.error(`${this.url}: Error connecting to ${url}: ${error.message}`);
        rej(error)
      });
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
      if(this.bootIncluded) this.bootstrap.push(this.url)
      if(this.connectErrorCount > 100){
        console.log(this.url + ": Giving up, couldn't connect to the bootstrap nodes, reset your server and try again if you want.")
      } else {
        this.connectWithBoostrap()
      }
    }
  }

  startSync(){
    const self = this;
    this.syncing = true;
    self.sync = setInterval(async () => {

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

      const groups = {}
      for(let j=0;j<self.nodes.length;j++){
        const groupIndex = jumpConsistentHash(self.nodes[j], self.getGroupTotal());
        if(!groups[groupIndex]) groups[groupIndex] = []
        groups[groupIndex].push(self.nodes[j]);
        groups[groupIndex].sort()
      }
      self.groups = groups

    }, 1000)
  }

  getGroupTotal = () => {
    const nodesPerGroup = 5;
    return Math.floor(this.totalNodes / nodesPerGroup) + ((this.totalNodes % nodesPerGroup == 0) ? 0 : 1)
  }

}

if(isLocal){
  let nodes = []
  for(let i=0;i<100;i++){
    nodes.push(new Node(defaultPort + i));
  }

  setInterval(() => {
    const i = 10 * Math.random() << 0;
    const node = nodes[i];

    // for(let j=0;j<1000;j++){
    //   const randId = uuidv4();
    //   const groupIndex = jumpConsistentHash(randId, node.getGroupTotal());
    //   if(groupIndex == 0){
    //     console.log("0 INDEX NEEDED");
    //   } else {
    //     // console.log(groupIndex)
    //   }
    // }

    const groups = {}
    for(let j=0;j<node.nodes.length;j++){
      const groupIndex = jumpConsistentHash(node.nodes[j], node.getGroupTotal());
      if(!groups[groupIndex]) groups[groupIndex] = []
      groups[groupIndex].push(node.nodes[j]);
    }
    console.log(groups)
  }, 1000)


} else {
  const node = new Node(process.env.PORT);

  setInterval(() => {
    const i = 10 * Math.random() << 0;

    // for(let j=0;j<1000;j++){
    //   const randId = uuidv4();
    //   const groupIndex = jumpConsistentHash(randId, node.getGroupTotal());
    //   if(groupIndex == 0){
    //     console.log("0 INDEX NEEDED");
    //   } else {
    //     // console.log(groupIndex)
    //   }
    // }

    const groups = {}
    for(let j=0;j<node.nodes.length;j++){
      const groupIndex = jumpConsistentHash(node.nodes[j], node.getGroupTotal());
      if(!groups[groupIndex]) groups[groupIndex] = []
      groups[groupIndex].push(node.nodes[j]);
    }
    console.log(groups)
  }, 1000)

  console.log(node)
}
