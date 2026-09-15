// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import "net/http"

// serveFlow serves the animated view: a map of the components with a dot
// that travels hop by hop as the demo's stage events arrive.
func serveFlow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(flowPage))
}

const flowPage = `<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>Connection preservation: request flow</title>
<style>
 body{margin:0;font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;background:#0d1117;color:#e6edf3}
 header{padding:14px 24px;border-bottom:1px solid #30363d;display:flex;gap:28px;align-items:center;flex-wrap:wrap}
 .k{color:#8b949e;font-size:12px;text-transform:uppercase;letter-spacing:.06em}
 .v{font-size:17px;font-weight:600}
 .badge{display:inline-block;padding:2px 10px;border-radius:999px;font-size:13px;font-weight:600}
 .RUNNING{background:#1a7f37;color:#fff}.SUSPENDED{background:#6e40c9;color:#fff}.RESUMING,.SUSPENDING{background:#9e6a03;color:#fff}.CRASHED{background:#cf222e;color:#fff}
 .bar{padding:10px 24px;display:flex;gap:10px;flex-wrap:wrap;border-bottom:1px solid #30363d;align-items:center}
 button{background:#21262d;color:#e6edf3;border:1px solid #30363d;border-radius:6px;padding:8px 14px;font-size:14px;cursor:pointer}
 button:hover{background:#30363d} button.primary{background:#1f6feb;border-color:#1f6feb} button.danger{border-color:#cf222e;color:#ff7b72}
 a{color:#58a6ff}
 #map{display:block;width:100%;max-width:1180px;margin:8px auto 0}
 .box{fill:#161b22;stroke:#30363d;stroke-width:1.5;rx:10}
 .box.active{stroke:#58a6ff;stroke-width:2.5}
 .box.off{opacity:.35}
 .inner{fill:#0d1117;stroke:#30363d;rx:6}
 .inner.hold{stroke:#d29922;fill:#2b2111}
 .inner.held{stroke:#f85149;fill:#3d1214}
 .title{fill:#e6edf3;font-size:15px;font-weight:600}
 .sub{fill:#8b949e;font-size:12px}
 .lbl{fill:#c9d1d9;font-size:12.5px}
 .edge{stroke:#30363d;stroke-width:2;fill:none}
 .edge.lit{stroke:#58a6ff}
 .edgelbl{fill:#6e7681;font-size:11px}
 .dot{fill:#58a6ff;stroke:#0d1117;stroke-width:1.5}
 .dot.resp{fill:#3fb950}.dot.ctl{fill:#d2a8ff}
 #caption{text-align:center;color:#c9d1d9;min-height:26px;font-size:15px;padding:6px 24px}
 #log{padding:8px 24px 30px;font:12.5px/1.6 ui-monospace,SFMono-Regular,Menlo,monospace;white-space:pre-wrap;max-height:220px;overflow:auto;border-top:1px solid #30363d}
 .row{display:flex;gap:12px}.t{color:#8b949e}.kind{width:52px;color:#8b949e}
 .sent .kind{color:#58a6ff}.recv .kind{color:#3fb950}.state .kind{color:#d2a8ff}.error .kind{color:#ff7b72}.ws .kind{color:#f0883e}.done .kind{color:#3fb950}.egress .kind{color:#79c0ff}
</style></head><body>
<header>
 <div><div class="k">Actor</div><div class="v" id="actor">...</div></div>
 <div><div class="k">State</div><div class="v"><span class="badge" id="state">...</span></div></div>
 <div><div class="k">Worker</div><div class="v" id="worker">-</div></div>
 <div><div class="k">Anchor gate</div><div class="v" id="gatev">open</div></div>
 <div style="margin-left:auto"><a href="/">log view</a></div>
</header>
<div class="bar">
 <button class="primary" onclick="post('slowhttp')">Slow HTTP request: block in http.Get, suspend, wake on the response</button>
 <button onclick="post('slow')">Slow TCP request</button>
 <button class="danger" onclick="if(confirm('Delete the demo Actor and quit?'))post('quit')">Quit and delete</button>
 <span id="busy" style="color:#8b949e"></span>
</div>
<svg id="map" viewBox="0 0 1180 560" xmlns="http://www.w3.org/2000/svg">
 <defs><marker id="arr" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8 z" fill="#30363d"/></marker></defs>
 <path id="e-a" class="edge" d="M250,120 C320,120 330,215 400,215"/>
 <text class="edgelbl" x="285" y="150">shuttle: raw frames over mTLS</text>
 <path id="e-b" class="edge" d="M250,430 C320,430 330,335 400,335"/>
 <text class="edgelbl" x="285" y="410">shuttle: raw frames over mTLS</text>
 <path id="e-gw" class="edge" d="M700,275 L800,275"/>
 <text class="edgelbl" x="705" y="265">atunnel CONNECT</text>
 <path id="e-echo" class="edge" d="M930,275 L1000,275"/>
 <path id="e-api" class="edge" d="M550,395 L550,460"/>
 <text class="edgelbl" x="560" y="432">ResumeActor</text>
 <path id="e-restore" class="edge" d="M400,490 C330,490 300,470 250,455"/>
 <text class="edgelbl" x="262" y="503">restore snapshot</text>

 <g id="b-a"><rect class="box" x="40" y="60" width="210" height="130"/>
  <text class="title" x="56" y="86">Worker A</text><text class="sub" id="a-pod" x="56" y="104">-</text>
  <rect class="inner" x="56" y="116" width="178" height="60"/>
  <text class="lbl" id="a-l1" x="66" y="140">sandbox</text><text class="lbl" id="a-l2" x="66" y="160">idle</text></g>

 <g id="b-b"><rect class="box" x="40" y="370" width="210" height="130"/>
  <text class="title" x="56" y="396">Worker B</text><text class="sub" id="b-pod" x="56" y="414">-</text>
  <rect class="inner" x="56" y="426" width="178" height="60"/>
  <text class="lbl" id="b-l1" x="66" y="450">no sandbox</text><text class="lbl" id="b-l2" x="66" y="470"></text></g>

 <g id="b-anchor"><rect class="box" x="400" y="150" width="300" height="245"/>
  <text class="title" x="416" y="176">Anchor (ate-system Deployment)</text>
  <rect class="inner" x="416" y="190" width="268" height="52"/>
  <text class="lbl" x="426" y="212">netstack: the Actor's TCP peer</text><text class="lbl" id="ns-l" x="426" y="231">169.254.17.1 to 169.254.17.2</text>
  <rect class="inner" id="gate-box" x="416" y="250" width="268" height="52"/>
  <text class="lbl" x="426" y="272">write gate</text><text class="lbl" id="gate-l" x="426" y="291">open: writes toward the Actor flow</text>
  <rect class="inner" x="416" y="310" width="268" height="52"/>
  <text class="lbl" x="426" y="332">egress relay (atunnel client)</text><text class="lbl" id="relay-l" x="426" y="351">no tunnel</text></g>

 <g id="b-gw"><rect class="box" x="800" y="225" width="130" height="100"/>
  <text class="title" x="816" y="251">Egress</text><text class="title" x="816" y="270">gateway</text><text class="sub" id="gw-l" x="816" y="300">authorizes the Actor</text></g>

 <g id="b-echo"><rect class="box" x="1000" y="225" width="150" height="100"/>
  <text class="title" x="1016" y="251">Echo target</text><text class="sub" id="echo-l1" x="1016" y="275">/delay</text><text class="lbl" id="echo-l2" x="1016" y="300">idle</text></g>

 <g id="b-api"><rect class="box" x="400" y="460" width="300" height="70"/>
  <text class="title" x="416" y="486">ateapi</text><text class="sub" id="api-l" x="416" y="508">control plane</text></g>
</svg>
<div id="caption">Press a button. The dot follows the bytes; the boxes show what each component holds.</div>
<div id="log"></div>
<script>
function post(a){fetch('/api/'+a,{method:'POST'})}
var $=function(id){return document.getElementById(id)};
var P={appA:[145,146],shA:[325,168],ns:[550,216],gate:[550,276],relay:[550,336],gw:[865,275],echo:[1075,275],api:[550,495],appB:[145,456],shB:[325,382]};
var loadedAt=Date.now();
var queue=Promise.resolve();
var workers={};        // pod name -> 'a' | 'b'
var current=null;      // box of the Actor's current worker
var echoDeadline=0, echoTimer=null;
var flowKind='http';

function setText(id,t){$(id).textContent=t}
function setBox(box,on){var g=$('b-'+box);g.querySelector('.box').classList.toggle('off',!on)}
function light(id,on){var e=$(id);if(e)e.classList.toggle('lit',on)}
function gate(mode,text){var b=$('gate-box');b.classList.remove('hold','held');if(mode!=='open')b.classList.add(mode);setText('gate-l',text);setText('gatev',mode==='open'?'open':(mode==='hold'?'holding writes':'holding a reply'))}
function caption(t){setText('caption',t)}
function sleep(ms){return new Promise(function(r){setTimeout(r,ms)})}

function travel(points,msPerHop,cls){
 return new Promise(function(resolve){
  var svg=$('map');var c=document.createElementNS('http://www.w3.org/2000/svg','circle');
  c.setAttribute('r','9');c.setAttribute('class','dot '+(cls||''));svg.appendChild(c);
  var seg=0,start=null;
  function frame(ts){
   if(start===null)start=ts;
   var t=Math.min(1,(ts-start)/msPerHop);
   var a=points[seg],b=points[seg+1];
   c.setAttribute('cx',a[0]+(b[0]-a[0])*t);c.setAttribute('cy',a[1]+(b[1]-a[1])*t);
   if(t>=1){seg++;start=null;if(seg>=points.length-1){svg.removeChild(c);resolve();return}}
   requestAnimationFrame(frame);
  }
  requestAnimationFrame(frame);
 });
}
function enqueue(fn){queue=queue.then(fn).catch(function(){})}

function boxFor(pod){
 if(!pod)return null;
 if(workers[pod])return workers[pod];
 var box=(current==='a')?'b':'a';
 workers[pod]=box;setText(box+'-pod',pod);
 return box;
}
function appPoint(box){return box==='b'?P.appB:P.appA}
function shuttlePoint(box){return box==='b'?P.shB:P.shA}

function startEchoCountdown(seconds){
 echoDeadline=Date.now()+seconds*1000;
 if(echoTimer)clearInterval(echoTimer);
 echoTimer=setInterval(function(){
  var left=Math.ceil((echoDeadline-Date.now())/1000);
  if(left>0){setText('echo-l2','answers in '+left+'s')}else{setText('echo-l2','answering');clearInterval(echoTimer);echoTimer=null}
 },250);
}

function onState(ev){
 var st=$('state');st.textContent=ev.state;st.className='badge '+ev.state;
 setText('worker',ev.worker||'-');
 var box=boxFor(ev.worker);
 if(ev.state==='RUNNING'&&box){
  current=box;setBox(box,true);
  setText(box+'-l1','sandbox running');setText(box+'-l2',flowKind==='http'?'app goroutine ready':'app ready');
 }
 if(ev.state==='SUSPENDING'&&current){setText(current+'-l2','checkpointing...')}
 if(ev.state==='SUSPENDED'&&current){
  setText(current+'-l1','no sandbox');setText(current+'-l2','snapshot saved, worker released');setBox(current,false);
  gate('hold','holding: nothing is written toward the Actor');
 }
 if(ev.state==='RESUMING'&&box){
  setBox(box,true);setText(box+'-l1','restoring the sandbox');setText(box+'-l2','same memory, same sockets');
 }
}

function onStage(ev){
 if(ev.stage==='state'){onState(ev);return}
 if(ev.ts&&ev.ts<loadedAt-2000)return;   // history from before this page opened
 var from=current||'a';
 if(ev.stage==='request-sent'){
  flowKind=/http\.Get/.test(ev.text)?'http':'tcp';
  var m=/(\d+)s/.exec(ev.text);var secs=m?parseInt(m[1],10):30;
  setText('echo-l1',flowKind==='http'?'GET /delay?d='+secs+'s':'delay='+secs+'s line');
  enqueue(function(){
   caption(flowKind==='http'?'The app goroutine calls http.Get. The request leaves the sandbox as raw frames.':'The app writes one line on its TCP connection. The bytes leave the sandbox as raw frames.');
   setText(from+'-l2',flowKind==='http'?'blocked in http.Get':'waiting for the reply line');
   light('e-'+from,true);
   return travel([appPoint(from),shuttlePoint(from),P.ns],650);
  });
  enqueue(function(){caption('The anchor’s netstack is the TCP peer. The relay opens a CONNECT tunnel through the egress gateway.');setText('relay-l','tunnel open to the echo target');light('e-gw',true);light('e-echo',true);return travel([P.ns,P.gate,P.relay,P.gw,P.echo],600)});
  enqueue(function(){caption('The echo target has the request and starts its timer. The call inside the Actor is blocked on the response.');setText('gw-l','tunnel authorized');startEchoCountdown(secs);light('e-'+from,false);return sleep(300)});
 }
 if(ev.stage==='suspending'){
  enqueue(function(){caption('SuspendActor: ateapi tells the anchor to quiesce, then checkpoints the sandbox.');gate('hold','holding: quiesced before the checkpoint');return travel([P.api,P.gate],500,'ctl')});
 }
 if(ev.stage==='suspended'){
  enqueue(function(){caption('Suspended. No sandbox anywhere. The anchor keeps both ends of the connection: the Actor side in its netstack and the tunnel to the echo target.');return sleep(200)});
 }
 if(ev.stage==='woken'){
  var to=boxFor((/after ([^\s,]+)/.exec(ev.text)||[])[1])||current||'b';
  enqueue(function(){caption('The response arrives at the anchor. The relay writes it toward the Actor and the write is held.');setText('echo-l2','answered');return travel([P.echo,P.gw,P.relay,P.gate],600,'resp')});
  enqueue(function(){gate('held','holding one reply for a suspended Actor');caption('First held write: the anchor asks ateapi to resume the Actor (wake on data).');light('e-api',true);return travel([P.gate,P.api],550,'ctl')});
  enqueue(function(){caption('ateapi picks a worker with room and restores the snapshot there.');light('e-restore',true);setBox(to,true);setText(to+'-l1','restoring the sandbox');setText(to+'-l2','same memory, same sockets');return travel([P.api,appPoint(to)],700,'ctl')});
  enqueue(function(){caption('The new worker’s shuttle attaches to the same netstack. The anchor forgets the old MAC and probes readiness.');light('e-'+to,true);current=to;setText(to+'-l1','sandbox running');return travel([appPoint(to),shuttlePoint(to),P.ns],600,'ctl')});
  enqueue(function(){gate('open','open: the held reply is released');caption('The gate opens and the held response goes down the new tunnel into the same socket.');return travel([P.gate,P.ns,shuttlePoint(to),appPoint(to)],600,'resp')});
  enqueue(function(){light('e-api',false);light('e-restore',false);light('e-gw',false);light('e-echo',false);setText(to+'-l2',flowKind==='http'?'http.Get returned':'reply line read');return sleep(100)});
 }
 if(ev.stage==='delivered'){
  enqueue(function(){caption(ev.text);return sleep(100)});
 }
 if(ev.stage==='done'){
  enqueue(function(){caption('Done: '+ev.text);if(current)light('e-'+current,false);return sleep(100)});
 }
}

function status(){fetch('/api/status').then(function(r){return r.json()}).then(function(s){
 setText('actor',s.actor);setText('busy',s.busy?'a step is running...':'');
 if(!$('state').textContent||$('state').textContent==='...'){onState({state:s.state,worker:s.worker})}
}).catch(function(){})}
setInterval(status,1000);status();

var log=$('log');
new EventSource('/events').onmessage=function(e){
 var ev=JSON.parse(e.data);
 var row=document.createElement('div');row.className='row '+ev.kind;
 row.innerHTML='<span class="t">'+ev.at+'</span><span class="kind">'+ev.kind+'</span><span>'+ev.text.replace(/&/g,'&amp;').replace(/</g,'&lt;')+'</span>';
 log.appendChild(row);log.scrollTop=log.scrollHeight;
 if(ev.stage)onStage(ev);
};
</script></body></html>`
