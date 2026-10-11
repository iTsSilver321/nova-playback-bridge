export function startWarmWatchdog({url,intervalMs=240000,fetchImpl=fetch}){
  const state={enabled:false,checks:0,lastSuccess:0,failures:0};let stopped=false,timer,active;
  let target;try{target=new URL(url);}catch{return{state,stop(){}};}
  if(target.protocol!=='https:' || target.hostname!=='nova-playback-bridge.onrender.com' || target.username || target.password || target.search || target.hash)return{state,stop(){}};
  target.pathname='/health';state.enabled=true;
  const check=async()=>{if(stopped)return;active=new AbortController();try{const r=await fetchImpl(target,{signal:AbortSignal.any([active.signal,AbortSignal.timeout(15000)]),headers:{'Cache-Control':'no-cache'}});const data=await r.json();if(!r.ok || data.status!=='ok' || data.protocol!==1)throw Error('Not ready');state.checks++;state.lastSuccess=Date.now();state.failures=0;}catch{if(!stopped)state.failures++;}finally{if(!stopped){timer=setTimeout(check,state.failures?30000:intervalMs);timer.unref?.();}}};
  timer=setTimeout(check,5000);timer.unref?.();
  return {state,stop(){stopped=true;clearTimeout(timer);active?.abort();}};
}
