var eS=Object.defineProperty,tS=Object.defineProperties;var nS=Object.getOwnPropertyDescriptors;var Fg=Object.getOwnPropertySymbols;var iS=Object.prototype.hasOwnProperty,rS=Object.prototype.propertyIsEnumerable;var kg=(n,e,t)=>e in n?eS(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,Ve=(n,e)=>{for(var t in e||={})iS.call(e,t)&&kg(n,t,e[t]);if(Fg)for(var t of Fg(e))rS.call(e,t)&&kg(n,t,e[t]);return n},vt=(n,e)=>tS(n,nS(e));var Bo=[null],Ec=[!1],Br=1,sS=null,_n=Symbol("SIGNAL");function we(n){let e=Bo[0];return Bo[0]=n,e}function bc(){return Bo[0]}var bs={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Tc(n){if(Ec[0])throw new Error("");let e=Bo[0];if(e===null)return;e.consumerOnSignalRead(n);let t=e.producersTail;if(t!==void 0&&t.producer===n)return;let i,r=e.recomputing;if(r&&(i=t!==void 0?t.nextProducer:e.producers,i!==void 0&&i.producer===n)){e.producersTail=i,i.lastReadVersion=n.version,i.knownValidAtEpoch=Br;return}let s=n.consumersTail;if(s!==void 0&&s.consumer===e&&(!r||s.knownValidAtEpoch===Br))return;let o=ws(e),a={producer:n,consumer:e,nextProducer:i,prevConsumer:void 0,knownValidAtEpoch:Br,lastReadVersion:n.version,nextConsumer:void 0};e.producersTail=a,t!==void 0?t.nextProducer=a:e.producers=a,o&&Hg(n,a)}function Ug(){Br++}function wc(n){if(!(ws(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===Br)){if(!n.producerMustRecompute(n)&&!Cc(n)){uf(n);return}n.producerRecomputeValue(n),uf(n)}}function df(n){if(n.consumers===void 0)return;let e=Ec[0];Ec[0]=!0;try{for(let t=n.consumers;t!==void 0;t=t.nextConsumer){let i=t.consumer;i.dirty||oS(i)}}finally{Ec[0]=e}}function ff(){return Bo[0]?.consumerAllowSignalWrites!==!1}function oS(n){n.dirty=!0,df(n),n.consumerMarkedDirty?.(n)}function uf(n){n.dirty=!1,n.lastCleanEpoch=Br}function Vo(n){return n&&Bg(n),we(n)}function Bg(n){if(n.producersTail?.knownValidAtEpoch===Br){let e=n.producers;for(;e!==void 0;)e.knownValidAtEpoch=null,e=e.nextProducer}n.producersTail=void 0,n.recomputing=!0}function Ic(n,e){we(e),n&&Vg(n)}function Vg(n){n.recomputing=!1;let e=n.producersTail,t=e!==void 0?e.nextProducer:n.producers;if(t!==void 0){if(ws(n))do t=hf(t);while(t!==void 0);e!==void 0?e.nextProducer=void 0:n.producers=void 0}}function Cc(n){for(let e=n.producers;e!==void 0;e=e.nextProducer){let t=e.producer,i=e.lastReadVersion;if(i!==t.version||(wc(t),i!==t.version))return!0}return!1}function Ts(n){if(ws(n)){let e=n.producers;for(;e!==void 0;)e=hf(e)}n.producers=void 0,n.producersTail=void 0,n.consumers=void 0,n.consumersTail=void 0}function Hg(n,e){let t=n.consumersTail,i=ws(n);if(t!==void 0?(e.nextConsumer=t.nextConsumer,t.nextConsumer=e):(e.nextConsumer=void 0,n.consumers=e),e.prevConsumer=t,n.consumersTail=e,!i)for(let r=n.producers;r!==void 0;r=r.nextProducer)Hg(r.producer,r)}function hf(n){let e=n.producer,t=n.nextProducer,i=n.nextConsumer,r=n.prevConsumer;if(n.nextConsumer=void 0,n.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:e.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(e.consumers=i,!ws(e)){let s=e.producers;for(;s!==void 0;)s=hf(s)}return t}function ws(n){return n.consumerIsAlwaysLive||n.consumers!==void 0}function pf(n){sS?.(n)}function mf(n,e){return Object.is(n,e)}function Ho(n,e){let t=Object.create(aS);t.computation=n,e!==void 0&&(t.equal=e);let i=()=>{if(wc(t),Tc(t),t.value===Es)throw t.error;return t.value};return i[_n]=t,pf(t),i}var cf=Symbol("UNSET"),lf=Symbol("COMPUTING"),Es=Symbol("ERRORED"),aS=vt(Ve({},bs),{value:cf,dirty:!0,error:null,equal:mf,kind:"computed",producerMustRecompute(n){return n.value===cf||n.value===lf},producerRecomputeValue(n){if(n.value===lf)throw new Error("");let e=n.value;n.value=lf;let t=Vo(n),i,r=!1;try{i=n.computation(),we(null),r=e!==cf&&e!==Es&&i!==Es&&n.equal(e,i)}catch(s){i=Es,n.error=s}finally{Ic(n,t)}if(r){n.value=e;return}n.value=i,n.version++}});function cS(){throw new Error}var zg=cS;function Gg(n){zg(n)}function gf(n){zg=n}var lS=null;function yf(n,e){let t=Object.create(vf);t.value=n,e!==void 0&&(t.equal=e);let i=()=>Wg(t);return i[_n]=t,pf(t),[i,o=>Ac(t,o),o=>jg(t,o)]}function Wg(n){return Tc(n),n.value}function Ac(n,e){ff()||Gg(n),n.equal(n.value,e)||(n.value=e,uS(n))}function jg(n,e){ff()||Gg(n),Ac(n,e(n.value))}var vf=vt(Ve({},bs),{equal:mf,value:void 0,kind:"signal"});function uS(n){n.version++,Ug(),df(n),lS?.(n)}var _f=vt(Ve({},bs),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function xf(n){if(n.dirty=!1,n.version>0&&!Cc(n))return;n.version++;let e=Vo(n);try{n.cleanup(),n.fn()}finally{Ic(n,e)}}var Mf=[void 0];function Dc(){return Mf[0]}function mi(n){let e=Mf[0];return Mf[0]=n,e}var $g=Symbol("NotFound");function Is(n){return n===$g||n?.name==="\u0275NotFound"}function xn(n){return typeof n=="function"}function Rc(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var Nc=Rc(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function zo(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var dn=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let s of t)s.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(xn(i))try{i()}catch(s){e=s instanceof Nc?s.errors:[s]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let s of r)try{qg(s)}catch(o){e=e??[],o instanceof Nc?e=[...e,...o.errors]:e.push(o)}}if(e)throw new Nc(e)}}add(e){var t;if(e&&e!==this)if(this.closed)qg(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&zo(t,e)}remove(e){let{_finalizers:t}=this;t&&zo(t,e),e instanceof n&&e._removeParent(this)}};dn.EMPTY=(()=>{let n=new dn;return n.closed=!0,n})();var Sf=dn.EMPTY;function Pc(n){return n instanceof dn||n&&"closed"in n&&xn(n.remove)&&xn(n.add)&&xn(n.unsubscribe)}function qg(n){xn(n)?n():n.unsubscribe()}var Jn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Cs={setTimeout(n,e,...t){let{delegate:i}=Cs;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=Cs;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function Xg(n){Cs.setTimeout(()=>{let{onUnhandledError:e}=Jn;if(e)e(n);else throw n})}function Ef(){}var Yg=bf("C",void 0,void 0);function Zg(n){return bf("E",void 0,n)}function Kg(n){return bf("N",n,void 0)}function bf(n,e,t){return{kind:n,value:e,error:t}}var Vr=null;function As(n){if(Jn.useDeprecatedSynchronousErrorHandling){let e=!Vr;if(e&&(Vr={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=Vr;if(Vr=null,t)throw i}}else n()}function Jg(n){Jn.useDeprecatedSynchronousErrorHandling&&Vr&&(Vr.errorThrown=!0,Vr.error=n)}var Hr=class extends dn{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,Pc(e)&&e.add(this)):this.destination=hS}static create(e,t,i){return new Ds(e,t,i)}next(e){this.isStopped?wf(Kg(e),this):this._next(e)}error(e){this.isStopped?wf(Zg(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?wf(Yg,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},dS=Function.prototype.bind;function Tf(n,e){return dS.call(n,e)}var If=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){Lc(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){Lc(i)}else Lc(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){Lc(t)}}},Ds=class extends Hr{constructor(e,t,i){super();let r;if(xn(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let s;this&&Jn.useDeprecatedNextContext?(s=Object.create(e),s.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&Tf(e.next,s),error:e.error&&Tf(e.error,s),complete:e.complete&&Tf(e.complete,s)}):r=e}this.destination=new If(r)}};function Lc(n){Jn.useDeprecatedSynchronousErrorHandling?Jg(n):Xg(n)}function fS(n){throw n}function wf(n,e){let{onStoppedNotification:t}=Jn;t&&Cs.setTimeout(()=>t(n,e))}var hS={closed:!0,next:Ef,error:fS,complete:Ef};var Qg=typeof Symbol=="function"&&Symbol.observable||"@@observable";function ey(n){return n}function ty(n){return n.length===0?ey:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var gi=class n{constructor(e){e&&(this._subscribe=e)}lift(e){let t=new n;return t.source=this,t.operator=e,t}subscribe(e,t,i){let r=mS(e)?e:new Ds(e,t,i);return As(()=>{let{operator:s,source:o}=this;r.add(s?s.call(r,o):o?this._subscribe(r):this._trySubscribe(r))}),r}_trySubscribe(e){try{return this._subscribe(e)}catch(t){e.error(t)}}forEach(e,t){return t=ny(t),new t((i,r)=>{let s=new Ds({next:o=>{try{e(o)}catch(a){r(a),s.unsubscribe()}},error:r,complete:i});this.subscribe(s)})}_subscribe(e){var t;return(t=this.source)===null||t===void 0?void 0:t.subscribe(e)}[Qg](){return this}pipe(...e){return ty(e)(this)}toPromise(e){return e=ny(e),new e((t,i)=>{let r;this.subscribe(s=>r=s,s=>i(s),()=>t(r))})}};gi.create=n=>new gi(n);function ny(n){var e;return(e=n??Jn.Promise)!==null&&e!==void 0?e:Promise}function pS(n){return n&&xn(n.next)&&xn(n.error)&&xn(n.complete)}function mS(n){return n&&n instanceof Hr||pS(n)&&Pc(n)}function gS(n){return xn(n?.lift)}function iy(n){return e=>{if(gS(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function ry(n,e,t,i,r){return new Cf(n,e,t,i,r)}var Cf=class extends Hr{constructor(e,t,i,r,s,o){super(e),this.onFinalize=s,this.shouldUnsubscribe=o,this._next=t?function(a){try{t(a)}catch(c){e.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){e.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};var sy=Rc(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var Hn=class extends gi{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let t=new Oc(this,this);return t.operator=e,t}_throwIfClosed(){if(this.closed)throw new sy}next(e){As(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let t of this.currentObservers)t.next(e)}})}error(e){As(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:t}=this;for(;t.length;)t.shift().error(e)}})}complete(){As(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:t,isStopped:i,observers:r}=this;return t||i?Sf:(this.currentObservers=null,r.push(e),new dn(()=>{this.currentObservers=null,zo(r,e)}))}_checkFinalizedStatuses(e){let{hasError:t,thrownError:i,isStopped:r}=this;t?e.error(i):r&&e.complete()}asObservable(){let e=new gi;return e.source=this,e}};Hn.create=(n,e)=>new Oc(n,e);var Oc=class extends Hn{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:Sf}};var Go=class extends Hn{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};function Af(n,e){return iy((t,i)=>{let r=0;t.subscribe(ry(i,s=>{i.next(n.call(e,s,r++))}))})}var $e=class extends Error{code;constructor(e,t){super(Wf(e,t)),this.code=e}};function yS(n){return`NG0${Math.abs(n)}`}function Wf(n,e){return`${yS(n)}${e?": "+e:""}`}function ft(n){for(let e in n)if(n[e]===ft)return e;throw Error("")}function Gc(n){if(typeof n=="string")return n;if(Array.isArray(n))return`[${n.map(Gc).join(", ")}]`;if(n==null)return""+n;let e=n.overriddenName||n.name;if(e)return`${e}`;let t=n.toString();if(t==null)return""+t;let i=t.indexOf(`
`);return i>=0?t.slice(0,i):t}function jf(n,e){return n?e?`${n} ${e}`:n:e||""}var vS=ft({__forward_ref__:ft});function Wc(n){return n.__forward_ref__=Wc,n}function wn(n){return uy(n)?n():n}function uy(n){return typeof n=="function"&&Object.hasOwn(n,vS)&&n.__forward_ref__===Wc}function jt(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function jc(n){return _S(n,$c)}function _S(n,e){return Object.hasOwn(n,e)&&n[e]||null}function xS(n){let e=n?.[$c]??null;return e||null}function Rf(n){return n&&Object.hasOwn(n,kc)?n[kc]:null}var $c=ft({\u0275prov:ft}),kc=ft({\u0275inj:ft}),et=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(e,t){this._desc=e,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=jt({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function $f(n){return n&&!!n.\u0275providers}var qf=ft({\u0275cmp:ft}),Xf=ft({\u0275dir:ft}),Yf=ft({\u0275pipe:ft});var Nf=ft({\u0275fac:ft}),$r=ft({__NG_ELEMENT_ID__:ft}),oy=ft({__NG_ENV_ID__:ft});function qr(n){return Zf(n,"@Component"),n[qf]||null}function qc(n){return Zf(n,"@Directive"),n[Xf]||null}function dy(n){return Zf(n,"@Pipe"),n[Yf]||null}function Zf(n,e){if(n==null)throw new $e(-919,!1)}function Kf(n){return typeof n=="string"?n:n==null?"":String(n)}var fy=ft({ngErrorCode:ft}),MS=ft({ngErrorMessage:ft}),SS=ft({ngTokenPath:ft});function Jf(n,e){return hy("",-200,e)}function Xc(n,e){throw new $e(-201,!1)}function hy(n,e,t){let i=new $e(e,n);return i[fy]=e,i[MS]=n,t&&(i[SS]=t),i}function ES(n){return n[fy]}var Pf;function py(){return Pf}function Tn(n){let e=Pf;return Pf=n,e}function Qf(n,e,t){let i=jc(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&8)return null;if(e!==void 0)return e;Xc(n,"")}var bS={},zr=bS,TS="__NG_DI_FLAG__",Lf=class{injector;constructor(e){this.injector=e}retrieve(e,t){let i=Gr(t)||0;try{return this.injector.get(e,i&8?null:zr,i)}catch(r){if(Is(r))return r;throw r}}};function wS(n,e=0){let t=Dc();if(t===void 0)throw new $e(-203,!1);if(t===null)return Qf(n,void 0,e);{let i=IS(e),r=t.retrieve(n,i);if(Is(r)){if(i.optional)return null;throw r}return r}}function lt(n,e=0){return(py()||wS)(wn(n),e)}function Ze(n,e){return lt(n,Gr(e))}function Gr(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function IS(n){return{optional:!!(n&8),host:!!(n&1),self:!!(n&2),skipSelf:!!(n&4)}}function Of(n){let e=[];for(let t=0;t<n.length;t++){let i=wn(n[t]);if(Array.isArray(i)){if(i.length===0)throw new $e(900,!1);let r,s=0;for(let o=0;o<i.length;o++){let a=i[o],c=CS(a);typeof c=="number"?c===-1?r=a.token:s|=c:r=a}e.push(lt(r,s))}else e.push(lt(i))}return e}function CS(n){return n[TS]}function Ns(n,e){let t=Object.hasOwn(n,Nf);return t?n[Nf]:null}function my(n,e,t){if(n.length!==e.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(t&&(r=t(r),s=t(s)),s!==r)return!1}return!0}function gy(n){return n.flat(Number.POSITIVE_INFINITY)}function Yc(n,e){n.forEach(t=>Array.isArray(t)?Yc(t,e):e(t))}function eh(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function Xo(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}function yy(n,e,t,i){let r=n.length;if(r==e)n.push(t,i);else if(r===1)n.push(i,n[0]),n[0]=t;else{for(r--,n.push(n[r-1],n[r]);r>e;){let s=r-2;n[r]=n[s],r--}n[e]=t,n[e+1]=i}}function vy(n,e,t){let i=Ls(n,e);return i>=0?n[i|1]=t:(i=~i,yy(n,i,e,t)),i}function Zc(n,e){let t=Ls(n,e);if(t>=0)return n[t|1]}function Ls(n,e){return AS(n,e,1)}function AS(n,e,t){let i=0,r=n.length>>t;for(;r!==i;){let s=i+(r-i>>1),o=n[s<<t];if(e===o)return s<<t;o>e?r=s:i=s+1}return~(r<<t)}var Kc={},Ui=[],Os=new et(""),th=new et("",-1),nh=new et(""),Ps=class{get(e,t=zr){if(t===zr){let r=hy("",-201);throw r.name="\u0275NotFound",r}return t}};function Yo(n){return{\u0275providers:n}}function _y(n){return Yo([{provide:Os,multi:!0,useValue:n}])}function xy(...n){return{\u0275providers:ih(!0,n),\u0275fromNgModule:!0}}function ih(n,...e){let t=[],i=new Set,r,s=o=>{t.push(o)};return Yc(e,o=>{let a=o;Uc(a,s,[],i)&&(r||=[],r.push(a))}),r!==void 0&&My(r,s),t}function My(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];rh(r,s=>{e(s,i)})}}function Uc(n,e,t,i){if(n=wn(n),!n)return!1;let r=null,s=Rf(n),o=!s&&qr(n);if(!s&&!o){let c=n.ngModule;if(s=Rf(c),s)r=c;else return!1}else{if(o&&!o.standalone)return!1;r=n}let a=i.has(r);if(o){if(a)return!1;if(i.add(r),o.dependencies){let c=typeof o.dependencies=="function"?o.dependencies():o.dependencies;for(let l of c)Uc(l,e,t,i)}}else if(s){if(s.imports!=null&&!a){i.add(r);let l;Yc(s.imports,u=>{Uc(u,e,t,i)&&(l||=[],l.push(u))}),l!==void 0&&My(l,e)}if(!a){let l=Ns(r)||(()=>new r);e({provide:r,useFactory:l,deps:Ui},r),e({provide:nh,useValue:r,multi:!0},r),e({provide:Os,useValue:()=>lt(r),multi:!0},r)}let c=s.providers;if(c!=null&&!a){let l=n;rh(c,u=>{e(u,l)})}}else return!1;return r!==n&&n.providers!==void 0}function rh(n,e){for(let t of n)$f(t)&&(t=t.\u0275providers),Array.isArray(t)?rh(t,e):e(t)}var DS=ft({provide:String,useValue:ft});function Sy(n){return n!==null&&typeof n=="object"&&DS in n}function RS(n){return!!(n&&n.useExisting)}function NS(n){return!!(n&&n.useFactory)}function Bc(n){return typeof n=="function"}var Zo=new et(""),Fc={},ay={},Df;function Ko(){return Df===void 0&&(Df=new Ps),Df}var Qn=class{},Wr=class extends Qn{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,kf(e,o=>this.processProvider(o)),this.records.set(th,Rs(void 0,this)),r.has("environment")&&this.records.set(Qn,Rs(void 0,this));let s=this.records.get(Zo);s!=null&&typeof s.value=="string"&&this.scopes.add(s.value),this.injectorDefTypes=new Set(this.get(nh,Ui,{self:!0}))}retrieve(e,t){let i=Gr(t)||0;try{return this.get(e,zr,i)}catch(r){if(Is(r))return r;throw r}}destroy(){Wo(this),this._destroyed=!0;let e=we(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),we(e)}}onDestroy(e){return Wo(this),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){Wo(this);let t=mi(this),i=Tn(void 0),r;try{return e()}finally{mi(t),Tn(i)}}get(e,t=zr,i){if(Wo(this),Object.hasOwn(e,oy))return e[oy](this);let r=Gr(i),s,o=mi(this),a=Tn(void 0);try{if(!(r&4)){let l=this.records.get(e);if(l===void 0){let u=kS(e)&&jc(e);u&&this.injectableDefInScope(u)?l=Rs(Ff(e),Fc):l=null,this.records.set(e,l)}if(l!=null)return this.hydrate(e,l,r)}let c=r&2?Ko():this.parent;return t=r&8&&t===zr?null:t,c.get(e,t)}catch(c){let l=ES(c);throw l===-200||l===-201?new $e(l,null):c}finally{Tn(a),mi(o)}}resolveInjectorInitializers(){let e=we(null),t=mi(this),i=Tn(void 0),r;try{let s=this.get(Os,Ui,{self:!0});for(let o of s)o()}finally{mi(t),Tn(i),we(e)}}toString(){return"R3Injector[...]"}processProvider(e){e=wn(e);let t=Bc(e)?e:wn(e&&e.provide),i=LS(e);if(!Bc(e)&&e.multi===!0){let r=this.records.get(t);r||(r=Rs(void 0,Fc,!0),r.factory=()=>Of(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t,i){let r=we(null);try{if(t.value===ay)throw Jf("");return t.value===Fc&&(t.value=ay,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&FS(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{we(r)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=wn(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function Ff(n){let e=jc(n),t=e!==null?e.factory:Ns(n);if(t!==null)return t;if(n instanceof et)throw new $e(-204,!1);if(n instanceof Function)return PS(n);throw new $e(-204,!1)}function PS(n){if(n.length>0)throw new $e(-204,!1);let t=xS(n);return t!==null?()=>t.factory(n):()=>new n}function LS(n){if(Sy(n))return Rs(void 0,n.useValue);{let e=Ey(n);return Rs(e,Fc)}}function Ey(n,e,t){let i;if(Bc(n)){let r=wn(n);return Ns(r)||Ff(r)}else if(Sy(n))i=()=>wn(n.useValue);else if(NS(n))i=()=>n.useFactory(...Of(n.deps||[]));else if(RS(n))i=(r,s)=>lt(wn(n.useExisting),s!==void 0&&s&8?8:void 0);else{let r=wn(n&&(n.useClass||n.provide));if(OS(n))i=()=>new r(...Of(n.deps));else return Ns(r)||Ff(r)}return i}function Wo(n){if(n.destroyed)throw new $e(-205,!1)}function Rs(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function OS(n){return!!n.deps}function FS(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function kS(n){return typeof n=="function"||typeof n=="object"&&n.ngMetadataName==="InjectionToken"}function kf(n,e){for(let t of n)Array.isArray(t)?kf(t,e):t&&$f(t)?kf(t.\u0275providers,e):e(t)}function Jc(n,e){let t;n instanceof Wr?(Wo(n),t=n):t=new Lf(n);let i,r=mi(t),s=Tn(void 0);try{return e()}finally{mi(r),Tn(s)}}function by(){return py()!==void 0||Dc()!=null}var ei=0,De=1,Pe=2,At=3,zn=4,Mn=5,Fs=6,ks=7,It=8,zi=9,vi=10,Pt=11,Us=12,sh=13,lr=14,Cn=15,ur=16,Xr=17,_i=18,Jo=19,oh=20,ki=21,Qc=22,Bi=23,An=24,el=25,Ty=26,Qo=27,$t=28,wy=1,ah=6,Yr=7,ea=8,Zr=9,Ct=10;function dr(n){return Array.isArray(n)&&typeof n[wy]=="object"}function Gn(n){return Array.isArray(n)&&n[wy]===!0}function ch(n){return(n.flags&4)!==0}function fr(n){return n.componentOffset>-1}function tl(n){return(n.flags&1)===1}function Kr(n){return!!n.template}function Bs(n){return(n[Pe]&512)!==0}function Jr(n){return(n[Pe]&256)===256}var lh="svg",Iy="math";function Kt(n){for(;Array.isArray(n);)n=n[ei];return n}function uh(n,e){return Kt(e[n])}function ti(n,e){return Kt(e[n.index])}function nl(n,e){return n.data[e]}function Cy(n,e){return n[e]}function xi(n,e){let t=e[n];return dr(t)?t:t[ei]}function il(n){return(n[Pe]&128)===128}function Ay(n){return Gn(n[At])}function Mi(n,e){return e==null?null:n[e]}function dh(n){n[Xr]=0}function fh(n){n[Pe]&1024||(n[Pe]|=1024,il(n)&&Vs(n))}function Dy(n,e){for(;n>0;)e=e[lr],n--;return e}function ta(n){return!!(n[Pe]&9216||n[An]?.dirty)}function rl(n){n[vi].changeDetectionScheduler?.notify(8),n[Pe]&64&&(n[Pe]|=1024),ta(n)&&Vs(n)}function Vs(n){n[vi].changeDetectionScheduler?.notify(0);let e=Vi(n);for(;e!==null&&!(e[Pe]&8192||(e[Pe]|=8192,!il(e)));)e=Vi(e)}function sl(n,e){if(Jr(n))throw new $e(911,!1);n[ki]===null&&(n[ki]=[]),n[ki].push(e)}function Ry(n,e){if(n[ki]===null)return;let t=n[ki].indexOf(e);t!==-1&&n[ki].splice(t,1)}function Vi(n){let e=n[At];return Gn(e)?e[At]:e}function hh(n){return n[ks]??=[]}function ph(n){return n.cleanup??=[]}function Ny(n,e,t,i){let r=hh(e);r.push(t),n.firstCreatePass&&ph(n).push(i,r.length-1)}var Ge={lFrame:Xy(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Uf=!1;function Py(){return Ge.lFrame.elementDepthCount}function Ly(){Ge.lFrame.elementDepthCount++}function mh(){Ge.lFrame.elementDepthCount--}function Oy(){return Ge.bindingsEnabled}function Fy(){return Ge.skipHydrationRootTNode!==null}function gh(n){return Ge.skipHydrationRootTNode===n}function yh(){Ge.skipHydrationRootTNode=null}function ut(){return Ge.lFrame.lView}function Wn(){return Ge.lFrame.tView}function Lt(n){return Ge.lFrame.contextLView=n,n[It]}function Ot(n){return Ge.lFrame.contextLView=null,n}function ni(){let n=vh();for(;n!==null&&n.type===64;)n=n.parent;return n}function vh(){return Ge.lFrame.currentTNode}function ky(){let n=Ge.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function Hs(n,e){let t=Ge.lFrame;t.currentTNode=n,t.isParent=e}function _h(){return Ge.lFrame.isParent}function Uy(){Ge.lFrame.isParent=!1}function By(){return Ge.lFrame.contextLView}function xh(){return Uf}function jo(n){let e=Uf;return Uf=n,e}function Vy(n){return Ge.lFrame.bindingIndex=n}function zs(){return Ge.lFrame.bindingIndex++}function Hy(n){let e=Ge.lFrame,t=e.bindingIndex;return e.bindingIndex=e.bindingIndex+n,t}function zy(){return Ge.lFrame.inI18n}function Gy(n,e){let t=Ge.lFrame;t.bindingIndex=t.bindingRootIndex=n,ol(e)}function Wy(){return Ge.lFrame.currentDirectiveIndex}function ol(n){Ge.lFrame.currentDirectiveIndex=n}function jy(n){let e=Ge.lFrame.currentDirectiveIndex;return e===-1?null:n[e]}function $y(){return Ge.lFrame.currentQueryIndex}function al(n){Ge.lFrame.currentQueryIndex=n}function US(n){let e=n[De];return e.type===2?e.declTNode:e.type===1?n[Mn]:null}function Mh(n,e,t){if(t&4){let r=e,s=n;for(;r=r.parent,r===null&&!(t&1);)if(r=US(s),r===null||(s=s[lr],r.type&10))break;if(r===null)return!1;e=r,n=s}let i=Ge.lFrame=qy();return i.currentTNode=e,i.lView=n,!0}function cl(n){let e=qy(),t=n[De];Ge.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function qy(){let n=Ge.lFrame,e=n===null?null:n.child;return e===null?Xy(n):e}function Xy(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function Yy(){let n=Ge.lFrame;return Ge.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var Sh=Yy;function ll(){let n=Yy();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function Zy(n){return(Ge.lFrame.contextLView=Dy(n,Ge.lFrame.contextLView))[It]}function hr(){return Ge.lFrame.selectedIndex}function pr(n){Ge.lFrame.selectedIndex=n}function Eh(){let n=Ge.lFrame;return nl(n.tView,n.selectedIndex)}function na(){Ge.lFrame.currentNamespace=lh}function ul(){BS()}function BS(){Ge.lFrame.currentNamespace=null}function bh(){return Ge.lFrame.currentNamespace}var Ky=!0;function dl(){return Ky}function fl(n){Ky=n}function Bf(n,e=null,t=null,i){let r=Jy(n,e,t,i);return r.resolveInjectorInitializers(),r}function Jy(n,e=null,t=null,i,r=new Set){let s=[t||Ui,xy(n)],o;return new Wr(s,e||Ko(),o||null,r)}var yi=class n{static THROW_IF_NOT_FOUND=zr;static NULL=new Ps;static create(e,t){if(Array.isArray(e))return Bf({name:""},t,e,"");{let i=e.name??"";return Bf({name:i},e.parent,e.providers,i)}}static \u0275prov=jt({token:n,providedIn:"any",factory:()=>lt(th)});static __NG_ELEMENT_ID__=-1},Dn=new et(""),cr=class{static __NG_ELEMENT_ID__=VS;static __NG_ENV_ID__=e=>e},Vc=class extends cr{_lView;constructor(e){super(),this._lView=e}get destroyed(){return Jr(this._lView)}onDestroy(e){let t=this._lView;return sl(t,e),()=>Ry(t,e)}};function VS(){return new Vc(ut())}var Qy=!1,ev=new et(""),Gs=(()=>{class n{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Go(!1);debugTaskTracker=Ze(ev,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new gi(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=jt({token:n,providedIn:"root",factory:()=>new n})}return n})(),Vf=class extends Hn{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(e=!1){super(),this.__isAsync=e,by()&&(this.destroyRef=Ze(cr,{optional:!0})??void 0,this.pendingTasks=Ze(Gs,{optional:!0})??void 0)}emit(e){let t=we(null);try{super.next(e)}finally{we(t)}}subscribe(e,t,i){let r=e,s=t||(()=>null),o=i;if(e&&typeof e=="object"){let c=e;r=c.next?.bind(c),s=c.error?.bind(c),o=c.complete?.bind(c)}this.__isAsync&&(s=this.wrapInTimeout(s),r&&(r=this.wrapInTimeout(r)),o&&(o=this.wrapInTimeout(o)));let a=super.subscribe({next:r,error:s,complete:o});return e instanceof dn&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{e(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},Fi=Vf;function Hc(...n){}function Th(n){let e,t;function i(){n=Hc;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch{}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function tv(n){return queueMicrotask(()=>n()),()=>{n=Hc}}var wh="isAngularZone",$o=wh+"_ID",HS=0,In=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new Fi(!1);onMicrotaskEmpty=new Fi(!1);onStable=new Fi(!1);onError=new Fi(!1);constructor(e){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:s=Qy}=e;if(typeof Zone>"u")throw new $e(908,!1);Zone.assertZonePatched();let o=this;o._nesting=0,o._outer=o._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(o._inner=o._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(o._inner=o._inner.fork(Zone.longStackTraceZoneSpec)),o.shouldCoalesceEventChangeDetection=!r&&i,o.shouldCoalesceRunChangeDetection=r,o.callbackScheduled=!1,o.scheduleInRootZone=s,WS(o)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(wh)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new $e(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new $e(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let s=this._inner,o=s.scheduleEventTask("NgZoneEvent: "+r,e,zS,Hc,Hc);try{return s.runTask(o,t,i)}finally{s.cancelTask(o)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},zS={};function Ih(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function GS(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){Th(()=>{n.callbackScheduled=!1,Hf(n),n.isCheckStableRunning=!0,Ih(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),Hf(n)}function WS(n){let e=()=>{GS(n)},t=HS++;n._inner=n._inner.fork({name:"angular",properties:{[wh]:!0,[$o]:t,[$o+t]:!0},onInvokeTask:(i,r,s,o,a,c)=>{if(jS(c))return i.invokeTask(s,o,a,c);try{return cy(n),i.invokeTask(s,o,a,c)}finally{(n.shouldCoalesceEventChangeDetection&&o.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),ly(n)}},onInvoke:(i,r,s,o,a,c,l)=>{try{return cy(n),i.invoke(s,o,a,c,l)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!$S(c)&&e(),ly(n)}},onHasTask:(i,r,s,o)=>{i.hasTask(s,o),r===s&&(o.change=="microTask"?(n._hasPendingMicrotasks=o.microTask,Hf(n),Ih(n)):o.change=="macroTask"&&(n.hasPendingMacrotasks=o.macroTask))},onHandleError:(i,r,s,o)=>(i.handleError(s,o),n.runOutsideAngular(()=>n.onError.emit(o)),!1)})}function Hf(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function cy(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function ly(n){n._nesting--,Ih(n)}var qo=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new Fi;onMicrotaskEmpty=new Fi;onStable=new Fi;onError=new Fi;run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function jS(n){return nv(n,"__ignore_ng_zone__")}function $S(n){return nv(n,"__scheduler_tick__")}function nv(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var Hi=class{_console=console;handleError(e){this._console.error("ERROR",e)}};function iv(n){return qS(n)?n:new zf(n)}function qS(n){return n instanceof Error||typeof n=="object"&&n!==null&&typeof n.name=="string"&&typeof n.message=="string"}var zf=class extends Error{constructor(e){super(String(e),{cause:e}),this.name="ErrorBoundaryWrappedError"}},Qr=new et("",{factory:()=>{let n=Ze(In),e=Ze(Qn),t;return i=>{n.runOutsideAngular(()=>{e.destroyed&&!t?setTimeout(()=>{throw i}):(t??=e.get(Hi),t.handleError(i))})}}}),rv={provide:Os,useValue:()=>{let n=Ze(Hi,{optional:!0})},multi:!0},XS=new et("",{factory:()=>{let n=Ze(Dn).defaultView;if(!n)return;let e=Ze(Qr),t=s=>{e(s.reason),s.preventDefault()},i=s=>{s.error?e(s.error):e(new Error(s.message,{cause:s})),s.preventDefault()},r=()=>{n.addEventListener("unhandledrejection",t),n.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),Ze(cr).onDestroy(()=>{n.removeEventListener("error",i),n.removeEventListener("unhandledrejection",t)})}});function Ch(){return Yo([_y(()=>{Ze(XS)})])}function Jt(n,e){let[t,i,r]=yf(n,e?.equal),s=t,o=s[_n];return s.set=i,s.update=r,s.asReadonly=sv.bind(s),s}function sv(){let n=this[_n];if(n.readonlyFn===void 0){let e=()=>this();e[_n]=n,n.readonlyFn=e}return n.readonlyFn}var ia=new et("",{factory:()=>YS}),YS="ng";var hl=new et(""),ra=new et("",{providedIn:"platform",factory:()=>"unknown"});var pl=new et("",{factory:()=>Ze(Dn).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var ml=(()=>{class n{view;node;constructor(t,i){this.view=t,this.node=i}static __NG_ELEMENT_ID__=ZS}return n})();function ZS(){return new ml(ut(),ni())}var jr=class{},sa=new et("",{factory:()=>!0});var Ah=new et(""),gl=(()=>{class n{static \u0275prov=jt({token:n,providedIn:"root",factory:()=>new Gf})}return n})(),Gf=class{dirtyEffectCount=0;queues=new Map;add(e){this.enqueue(e),this.schedule(e)}schedule(e){e.dirty&&this.dirtyEffectCount++}remove(e){let t=e.zone,i=this.queues.get(t);i.has(e)&&(i.delete(e),e.dirty&&this.dirtyEffectCount--)}enqueue(e){let t=e.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(e)||i.add(e)}flush(){for(;this.dirtyEffectCount>0;){let e=!1;for(let[t,i]of this.queues)t===null?e||=this.flushQueue(i):e||=t.run(()=>this.flushQueue(i));e||(this.dirtyEffectCount=0)}}flushQueue(e){let t=!1;for(let i of e)i.dirty&&(this.dirtyEffectCount--,t=!0,i.run());return t}},zc=class{[_n];constructor(e){this[_n]=e}destroy(){this[_n].destroy()}};function es(n,e){let t=e?.injector??Ze(yi),i=e?.manualCleanup!==!0?t.get(cr):null,r,s=t.get(ml,null,{optional:!0}),o=t.get(jr);return s!==null?(r=av(s.view,o,n),i instanceof Vc&&i._lView===s.view&&(i=null)):r=QS(n,t.get(gl),o),r.injector=t,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new zc(r)}var ov=vt(Ve({},_f),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let n=jo(!1);try{xf(this)}finally{jo(n)}},cleanup(){if(!this.cleanupFns?.length)return;let n=we(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],we(n)}}}),KS=vt(Ve({},ov),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Ts(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.scheduler.remove(this)}}),JS=vt(Ve({},ov),{consumerMarkedDirty(){this.view[Pe]|=8192,Vs(this.view),this.notifier.notify(13)},destroy(){if(Ts(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.view[Bi]?.delete(this)}});function av(n,e,t){let i=Object.create(JS);return i.view=n,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=e,i.userFn=t,i.fn=cv(i,t),n[Bi]??=new Set,n[Bi].add(i),i.consumerMarkedDirty(i),i}function QS(n,e,t){let i=Object.create(KS);return i.userFn=n,i.fn=cv(i,n),i.scheduler=e,i.notifier=t,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function cv(n,e){return()=>{e(t=>(n.cleanupFns??=[]).push(t))}}function Bv(n){return{toString:n}.toString()}var it=(function(n){return n[n.TemplateCreateStart=0]="TemplateCreateStart",n[n.TemplateCreateEnd=1]="TemplateCreateEnd",n[n.TemplateUpdateStart=2]="TemplateUpdateStart",n[n.TemplateUpdateEnd=3]="TemplateUpdateEnd",n[n.LifecycleHookStart=4]="LifecycleHookStart",n[n.LifecycleHookEnd=5]="LifecycleHookEnd",n[n.OutputStart=6]="OutputStart",n[n.OutputEnd=7]="OutputEnd",n[n.BootstrapApplicationStart=8]="BootstrapApplicationStart",n[n.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",n[n.BootstrapComponentStart=10]="BootstrapComponentStart",n[n.BootstrapComponentEnd=11]="BootstrapComponentEnd",n[n.ChangeDetectionStart=12]="ChangeDetectionStart",n[n.ChangeDetectionEnd=13]="ChangeDetectionEnd",n[n.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",n[n.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",n[n.AfterRenderHooksStart=16]="AfterRenderHooksStart",n[n.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",n[n.ComponentStart=18]="ComponentStart",n[n.ComponentEnd=19]="ComponentEnd",n[n.DeferBlockStateStart=20]="DeferBlockStateStart",n[n.DeferBlockStateEnd=21]="DeferBlockStateEnd",n[n.DynamicComponentStart=22]="DynamicComponentStart",n[n.DynamicComponentEnd=23]="DynamicComponentEnd",n[n.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",n[n.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",n})(it||{});function vE(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}var _E=null;function xE(){return _E}var lv=[];var ht=function(n,e=null,t){for(let i=0;i<lv.length;i++){let r=lv[i];r(n,e,t)}};function ME(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:s}=e.type.prototype;if(i){let o=xE()(e);(t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o)}r&&(t.preOrderHooks??=[]).push(0-n,r),s&&((t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s))}function SE(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let s=n.data[t].type.prototype,{ngAfterContentInit:o,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:u}=s;o&&(n.contentHooks??=[]).push(-t,o),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),c&&(n.viewHooks??=[]).push(-t,c),l&&((n.viewHooks??=[]).push(t,l),(n.viewCheckHooks??=[]).push(t,l)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function vl(n,e,t){Vv(n,e,3,t)}function _l(n,e,t,i){(n[Pe]&3)===t&&Vv(n,e,t,i)}function Dh(n,e){let t=n[Pe];(t&3)===e&&(t&=16383,t+=1,n[Pe]=t)}function Vv(n,e,t,i){let r=i!==void 0?n[Xr]&65535:0,s=i??-1,o=e.length-1,a=0;for(let c=r;c<o;c++)if(typeof e[c+1]=="number"){if(a=e[c],i!=null&&a>=i)break}else e[c]<0&&(n[Xr]+=65536),(a<s||s==-1)&&(EE(n,t,e,c),n[Xr]=(n[Xr]&4294901760)+c+2),c++}function uv(n,e){ht(it.LifecycleHookStart,n,e);let t=we(null);try{e.call(n)}finally{we(t),ht(it.LifecycleHookEnd,n,e)}}function EE(n,e,t,i){let r=t[i]<0,s=t[i+1],o=r?-t[i]:t[i],a=n[o];r?n[Pe]>>14<n[Xr]>>16&&(n[Pe]&3)===e&&(n[Pe]+=16384,uv(a,s)):uv(a,s)}var $s=-1,ca=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(e,t,i,r){this.factory=e,this.name=r,this.canSeeViewProviders=t,this.injectImpl=i}};function bE(n){return(n.flags&8)!==0}function TE(n){return(n.flags&16)!==0}function wE(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let s=t[i++],o=t[i++],a=t[i++];n.setAttribute(e,o,a,s)}else{let s=r,o=t[++i];CE(s)?n.setProperty(e,s,o):n.setAttribute(e,s,o),i++}}return i}function IE(n){return n===3||n===4||n===6}function CE(n){return n.charCodeAt(0)===64}function Ol(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?dv(n,t,r,null,e[++i]):dv(n,t,r,null,null))}}return n}function dv(n,e,t,i,r){let s=0,o=n.length;if(e===-1)o=-1;else for(;s<n.length;){let a=n[s++];if(typeof a=="number"){if(a===e){o=-1;break}else if(a>e){o=s-1;break}}}for(;s<n.length;){let a=n[s];if(typeof a=="number")break;if(a===t){r!==null&&(n[s+1]=r);return}s++,r!==null&&s++}o!==-1&&(n.splice(o,0,e),s=o+1),n.splice(s++,0,t),r!==null&&n.splice(s++,0,r)}function Hv(n){return n!==$s}function Sl(n){return n&32767}function AE(n){return n>>16}function El(n,e){let t=AE(n),i=e;for(;t>0;)i=i[lr],t--;return i}var Uh=!0;function fv(n){let e=Uh;return Uh=n,e}var DE=256,zv=DE-1,Gv=5,RE=0,Si={};function NE(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:Object.hasOwn(t,$r)&&(i=t[$r]),i==null&&(i=t[$r]=RE++);let r=i&zv,s=1<<r;e.data[n+(r>>Gv)]|=s}function Wv(n,e){let t=jv(n,e);if(t!==-1)return t;let i=e[De];i.firstCreatePass&&(n.injectorIndex=e.length,Rh(i.data,n),Rh(e,null),Rh(i.blueprint,null));let r=dp(n,e),s=n.injectorIndex;if(Hv(r)){let o=Sl(r),a=El(r,e),c=a[De].data;for(let l=0;l<8;l++)e[s+l]=a[o+l]|c[o+l]}return e[s+8]=r,s}function Rh(n,e){n.push(0,0,0,0,0,0,0,0,e)}function jv(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function dp(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=Zv(r),i===null)return $s;if(t++,r=r[lr],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return $s}function PE(n,e,t){NE(n,e,t)}function $v(n,e,t){if(t&8||n!==void 0)return n;Xc(e,"NodeInjector")}function qv(n,e,t,i){if(t&8&&i===void 0&&(i=null),(t&3)===0){let r=n[zi],s=Tn(void 0);try{return r?r.get(e,i,t&8):Qf(e,i,t&8)}finally{Tn(s)}}return $v(i,e,t)}function Xv(n,e,t,i=0,r){if(n!==null){if(e[Pe]&2048&&!(i&2)){let o=kE(n,e,t,i,Si);if(o!==Si)return o}let s=Yv(n,e,t,i,Si);if(s!==Si)return s}return qv(e,t,i,r)}function Yv(n,e,t,i,r){let s=OE(t);if(typeof s=="function"){if(!Mh(e,n,i))return i&1?$v(r,t,i):qv(e,t,i,r);try{let o;if(o=s(i),o==null&&!(i&8))Xc(t);else return o}finally{Sh()}}else if(typeof s=="number"){let o=null,a=jv(n,e),c=$s,l=i&1?e[Cn][Mn]:null;for((a===-1||i&4)&&(c=a===-1?dp(n,e):e[a+8],c===$s||!pv(i,!1)?a=-1:(o=e[De],a=Sl(c),e=El(c,e)));a!==-1;){let u=e[De];if(hv(s,a,u.data)){let d=LE(a,e,t,o,i,l);if(d!==Si)return d}c=e[a+8],c!==$s&&pv(i,e[De].data[a+8]===l)&&hv(s,a,e)?(o=u,a=Sl(c),e=El(c,e)):a=-1}}return r}function LE(n,e,t,i,r,s){let o=e[De],a=o.data[n+8],c=i==null?fr(a)&&Uh:i!=o&&(a.type&3)!==0,l=r&1&&s===a,u=xl(a,o,t,c,l);return u!==null?bl(e,o,u,a,r):Si}function xl(n,e,t,i,r){let s=n.providerIndexes,o=e.data,a=s&1048575,c=n.directiveStart,l=n.directiveEnd,u=s>>20,d=i?a:a+u,f=r?a+u:l;for(let h=d;h<f;h++){let g=o[h];if(h<c&&t===g||h>=c&&g.type===t)return h}if(r){let h=o[c];if(h&&Kr(h)&&h.type===t)return c}return null}function bl(n,e,t,i,r){let s=n[t],o=e.data;if(s instanceof ca){let a=s;if(a.resolving)throw Jf("");let c=fv(a.canSeeViewProviders);a.resolving=!0;let l=o[t].type||o[t],u,d=a.injectImpl?Tn(a.injectImpl):null,f=Mh(n,i,0);try{s=n[t]=a.factory(void 0,r,o,n,i),e.firstCreatePass&&t>=i.directiveStart&&ME(t,o[t],e)}finally{d!==null&&Tn(d),fv(c),a.resolving=!1,Sh()}}return s}function OE(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=Object.hasOwn(n,$r)?n[$r]:void 0;return typeof e=="number"?e>=0?e&zv:FE:e}function hv(n,e,t){let i=1<<n;return!!(t[e+(n>>Gv)]&i)}function pv(n,e){return!(n&2)&&!(n&1&&e)}var Gi=class{_tNode;_lView;constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return Xv(this._tNode,this._lView,e,Gr(i),t)}};function FE(){return new Gi(ni(),ut())}function kE(n,e,t,i,r){let s=n,o=e;for(;s!==null&&o!==null&&o[Pe]&2048&&!Bs(o);){let a=Yv(s,o,t,i|2,Si);if(a!==Si)return a;i&=-5;let c=s.parent;if(!c){let l=o[oh];if(l){let u=l.get(t,Si,i);if(u!==Si)return u}c=Zv(o),o=o[lr]}s=c}return r}function Zv(n){let e=n[De],t=e.type;return t===2?e.declTNode:t===1?n[Mn]:null}function Fl(n){return{token:n.token,providedIn:n.autoProvided===!1?null:"root",factory:n.factory,value:void 0}}function UE(){return Zs(ni(),ut())}function Zs(n,e){return new rs(ti(n,e))}var rs=(()=>{class n{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=UE}return n})();function BE(n){return n instanceof rs?n.nativeElement:n}function VE(){return this._results[Symbol.iterator]()}var Tl=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new Hn}constructor(e=!1){this._emitDistinctChangesOnly=e}get(e){return this._results[e]}map(e){return this._results.map(e)}filter(e){return this._results.filter(e)}find(e){return this._results.find(e)}reduce(e,t){return this._results.reduce(e,t)}forEach(e){this._results.forEach(e)}some(e){return this._results.some(e)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(e,t){this.dirty=!1;let i=gy(e);(this._changesDetected=!my(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(e){this._onDirty=e}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=VE};function Kv(n){return(n.flags&128)===128}var fp=(function(n){return n[n.OnPush=0]="OnPush",n[n.Eager=1]="Eager",n[n.Default=1]="Default",n})(fp||{}),Jv=new Map,HE=0;function zE(){return HE++}function GE(n){Jv.set(n[Jo],n)}function Bh(n){Jv.delete(n[Jo])}var mv="__ngContext__";function qs(n,e){dr(e)?(n[mv]=e[Jo],GE(e)):n[mv]=e}function Qv(n){return t_(n[Us])}function e_(n){return t_(n[zn])}function t_(n){for(;n!==null&&!Gn(n);)n=n[zn];return n}var Vh;function hp(n){Vh=n}function n_(){if(Vh!==void 0)return Vh;if(typeof document<"u")return document;throw new $e(210,!1)}var i_="r";var r_="di";var s_=!1,o_=new et("",{factory:()=>s_});var gv=new WeakMap;function WE(n,e){if(n==null||typeof n!="object")return;let t=gv.get(n);t||(t=new WeakSet,gv.set(n,t)),t.add(e)}var jE=(n,e,t,i)=>{};function $E(n,e,t,i){jE(n,e,t,i)}function pp(n){return(n.flags&32)===32}var qE=()=>null;function a_(n,e,t=!1){return qE(n,e,t)}function c_(n,e){let t=n.contentQueries;if(t!==null){let i=we(null);try{for(let r=0;r<t.length;r+=2){let s=t[r],o=t[r+1];if(o!==-1){let a=n.data[o];al(s),a.contentQueries(2,e[o],o)}}}finally{we(i)}}}function Hh(n,e,t){al(0);let i=we(null);try{e(n,t)}finally{we(i)}}function l_(n,e,t){if(ch(e)){let i=we(null);try{let r=e.directiveStart,s=e.directiveEnd;for(let o=r;o<s;o++){let a=n.data[o];if(a.contentQueries){let c=t[o];a.contentQueries(1,c,o)}}}finally{we(i)}}}var ri=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n[n.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",n})(ri||{}),zh=class{changingThisBreaksApplicationSecurity;constructor(e){this.changingThisBreaksApplicationSecurity=e}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity}`}};function wl(n){return n instanceof zh?n.changingThisBreaksApplicationSecurity:n}function XE(n,e){return n.createText(e)}function YE(n,e,t){n.setValue(e,t)}function u_(n,e,t){return n.createElement(e,t)}function ts(n,e,t,i,r){n.insertBefore(e,t,i,r)}function d_(n,e,t){n.appendChild(e,t)}function yv(n,e,t,i,r){i!==null?ts(n,e,t,i,r):d_(n,e,t)}function f_(n,e,t,i){n.removeChild(null,e,t,i)}function ZE(n,e,t){n.setAttribute(e,"style",t)}function KE(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function h_(n,e,t){let{mergedAttrs:i,classes:r,styles:s}=t;i!==null&&wE(n,e,i),r!==null&&KE(n,e,r),s!==null&&ZE(n,e,s)}function mp(n){return n.ownerDocument.defaultView}function JE(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let s=e.length;if(r+s===i||n.charCodeAt(r+s)<=32)return r}t=r+1}}var p_="ng-template";function QE(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&JE(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(gp(n))return!1;if(r=e.indexOf(1,r),r>-1){let s;for(;++r<e.length&&typeof(s=e[r])=="string";)if(s.toLowerCase()===t)return!0}return!1}function gp(n){return n.type===4&&n.value!==p_}function eb(n,e,t){let i=n.type===4&&!t?p_:n.value;return e===i}function tb(n,e,t){let i=4,r=n.attrs,s=r!==null?rb(r):0,o=!1;for(let a=0;a<e.length;a++){let c=e[a];if(typeof c=="number"){if(!o&&!ii(i)&&!ii(c))return!1;if(o&&ii(c))continue;o=!1,i=c|i&1;continue}if(!o)if(i&4){if(i=2|i&1,c!==""&&!eb(n,c,t)||c===""&&e.length===1){if(ii(i))return!1;o=!0}}else if(i&8){if(r===null||!QE(n,r,c,t)){if(ii(i))return!1;o=!0}}else{let l=e[++a],u=nb(c,r,gp(n),t);if(u===-1){if(ii(i))return!1;o=!0;continue}if(l!==""){let d;if(u>s?d="":d=r[u+1].toLowerCase(),i&2&&l!==d){if(ii(i))return!1;o=!0}}}}return ii(i)||o}function ii(n){return(n&1)===0}function nb(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let s=!1;for(;r<e.length;){let o=e[r];if(o===n)return r;if(o===3||o===6)s=!0;else if(o===1||o===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(o===4)break;if(o===0){r+=4;continue}}r+=s?1:2}return-1}else return sb(e,n)}function ib(n,e,t=!1){for(let i=0;i<e.length;i++)if(tb(n,e[i],t))return!0;return!1}function rb(n){for(let e=0;e<n.length;e++){let t=n[e];if(IE(t))return e}return n.length}function sb(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function vv(n,e){return n?":not("+e.trim()+")":e}function ob(n){let e=n[0],t=1,i=2,r="",s=!1;for(;t<n.length;){let o=n[t];if(typeof o=="string")if(i&2){let a=n[++t];r+="["+o+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+o:i&4&&(r+=" "+o);else r!==""&&!ii(o)&&(e+=vv(s,r),r=""),i=o,s=s||!ii(i);t++}return r!==""&&(e+=vv(s,r)),e}function ab(n){return n.map(ob).join(",")}function cb(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let s=n[i];if(typeof s=="string")r===2?s!==""&&e.push(s,n[++i]):r===8&&t.push(s);else{if(!ii(r))break;r=s}i++}return t.length&&e.push(1,...t),e}var ji={},Wi=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})(Wi||{}),lb;function yp(n,e){return lb(n,e)}var vp=new Set;var $O=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Gh=new WeakMap;function m_(n){return n?n[lr]??n:null}var Ws=new WeakSet;function ub(n,e,t){let i=Gh.get(n);if(!i||i.length===0)return;let r=e.parentNode,s=e.previousSibling,o=m_(t);for(let a=i.length-1;a>=0;a--){let{el:c,declarationView:l}=i[a],u=c.parentNode;c===e?(i.splice(a,1),Ws.add(c),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):s&&c===s?(i.splice(a,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c)):u&&r&&u!==r&&(o===null||l===null||o===l)&&(i.splice(a,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c))}}function db(n,e,t){let i=m_(t),r=Gh.get(n);r?r.some(s=>s.el===e)||r.push({el:e,declarationView:i}):Gh.set(n,[{el:e,declarationView:i}])}var _p=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(_p||{}),ss=new et(""),_v=new Set;function Ks(n){_v.has(n)||(_v.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var g_=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=jt({token:n,providedIn:"root",factory:()=>new n})}return n})();var Wh=null,y_=null;function xv(n,e,t,i){Wh?.maybeQueueEnterAnimation(n,e,t,i)}function Mv(n,e,t,i){Wh===null?i(!1):Wh.runLeaveAnimationsWithCallback(n,e,t,i)}function fb(n){y_?.initViewDetachAnimations(n)}function hb(n){y_?.clearViewDetachAnimations(n)}function js(n,e,t,i,r,s,o,a){if(r!=null){let c,l=!1;Gn(r)?c=r:dr(r)&&(l=!0,r=r[ei]);let u=Kt(r);n===0&&i!==null?(xv(a,i,s,t),o==null?d_(e,i,u):ts(e,i,u,o||null,!0)):n===1&&i!==null?(ts(e,i,u,o||null,!0),ub(s,u,a),Ws.has(u)||xv(a,i,s,t)):n===2?(a?.[Ty]?.leave?.has(s.index)&&db(s,u,a),Ws.delete(u),Mv(a,s,t,d=>{if(Ws.has(u)){Ws.delete(u);return}f_(e,u,l,d)})):n===3&&(Ws.delete(u),Mv(a,s,t,()=>{e.destroyNode(u)})),c!=null&&wb(e,n,t,c,s,i,o)}}function pb(n,e){v_(n,e),e[ei]=null,e[Mn]=null}function mb(n,e,t,i,r,s){i[ei]=r,i[Mn]=e,Ul(n,i,t,1,r,s)}function v_(n,e){e[vi].changeDetectionScheduler?.notify(9),Ul(n,e,e[Pt],2,null,null)}function gb(n){let e=n[Us];if(!e)return Nh(n[De],n);for(;e;){let t=null;if(dr(e))t=e[Us];else{let i=e[Ct];i&&(t=i)}if(!t){for(;e&&!e[zn]&&e!==n;)dr(e)&&Nh(e[De],e),e=e[At];e===null&&(e=n),dr(e)&&Nh(e[De],e),t=e&&e[zn]}e=t}}function xp(n,e){let t=n[Zr],i=t.indexOf(e);t.splice(i,1)}function kl(n,e){if(Jr(e))return;let t=e[Pt];t.destroyNode&&Ul(n,e,t,3,null,null),gb(e)}function Nh(n,e){if(Jr(e))return;let t=we(null);try{e[Pe]&=-129,e[Pe]|=256,e[An]&&Ts(e[An]),vb(n,e),yb(n,e),e[De].type===1&&e[Pt].destroy();let i=e[ur];if(i!==null&&Gn(e[At])){i!==e[At]&&xp(i,e);let r=e[_i];r!==null&&r.detachView(n)}Bh(e)}finally{we(t)}}function yb(n,e){let t=n.cleanup,i=e[ks];if(t!==null)for(let o=0;o<t.length-1;o+=2)if(typeof t[o]=="string"){let a=t[o+3];a>=0?i[a]():i[-a].unsubscribe(),o+=2}else{let a=i[t[o+1]];t[o].call(a)}i!==null&&(e[ks]=null);let r=e[ki];if(r!==null){e[ki]=null;for(let o=0;o<r.length;o++){let a=r[o];a()}}let s=e[Bi];if(s!==null){e[Bi]=null;for(let o of s)o.destroy()}}function vb(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof ca)){let s=t[i+1];if(Array.isArray(s))for(let o=0;o<s.length;o+=2){let a=r[s[o]],c=s[o+1];ht(it.LifecycleHookStart,a,c);try{c.call(a)}finally{ht(it.LifecycleHookEnd,a,c)}}else{ht(it.LifecycleHookStart,r,s);try{s.call(r)}finally{ht(it.LifecycleHookEnd,r,s)}}}}}function _b(n,e,t){if(e===null)throw new $e(510,!1);return xb(n,e.parent,t)}function xb(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[ei];if(fr(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===ri.None||r===ri.Emulated)return null}return ti(i,t)}function Mb(n,e,t){return Eb(n,e,t)}function Sb(n,e,t){return n.type&40?ti(n,t):null}var Eb=Sb,Sv;function Mp(n,e,t,i){let r=_b(n,i,e),s=e[Pt],o=i.parent||e[Mn],a=Mb(o,i,e);if(r!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)yv(s,r,t[c],a,!1);else yv(s,r,t,a,!1);Sv!==void 0&&Sv(s,i,e,t,r)}function oa(n,e){if(e!==null){let t=e.type;if(t&3)return ti(e,n);if(t&4)return jh(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return oa(n,i);{let r=n[e.index];return Gn(r)?jh(-1,r):Kt(r)}}else{if(t&128)return oa(n,e.next);if(t&32)return yp(e,n)()||Kt(n[e.index]);{let i=__(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=Vi(n[Cn]);return oa(r,i)}else return oa(n,e.next)}}}return null}function __(n,e){if(e!==null){let i=n[Cn][Mn],r=e.projection;return i.projection[r]}return null}function jh(n,e){let t=Ct+n+1;if(t<e.length){let i=e[t],r=i[De].firstChild;if(r!==null)return oa(i,r)}return e[Yr]}function Sp(n,e,t,i,r,s,o){for(;t!=null;){let a=i[zi];if(t.type===128){t=t.next;continue}let c=i[t.index],l=t.type;if(o&&e===0&&(c&&qs(Kt(c),i),t.flags|=2),!pp(t))if(l&8)Sp(n,e,t.child,i,r,s,!1),js(e,n,a,r,c,t,s,i);else if(l&32){let u=yp(t,i),d;for(;d=u();)js(e,n,a,r,d,t,s,i);js(e,n,a,r,c,t,s,i)}else l&16?Tb(n,e,i,t,r,s):js(e,n,a,r,c,t,s,i);t=o?t.projectionNext:t.next}}function Ul(n,e,t,i,r,s){n.type===3?bb(t,i,e,r,s):Sp(t,i,n.firstChild,e,r,s,!1)}function bb(n,e,t,i,r){let o=t[De].firstChild,a=o.next,c=Kt(t[o.index]),l=Kt(t[a.index]),u=a.index+1,d=t[u];if(e===1||e===0)i!==null&&(d&&d.hasChildNodes()?ts(n,i,d,r,!0):(ts(n,i,c,r,!0),ts(n,i,l,r,!0)));else if(e===2){if(d||(d=document.createDocumentFragment(),t[u]=d),c&&c.parentNode===d)return;let f=c;for(;f!==null;){let h=f.nextSibling;if(d.appendChild(f),f===l)break;f=h}}}function Tb(n,e,t,i,r,s){let o=t[Cn],c=o[Mn].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let u=c[l];js(e,n,t[zi],r,u,i,s,t)}else{let l=c,u=o[At];Kv(i)&&(l.flags|=128),Sp(n,e,l,u,r,s,!0)}}function wb(n,e,t,i,r,s,o){let a=i[Yr],c=Kt(i);if(a!==c&&js(e,n,t,s,a,r,o),(i[Pe]&4)===0)for(let l=Ct;l<i.length;l++){let u=i[l];Ul(u[De],u,n,e,s,a)}}function Ib(n,e,t,i,r){if(e)r?n.addClass(t,i):n.removeClass(t,i);else{let s=i.indexOf("-")===-1?void 0:Wi.DashCase;r==null?n.removeStyle(t,i,s):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),s|=Wi.Important),n.setStyle(t,i,r,s))}}function Ep(n,e,t,i,r,s,o,a,c,l,u){let d=$t+i,f=d+r,h=Cb(d,f),g=typeof l=="function"?l():l;return h[De]={type:n,blueprint:h,template:t,queries:null,viewQuery:a,declTNode:e,data:h.slice().fill(null,d),bindingStartIndex:d,expandoStartIndex:f,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof s=="function"?s():s,pipeRegistry:typeof o=="function"?o():o,firstChild:null,schemas:c,consts:g,incompleteFirstPass:!1,ssrId:u}}function Cb(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:ji);return t}function Ab(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=Ep(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function bp(n,e,t,i,r,s,o,a,c,l,u){let d=e.blueprint.slice();return d[ei]=r,d[Pe]=i|4|128|8|64|1024,(l!==null||n&&n[Pe]&2048)&&(d[Pe]|=2048),dh(d),d[At]=d[lr]=n,d[It]=t,d[vi]=o||n&&n[vi],d[Pt]=a||n&&n[Pt],d[zi]=c||n&&n[zi]||null,d[Mn]=s,d[Jo]=zE(),d[Fs]=u,d[oh]=l,d[Cn]=e.type==2?n[Cn]:d,d}function Db(n,e,t){let i=ti(e,n),r=Ab(t),s=n[vi].rendererFactory,o=Tp(n,bp(n,r,null,x_(t),i,e,null,s.createRenderer(i,t),null,null,null));return n[e.index]=o}function x_(n){let e=16;return n.signals?e=4096:n.onPush&&(e=64),e}function M_(n,e,t,i){if(t===0)return-1;let r=e.length;for(let s=0;s<t;s++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function Tp(n,e){return n[Us]?n[sh][zn]=e:n[Us]=e,n[sh]=e,e}function gt(n=1){S_(Wn(),ut(),hr()+n,!1)}function S_(n,e,t,i){if(!i)if((e[Pe]&3)===3){let s=n.preOrderCheckHooks;s!==null&&vl(e,s,t)}else{let s=n.preOrderHooks;s!==null&&_l(e,s,0,t)}pr(t)}var Bl=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(Bl||{});function $h(n,e,t,i){let r=we(null);try{let[s,o,a]=n.inputs[t],c=null;(o&Bl.SignalBased)!==0&&(c=e[s][_n]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(e,i)),n.setInput!==null?n.setInput(e,c,i,t,s):vE(e,c,s,i)}finally{we(r)}}function E_(n,e,t,i,r){let s=hr(),o=i&2;try{pr(-1),o&&e.length>$t&&S_(n,e,$t,!1);let a=o?it.TemplateUpdateStart:it.TemplateCreateStart;ht(a,r,t),t(i,r)}finally{pr(s);let a=o?it.TemplateUpdateEnd:it.TemplateCreateEnd;ht(a,r,t)}}function b_(n,e,t){Ub(n,e,t),(t.flags&64)===64&&Bb(n,e,t)}function wp(n,e,t=ti){let i=e.localNames;if(i!==null){let r=e.index+1;for(let s=0;s<i.length;s+=2){let o=i[s+1],a=o===-1?t(e,n):n[o];n[r++]=a}}}function Rb(n,e,t,i){let s=i.get(o_,s_)||t===ri.ShadowDom||t===ri.ExperimentalIsolatedShadowDom,o=n.selectRootElement(e,s);return Nb(o),o}function Nb(n){Pb(n)}var Pb=()=>null;function Lb(n){return n==="class"?"className":n==="for"?"htmlFor":n==="formaction"?"formAction":n==="innerHtml"?"innerHTML":n==="readonly"?"readOnly":n==="tabindex"?"tabIndex":n}function Ob(n,e,t,i,r,s){let o=e[De];if(Ip(n,o,e,t,i)){fr(n)&&kb(e,n.index);return}n.type&3&&(t=Lb(t)),Fb(n,e,t,i,r,s)}function Fb(n,e,t,i,r,s){if(n.type&3){let o=ti(n,e);i=s!=null?s(i,n.value||"",t):i,i=T_(t,o,i),r.setProperty(o,t,i)}else n.type&12}function T_(n,e,t,i=!1){return n==="src"&&(e.tagName==="IMG"||e.tagName==="VIDEO")?t==null?i?t:"":wl(t):t}function kb(n,e){let t=xi(e,n);t[Pe]&16||(t[Pe]|=64)}function Ub(n,e,t){let i=t.directiveStart,r=t.directiveEnd;fr(t)&&Db(e,t,n.data[i+t.componentOffset]),n.firstCreatePass||Wv(t,e);let s=t.initialInputs;for(let o=i;o<r;o++){let a=n.data[o],c=bl(e,n,o,t);if(qs(c,e),s!==null&&Wb(e,o-i,c,a,t,s),Kr(a)){let l=xi(t.index,e);l[It]=bl(e,n,o,t)}}}function Bb(n,e,t){let i=t.directiveStart,r=t.directiveEnd,s=t.index,o=Wy();try{pr(s);for(let a=i;a<r;a++){let c=n.data[a],l=e[a];ol(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&Vb(c,l)}}finally{pr(-1),ol(o)}}function Vb(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function Hb(n,e){let t=n.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let s=t[r];ib(e,s.selectors,!1)&&(i??=[],Kr(s)?i.unshift(s):i.push(s))}return i}function zb(n,e,t,i,r,s){let o=ti(n,e);i=T_(t,o,i,!0),Gb(e[Pt],o,s,n.value,t,i,r)}function Gb(n,e,t,i,r,s,o){if(s==null)o?.(s,i||"",r),n.removeAttribute(e,r,t);else{let a=o==null?Kf(s):o(s,i||"",r);n.setAttribute(e,r,a,t)}}function Wb(n,e,t,i,r,s){let o=s[e];if(o!==null)for(let a=0;a<o.length;a+=2){let c=o[a],l=o[a+1];$h(i,t,c,l)}}function w_(n,e,t,i,r){let s=$t+t,o=e[De],a=r(o,e,n,i,t);e[s]=a,Hs(n,!0);let c=n.type===2;return c?(h_(e[Pt],a,n),(Py()===0||tl(n))&&qs(a,e),Ly()):qs(a,e),dl()&&(!c||!pp(n))&&Mp(o,e,a,n),n}function I_(n){let e=n;return _h()?Uy():(e=e.parent,Hs(e,!1)),e}function jb(n,e){let t=n[zi];if(!t)return;let i;try{i=t.get(Qr,null)}catch{i=null}i?.(e)}function Ip(n,e,t,i,r){let s=n.inputs?.[i],o=n.hostDirectiveInputs?.[i],a=!1;if(o)for(let c=0;c<o.length;c+=2){let l=o[c],u=o[c+1],d=e.data[l];$h(d,t[l],u,r),a=!0}if(s)for(let c of s){let l=t[c],u=e.data[c];$h(u,l,i,r),a=!0}return a}function $b(n,e){let t=xi(e,n),i=t[De];qb(i,t);let r=t[ei];r!==null&&t[Fs]===null&&(t[Fs]=a_(r,t[zi])),ht(it.ComponentStart);try{Cp(i,t,t[It])}finally{ht(it.ComponentEnd,t[It])}}function qb(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function Cp(n,e,t){cl(e);try{let i=n.viewQuery;i!==null&&Hh(1,i,t);let r=n.template;r!==null&&E_(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[_i]?.finishViewCreation(n),n.staticContentQueries&&c_(n,e),n.staticViewQueries&&Hh(2,n.viewQuery,t);let s=n.components;s!==null&&Xb(e,s)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[Pe]&=-5,ll()}}function Xb(n,e){for(let t=0;t<e.length;t++)$b(n,e[t])}function Vl(n,e,t,i){let r=we(null);try{let s=e.tView,a=n[Pe]&4096?4096:16,c=bp(n,s,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=n[e.index];c[ur]=l;let u=n[_i];return u!==null&&(c[_i]=u.createEmbeddedView(s)),Cp(s,c,t),c}finally{we(r)}}function la(n,e){return!e||e.firstChild===null||Kv(n)}function ua(n,e,t,i,r=!1){if(n.type===3){let s=n.firstChild,o=s.next,a=Kt(e[s.index]),c=Kt(e[o.index]),l=a;for(;l!==null&&(i.push(l),l!==c);)l=l.nextSibling;return i}for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let s=e[t.index];if(s!==null)if(Gn(s)){let a=s[Yr];a!==s[ei]&&i.push(Kt(s)),s[Pe]&4||C_(s,i),i.push(a)}else i.push(Kt(s));let o=t.type;if(o&8)ua(n,e,t.child,i);else if(o&32){let a=yp(t,e),c;for(;c=a();)i.push(c)}else if(o&16){let a=__(e,t);if(Array.isArray(a))i.push(...a);else{let c=Vi(e[Cn]);ua(c[De],c,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function C_(n,e){for(let t=Ct;t<n.length;t++){let i=n[t],r=i[De].firstChild;r!==null&&ua(i[De],i,r,e)}}function A_(n){if(n[el]!==null){for(let e of n[el])e.impl.addSequence(e);n[el].length=0}}var D_=[];function Yb(n){return n[An]??Zb(n)}function Zb(n){let e=D_.pop()??Object.create(Jb);return e.lView=n,e}function Kb(n){n.lView[An]!==n&&(n.lView=null,D_.push(n))}var Jb=vt(Ve({},bs),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{Vs(n.lView)},consumerOnSignalRead(){this.lView[An]=this}});function Qb(n){let e=n[An]??Object.create(eT);return e.lView=n,e}var eT=vt(Ve({},bs),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let e=Vi(n.lView);for(;e&&!R_(e[De]);)e=Vi(e);e&&fh(e)},consumerOnSignalRead(){this.lView[An]=this}});function R_(n){return n.type!==2}function N_(n){if(n[Bi]===null)return;let e=!0;for(;e;){let t=!1;for(let i of n[Bi])if(i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()),n[Bi]===null))return;e=t&&!!(n[Pe]&8192)}}var tT=100;function P_(n,e=0){let i=n[vi].rendererFactory,r=!1;r||i.begin?.();try{nT(n,e)}finally{r||i.end?.()}}function nT(n,e){let t=xh();try{jo(!0),qh(n,e);let i=0;for(;ta(n);){if(i===tT)throw new $e(103,!1);i++,qh(n,1)}}finally{jo(t)}}function iT(n,e,t,i){if(Jr(e))return;let r=e[Pe],s=!1,o=!1;cl(e);let a=!0,c=null,l=null;s||(R_(n)?(l=Yb(e),c=Vo(l)):bc()===null?(a=!1,l=Qb(e),c=Vo(l)):e[An]&&(Ts(e[An]),e[An]=null));try{dh(e),Vy(n.bindingStartIndex),t!==null&&E_(n,e,t,2,i);let u=(r&3)===3;if(!s)if(u){let h=n.preOrderCheckHooks;h!==null&&vl(e,h,null)}else{let h=n.preOrderHooks;h!==null&&_l(e,h,0,null),Dh(e,0)}if(o||rT(e),N_(e),O_(e,0),n.contentQueries!==null&&c_(n,e),!s)if(u){let h=n.contentCheckHooks;h!==null&&vl(e,h)}else{let h=n.contentHooks;h!==null&&_l(e,h,1),Dh(e,1)}oT(n,e);let d=n.components;d!==null&&k_(e,d,0);let f=n.viewQuery;if(f!==null&&Hh(2,f,i),!s)if(u){let h=n.viewCheckHooks;h!==null&&vl(e,h)}else{let h=n.viewHooks;h!==null&&_l(e,h,2),Dh(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[Qc]){for(let h of e[Qc])h();e[Qc]=null}s||(A_(e),e[Pe]&=-73)}catch(u){L_(e,u,s)}finally{l!==null&&(Ic(l,c),a&&Kb(l)),ll()}}function L_(n,e,t){let i=!1,r=e,s=n,o=we(null);try{for(;s!==null;){if(Gn(s)){s=s[At];continue}let a=s[Qo];if(a)try{let c=n[Cn][It],l=c?.constructor,u={declarationInstance:c,declarationType:l,caughtBy:a},d=we(null);try{a(iv(r),u)}finally{we(d)}i=!0;break}catch(c){r=c}s=s[At]}}finally{we(o)}if(!i)throw t||Vs(n),r}function O_(n,e){for(let t=Qv(n);t!==null;t=e_(t))for(let i=Ct;i<t.length;i++){let r=t[i];F_(r,e)}}function rT(n){for(let e=Qv(n);e!==null;e=e_(e)){if(!(e[Pe]&2))continue;let t=e[Zr];for(let i=0;i<t.length;i++){let r=t[i];fh(r)}}}function sT(n,e,t){ht(it.ComponentStart);let i=xi(e,n);try{F_(i,t)}finally{ht(it.ComponentEnd,i[It])}}function F_(n,e){il(n)&&qh(n,e)}function qh(n,e){let i=n[De],r=n[Pe],s=n[An],o=!!(e===0&&r&16);if(o||=!!(r&64&&e===0),o||=!!(r&1024),o||=!!(s?.dirty&&Cc(s)),o||=!1,s&&(s.dirty=!1),n[Pe]&=-9217,o)iT(i,n,i.template,n[It]);else if(r&8192){let a=we(null);try{try{N_(n)}catch(l){L_(n,l,!1)}O_(n,1);let c=i.components;c!==null&&k_(n,c,1),A_(n)}finally{we(a)}}}function k_(n,e,t){for(let i=0;i<e.length;i++)sT(n,e[i],t)}function oT(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)pr(~r);else{let s=r,o=t[++i],a=t[++i];Gy(o,s);let c=e[s];ht(it.HostBindingsUpdateStart,c);try{a(2,c)}finally{ht(it.HostBindingsUpdateEnd,c)}}}}finally{pr(-1)}}function Ap(n,e){let t=xh()?64:1088;for(n[vi].changeDetectionScheduler?.notify(e);n;){n[Pe]|=t;let i=Vi(n);if(Bs(n)&&!i)return n;n=i}return null}function U_(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function B_(n,e){let t=Ct+e;if(t<n.length)return n[t]}function Hl(n,e,t,i=!0){let r=e[De];if(aT(r,e,n,t),i){let o=jh(t,n),a=e[Pt],c=a.parentNode(n[Yr]);c!==null&&mb(r,n[Mn],a,e,c,o)}let s=e[Fs];s!==null&&s.firstChild!==null&&(s.firstChild=null)}function V_(n,e){let t=da(n,e);return t!==void 0&&kl(t[De],t),t}function da(n,e){if(n.length<=Ct)return;let t=Ct+e,i=n[t];if(i){let r=i[ur];r!==null&&r!==n&&xp(r,i),e>0&&(n[t-1][zn]=i[zn]);let s=Xo(n,Ct+e);pb(i[De],i);let o=s[_i];o!==null&&o.detachView(s[De]),i[At]=null,i[zn]=null,i[Pe]&=-129}return i}function aT(n,e,t,i){let r=Ct+i,s=t.length;i>0&&(t[r-1][zn]=e),i<s-Ct?(e[zn]=t[r],eh(t,Ct+i,e)):(t.push(e),e[zn]=null),e[At]=t;let o=e[ur];o!==null&&t!==o&&H_(o,e);let a=e[_i];a!==null&&a.insertView(n),rl(e),e[Pe]|=128}function H_(n,e){let t=n[Zr],i=e[At];if(dr(i))n[Pe]|=2;else{let r=i[At][Cn];e[Cn]!==r&&(n[Pe]|=2)}t===null?n[Zr]=[e]:t.push(e)}var Xs=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let e=this._lView,t=e[De];return ua(t,e,t.firstChild,[])}constructor(e,t){this._lView=e,this._cdRefInjectingView=t}get context(){return this._lView[It]}set context(e){this._lView[It]=e}get destroyed(){return Jr(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[At];if(Gn(e)){let t=e[ea],i=t?t.indexOf(this):-1;i>-1&&(da(e,i),Xo(t,i))}this._attachedToViewContainer=!1}kl(this._lView[De],this._lView)}onDestroy(e){sl(this._lView,e)}markForCheck(){Ap(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[Pe]&=-129}reattach(){rl(this._lView),this._lView[Pe]|=128}detectChanges(){this._lView[Pe]|=1024,P_(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new $e(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=Bs(this._lView),t=this._lView[ur];t!==null&&!e&&xp(t,this._lView),v_(this._lView[De],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new $e(902,!1);this._appRef=e;let t=Bs(this._lView),i=this._lView[ur];i!==null&&!t&&H_(i,this._lView),rl(this._lView)}};var fa=(()=>{class n{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=cT;constructor(t,i,r){this._declarationLView=t,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,i){return this.createEmbeddedViewImpl(t,i)}createEmbeddedViewImpl(t,i,r){let s=Vl(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:i,dehydratedView:r});return new Xs(s)}}return n})();function cT(){return Dp(ni(),ut())}function Dp(n,e){return n.type&4?new fa(e,n,Zs(n,e)):null}function zl(n,e,t,i,r){let s=n.data[e];if(s===null)s=lT(n,e,t,i,r),zy()&&(s.flags|=32);else if(s.type&64){s.type=t,s.value=i,s.attrs=r;let o=ky();s.injectorIndex=o===null?-1:o.injectorIndex}return Hs(s,!0),s}function lT(n,e,t,i,r){let s=vh(),o=_h(),a=o?s:s&&s.parent,c=n.data[e]=dT(n,a,t,e,i,r);return uT(n,c,s,o),c}function uT(n,e,t,i){n.firstChild===null&&(n.firstChild=e),t!==null&&(i?t.child==null&&e.parent!==null&&(t.child=e):t.next===null&&(t.next=e,e.prev=t))}function dT(n,e,t,i,r,s){let o=e?e.injectorIndex:-1,a=0;return Fy()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:o,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:bh(),attrs:s,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function fT(n){let e=n[ah]??[],i=n[At][Pt],r=[];for(let s of e)s.data[r_]!==void 0?r.push(s):hT(s,i);n[ah]=r}function hT(n,e){let t=0,i=n.firstChild;if(i){let r=n.data[i_];for(;t<r;){let s=i.nextSibling;f_(e,i,!1),i=s,t++}}}var pT=()=>null,mT=()=>null;function Xh(n,e){return pT(n,e)}function z_(n,e,t){return mT(n,e,t)}var G_=class{},ns=class{};var W_=(()=>{class n{static \u0275prov=jt({token:n,providedIn:"root",factory:()=>null})}return n})();function j_(n){return n.debugInfo?.className||n.type.name||null}var Ml={},Il=class{injector;parentInjector;constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){let r=this.injector.get(e,Ml,i);return r!==Ml||t===Ml?r:this.parentInjector.get(e,t,i)}};function Js(n,e,t){if(t===ji)return!1;let i=n[e];return Object.is(i,t)?!1:(n[e]=t,!0)}function Ph(n,e,t){return function i(r){let s=i.__ngNativeEl__;s!==void 0&&WE(r,s);let o=fr(n)?xi(n.index,e):e;Ap(o,5);let a=e[It],c=Ev(e,a,t,r),l=i.__ngNextListenerFn__;for(;l;)c=Ev(e,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function Ev(n,e,t,i){let r=we(null);try{return ht(it.OutputStart,e,t),t(i)!==!1}catch(s){return jb(n,s),!1}finally{ht(it.OutputEnd,e,t),we(r)}}function gT(n,e,t,i,r,s,o,a){let c=tl(n),l=!1,u=null;if(!i&&c&&(u=vT(e,t,s,n.index)),u!==null){let d=u.__ngLastListenerFn__||u;d.__ngNextListenerFn__=o,u.__ngLastListenerFn__=o,l=!0}else{let d=ti(n,t),f=i?i(d):d;$E(t,f,s,a),i||(a.__ngNativeEl__=d);let h=r.listen(f,s,a);if(!yT(s)){let g=i?M=>i(Kt(M[n.index])):n.index;$_(g,e,t,s,a,h,!1)}}return l}function yT(n){return n.startsWith("animation")||n.startsWith("transition")}function vT(n,e,t,i){let r=n.cleanup;if(r!=null)for(let s=0;s<r.length-1;s+=2){let o=r[s];if(o===t&&r[s+1]===i){let a=e[ks],c=r[s+2];return a&&a.length>c?a[c]:null}typeof o=="string"&&(s+=2)}return null}function $_(n,e,t,i,r,s,o){let a=e.firstCreatePass?ph(e):null,c=hh(t),l=c.length;c.push(r,s),a&&a.push(i,n,l,(l+1)*(o?-1:1))}function bv(n,e,t,i,r,s){let o=e[t],a=e[De],l=a.data[t].outputs[i],d=o[l].subscribe(s);$_(n.index,a,e,r,s,d,!0)}var Yh=Symbol("BINDING");var os=new et("");function Cl(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,s=0;if(e!==null)for(let o=0;o<e.length;o++){let a=e[o];if(typeof a=="number")s=a;else if(s==1)r=jf(r,a);else if(s==2){let c=a,l=e[++o];i=jf(i,c+": "+l+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}function q_(n,e=0){let t=ut();if(t===null)return lt(n,e);let i=ni();return Xv(i,t,wn(n),e)}function _T(n,e,t,i,r){let s=i===null?null:{"":-1},o=r(n,t);if(o!==null){let a=o,c=null,l=null;for(let u of o)if(u.resolveHostDirectives!==null){[a,c,l]=u.resolveHostDirectives(o);break}ST(n,e,t,a,s,c,l)}s!==null&&i!==null&&xT(t,i,s)}function xT(n,e,t){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let s=t[e[r+1]];if(s==null)throw new $e(-301,!1);i.push(e[r],s)}}function MT(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function ST(n,e,t,i,r,s,o){let a=i.length,c=null;for(let f=0;f<a;f++){let h=i[f];c===null&&Kr(h)&&(c=h,MT(n,t,f)),PE(Wv(t,e),n,h.type)}CT(t,n.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let f=0;f<a;f++){let h=i[f];h.providersResolver&&h.providersResolver(h)}let l=!1,u=!1,d=M_(n,e,a,null);a>0&&(t.directiveToIndex=new Map);for(let f=0;f<a;f++){let h=i[f];if(t.mergedAttrs=Ol(t.mergedAttrs,h.hostAttrs),bT(n,t,e,d,h),IT(d,h,r),o!==null&&o.has(h)){let[M,m]=o.get(h);t.directiveToIndex.set(h.type,[d,M+t.directiveStart,m+t.directiveStart])}else(s===null||!s.has(h))&&t.directiveToIndex.set(h.type,d);h.contentQueries!==null&&(t.flags|=4),(h.hostBindings!==null||h.hostAttrs!==null||h.hostVars!==0)&&(t.flags|=64);let g=h.type.prototype;!l&&(g.ngOnChanges||g.ngOnInit||g.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),l=!0),!u&&(g.ngOnChanges||g.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),u=!0),d++}ET(n,t,s)}function ET(n,e,t){for(let i=e.directiveStart;i<e.directiveEnd;i++){let r=n.data[i];if(t===null||!t.has(r))Tv(0,e,r,i),Tv(1,e,r,i),Iv(e,i,!1);else{let s=t.get(r);wv(0,e,s,i),wv(1,e,s,i),Iv(e,i,!0)}}}function Tv(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(Object.hasOwn(r,s)){let o;n===0?o=e.inputs??={}:o=e.outputs??={},o[s]??=[],o[s].push(i),X_(e,s)}}function wv(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(Object.hasOwn(r,s)){let o=r[s],a;n===0?a=e.hostDirectiveInputs??={}:a=e.hostDirectiveOutputs??={},a[o]??=[],a[o].push(i,s),X_(e,o)}}function X_(n,e){e==="class"?n.flags|=8:e==="style"&&(n.flags|=16)}function Iv(n,e,t){let{attrs:i,inputs:r,hostDirectiveInputs:s}=n;if(i===null||!t&&r===null||t&&s===null||gp(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let o=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&Object.hasOwn(r,c)){let l=r[c];for(let u of l)if(u===e){o??=[],o.push(c,i[a+1]);break}}else if(t&&Object.hasOwn(s,c)){let l=s[c];for(let u=0;u<l.length;u+=2)if(l[u]===e){o??=[],o.push(l[u+1],i[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(o)}function bT(n,e,t,i,r){n.data[i]=r;let s=r.factory||(r.factory=Ns(r.type,!0)),o=new ca(s,Kr(r),q_,null);n.blueprint[i]=o,t[i]=o,TT(n,e,i,M_(n,t,r.hostVars,ji),r)}function TT(n,e,t,i,r){let s=r.hostBindings;if(s){let o=n.hostBindingOpCodes;o===null&&(o=n.hostBindingOpCodes=[]);let a=~e.index;wT(o)!=a&&o.push(a),o.push(t,i,s)}}function wT(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function IT(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;Kr(e)&&(t[""]=n)}}function CT(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function Y_(n,e,t,i,r,s,o,a){let c=e[De],l=c.consts,u=Mi(l,o),d=zl(c,n,t,i,u);return s&&_T(c,e,d,Mi(l,a),r),d.mergedAttrs=Ol(d.mergedAttrs,d.attrs),d.attrs!==null&&Cl(d,d.attrs,!1),d.mergedAttrs!==null&&Cl(d,d.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,d),d}function Z_(n,e){SE(n,e),ch(e)&&n.queries.elementEnd(e)}function AT(n,e,t,i,r,s){let o=e.consts,a=Mi(o,r),c=zl(e,n,t,i,a);if(c.mergedAttrs=Ol(c.mergedAttrs,c.attrs),s!=null){let l=Mi(o,s);c.localNames=[];for(let u=0;u<l.length;u+=2)c.localNames.push(l[u],-1)}return c.attrs!==null&&Cl(c,c.attrs,!1),c.mergedAttrs!==null&&Cl(c,c.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,c),c}var K_=typeof ShadowRoot<"u",DT=typeof Document<"u";function RT(n){return Object.keys(n).map(e=>{let[t,i,r]=n[e],s={propName:t,templateName:e,isSignal:(i&Bl.SignalBased)!==0};return r&&(s.transform=r),s})}function NT(n){return Object.keys(n).map(e=>({propName:n[e],templateName:e}))}function PT(n,e,t){let i=e instanceof Qn?e:e?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new Il(t,i):t}function LT(n){let e=n.get(ns,null);if(e===null)throw new $e(407,!1);let t=n.get(W_,null),i=n.get(jr,null),r=n.get(ss,null,{optional:!0});return{rendererFactory:e,sanitizer:t,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function OT(n,e,t){let i=J_(n);return u_(e,i,i==="svg"?lh:i==="math"?Iy:t)}function FT(n){if((n&&"localName"in n&&typeof n.localName=="string"?n.localName:n?.tagName)?.toLowerCase()==="script")throw new $e(905,!1)}function J_(n){return(n.selectors[0][0]||"div").toLowerCase()}var ha=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=RT(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=NT(this.componentDef.outputs),this.cachedOutputs}constructor(e,t){this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=ab(e.selectors),this.ngContentSelectors=e.ngContentSelectors??[],this.isBoundToModule=!!t}create(e,t,i,r,s,o,a,c){ht(it.DynamicComponentStart);let l=we(null);try{let u=this.componentDef,d=PT(u,r||this.ngModule,e),f=LT(d),h=f.tracingService;return h&&h.componentCreate?h.componentCreate(j_(u),()=>this.createComponentRef(f,d,t,i,s,o,a,c)):this.createComponentRef(f,d,t,i,s,o,a,c)}finally{we(l)}}createComponentRef(e,t,i,r,s,o,a,c){let l=this.componentDef,u=kT(r,l,o,s,c),d=e.rendererFactory.createRenderer(null,l),f=r?Rb(d,r,l.encapsulation,t):OT(l,d,a??null);FT(f);let h=t.get(os,null),g=UT(f,()=>t.get(Dn,null)??n_());h&&h.addHost(g);let M=o?.some(Cv)||s?.some(x=>typeof x!="function"&&x.bindings.some(Cv)),m=bp(null,u,null,512|x_(l),null,null,e,d,t,null,a_(f,t,!0));h&&K_&&g instanceof ShadowRoot&&sl(m,()=>{h.removeHost(g)}),m[$t]=f,cl(m);let p=null;try{let x=Y_($t,m,2,"#host",()=>u.directiveRegistry,!0,0);h_(d,f,x),qs(f,m),b_(u,m,x),l_(u,x,m),Z_(u,x),i!==void 0&&VT(x,this.ngContentSelectors,i),p=xi(x.index,m),m[It]=p[It],Cp(u,m,null)}catch(x){throw p!==null&&Bh(p),Bh(m),x}finally{ht(it.DynamicComponentEnd),ll()}return new Al(this.componentType,m,!!M)}};function kT(n,e,t,i,r){let s=n?["ng-version","22.2.5"]:cb(e.selectors[0]),o=null,a=null,c=0;if(t)for(let d of t)c+=d[Yh].requiredVars,d.create&&(d.targetIdx=0,(o??=[]).push(d)),d.update&&(d.targetIdx=0,(a??=[]).push(d));if(i)for(let d=0;d<i.length;d++){let f=i[d];if(typeof f!="function")for(let h of f.bindings){c+=h[Yh].requiredVars;let g=d+1;h.create&&(h.targetIdx=g,(o??=[]).push(h)),h.update&&(h.targetIdx=g,(a??=[]).push(h))}}let l=[e];if(i)for(let d of i){let f=typeof d=="function"?d:d.type,h=qc(f);l.push(h)}return Ep(0,null,BT(o,a),1,c,l,null,null,null,[s],null)}function UT(n,e){let t=n.getRootNode?.();return DT&&t instanceof Document?t.head:t&&K_&&t instanceof ShadowRoot?t:e().head}function BT(n,e){return!n&&!e?null:t=>{if(t&1&&n)for(let i of n)i.create();if(t&2&&e)for(let i of e)i.update()}}function Cv(n){let e=n[Yh].kind;return e==="input"||e==="twoWay"}var Al=class extends G_{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(e,t,i){super(),this._rootLView=t,this._hasInputBindings=i,this._tNode=nl(t[De],$t),this.location=Zs(this._tNode,t),this.instance=xi(this._tNode.index,t)[It],this.hostView=this.changeDetectorRef=new Xs(t,void 0),this.componentType=e}setInput(e,t){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let r=this._rootLView,s=Ip(i,r[De],r,e,t);this.previousInputValues.set(e,t);let o=xi(i.index,r);Ap(o,1)}get injector(){return new Gi(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function VT(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let s=t[r];i.push(s!=null&&s.length?Array.from(s):null)}}var Gl=(()=>{class n{static __NG_ELEMENT_ID__=HT}return n})();function HT(){let n=ni();return Q_(n,ut())}var Zh=class n extends Gl{_lContainer;_hostTNode;_hostLView;constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return Zs(this._hostTNode,this._hostLView)}get injector(){return new Gi(this._hostTNode,this._hostLView)}get parentInjector(){let e=dp(this._hostTNode,this._hostLView);if(Hv(e)){let t=El(e,this._hostLView),i=Sl(e),r=t[De].data[i+8];return new Gi(r,t)}else return new Gi(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=Av(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-Ct}createEmbeddedView(e,t,i){let r,s,o;typeof i=="number"?r=i:i!=null&&(r=i.index,s=i.injector,o=i.onError);let a=Xh(this._lContainer,e.ssrId),c=e.createEmbeddedViewImpl(t||{},s,a);return o&&(c._lView[Qo]=o),this.insertImpl(c,r,la(this._hostTNode,a)),c}createComponent(e,t,i,r,s,o,a){let c,l,u=t||{};c=u.index,i=u.injector,r=u.projectableNodes,s=u.environmentInjector||u.ngModuleRef,o=u.directives,a=u.bindings,l=u.onError;let d=new ha(qr(e)),f=i||this.parentInjector;if(!s&&d.ngModule==null){let x=this.parentInjector.get(Qn,null);x&&(s=x)}let h=qr(d.componentType??{}),g=Xh(this._lContainer,h?.id??null),M=g?.firstChild??null,m=d.create(f,r,M,s,o,a,this._getHostElementNamespace());return l&&(m.hostView._lView[Qo]=l),this.insertImpl(m.hostView,c,la(this._hostTNode,g)),m}_getHostElementNamespace(){if(this._hostTNode.type&2){let e=this._hostTNode.parent??this._hostLView[Mn];return e!==null&&e.type&2&&typeof e.value=="string"&&e.value.toLowerCase()==="foreignobject"?null:e?.namespace??null}return this._hostTNode.namespace}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if(Ay(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let c=r[At],l=new n(c,c[Mn],c[At]);l.detach(l.indexOf(e))}}let s=this._adjustIndex(t),o=this._lContainer;return Hl(o,r,s,i),e.attachToViewContainerRef(),eh(Lh(o),s,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=Av(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=da(this._lContainer,t);i&&(Xo(Lh(this._lContainer),t),kl(i[De],i))}detach(e){let t=this._adjustIndex(e,-1),i=da(this._lContainer,t);return i&&Xo(Lh(this._lContainer),t)!=null?new Xs(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function Av(n){return n[ea]}function Lh(n){return n[ea]||(n[ea]=[])}function Q_(n,e){let t,i=e[n.index];return Gn(i)?t=i:(t=U_(i,e,null,n),e[n.index]=t,Tp(e,t)),GT(t,e,n,i),new Zh(t,n,e)}function zT(n,e){let t=n[Pt],i=t.createComment(""),r=ti(e,n),s=t.parentNode(r);return ts(t,s,i,t.nextSibling(r),!1),i}var GT=$T,WT=()=>!1;function jT(n,e,t){return WT(n,e,t)}function $T(n,e,t,i){if(n[Yr])return;let r;t.type&8?r=Kt(i):r=zT(e,t),n[Yr]=r}var Kh=class n{queryList;matches=null;constructor(e){this.queryList=e}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},Jh=class n{queries;constructor(e=[]){this.queries=e}createEmbeddedView(e){let t=e.queries;if(t!==null){let i=e.contentQueries!==null?e.contentQueries[0]:t.length,r=[];for(let s=0;s<i;s++){let o=t.getByIndex(s),a=this.queries[o.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(e){this.dirtyQueriesWithMatches(e)}detachView(e){this.dirtyQueriesWithMatches(e)}finishViewCreation(e){this.dirtyQueriesWithMatches(e)}dirtyQueriesWithMatches(e){for(let t=0;t<this.queries.length;t++)n0(e,t).matches!==null&&this.queries[t].setDirty()}},Qh=class{flags;read;predicate;constructor(e,t,i=null){this.flags=t,this.read=i,typeof e=="string"?this.predicate=QT(e):this.predicate=e}},ep=class n{queries;constructor(e=[]){this.queries=e}elementStart(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(e,t)}elementEnd(e){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(e)}embeddedTView(e){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,s=this.getByIndex(i).embeddedTView(e,r);s&&(s.indexInDeclarationView=i,t!==null?t.push(s):t=[s])}return t!==null?new n(t):null}template(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(e,t)}getByIndex(e){return this.queries[e]}get length(){return this.queries.length}track(e){this.queries.push(e)}},tp=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(e,t=-1){this.metadata=e,this._declarationNodeIndex=t}elementStart(e,t){this.isApplyingToNode(t)&&this.matchTNode(e,t)}elementEnd(e){this._declarationNodeIndex===e.index&&(this._appliesToNextNode=!1)}template(e,t){this.elementStart(e,t)}embeddedTView(e,t){return this.isApplyingToNode(e)?(this.crossesNgTemplate=!0,this.addMatch(-e.index,t),new n(this.metadata)):null}isApplyingToNode(e){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=e.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(e,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let s=i[r];this.matchTNodeWithReadOption(e,t,qT(t,s)),this.matchTNodeWithReadOption(e,t,xl(t,e,s,!1,!1))}else i===fa?t.type&4&&this.matchTNodeWithReadOption(e,t,-1):this.matchTNodeWithReadOption(e,t,xl(t,e,i,!1,!1))}matchTNodeWithReadOption(e,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===rs||r===Gl||r===yi||r===fa&&t.type&4)this.addMatch(t.index,-2);else{let s=xl(t,e,r,!1,!1);s!==null&&this.addMatch(t.index,s)}else this.addMatch(t.index,i)}}addMatch(e,t){this.matches===null?this.matches=[e,t]:this.matches.push(e,t)}};function qT(n,e){let t=n.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===e)return t[i+1]}return null}function XT(n,e){return n.type&11?Zs(n,e):n.type&4?Dp(n,e):null}function YT(n,e,t,i){return t===-1?XT(e,n):t===-2?ZT(n,e,i):bl(n,n[De],t,e)}function ZT(n,e,t){if(t===rs)return Zs(e,n);if(t===fa)return Dp(e,n);if(t===Gl)return Q_(e,n);if(t===yi)return new Gi(e,n)}function e0(n,e,t,i){let r=e[_i].queries[i];if(r.matches===null){let s=n.data,o=t.matches,a=[];for(let c=0;o!==null&&c<o.length;c+=2){let l=o[c];if(l<0)a.push(null);else{let u=s[l];a.push(YT(e,u,o[c+1],t.metadata.read))}}r.matches=a}return r.matches}function np(n,e,t,i){let r=n.queries.getByIndex(t),s=r.matches;if(s!==null){let o=e0(n,e,r,t);for(let a=0;a<s.length;a+=2){let c=s[a];if(c>0)i.push(o[a/2]);else{let l=s[a+1],u=e[-c];for(let d=Ct;d<u.length;d++){let f=u[d];f[ur]===f[At]&&np(f[De],f,l,i)}if(u[Zr]!==null){let d=u[Zr];for(let f=0;f<d.length;f++){let h=d[f];np(h[De],h,l,i)}}}}}return i}function t0(n,e){return n[_i].queries[e].queryList}function KT(n,e,t){let i=new Tl((t&4)===4);return Ny(n,e,i,i.destroy),(e[_i]??=new Jh).queries.push(new Kh(i))-1}function JT(n,e,t){let i=Wn();return i.firstCreatePass&&(ew(i,new Qh(n,e,t),-1),(e&2)===2&&(i.staticViewQueries=!0)),KT(i,ut(),e)}function QT(n){return n.split(",").map(e=>e.trim())}function ew(n,e,t){n.queries===null&&(n.queries=new ep),n.queries.track(new tp(e,t))}function n0(n,e){return n.queries.getByIndex(e)}function tw(n,e){let t=n[De],i=n0(t,e);return i.crossesNgTemplate?np(t,n,e,[]):e0(t,n,i,e)}function i0(n,e,t){let i,r=Ho(()=>{i._dirtyCounter();let s=iw(i,n);if(e&&s===void 0)throw new $e(-951,!1);return s});return i=r[_n],i._dirtyCounter=Jt(0),i._flatValue=void 0,r}function r0(n){return i0(!0,!1,n)}function s0(n){return i0(!0,!0,n)}function nw(n,e){let t=n[_n];t._lView=ut(),t._queryIndex=e,t._queryList=t0(t._lView,e),t._queryList.onDirty(()=>t._dirtyCounter.update(i=>i+1))}function iw(n,e){let t=n._lView,i=n._queryIndex;if(t===void 0||i===void 0||t[Pe]&4)return e?void 0:Ui;let r=t0(t,i),s=tw(t,i);return r.reset(s,BE),e?r.first:r._changesDetected||n._flatValue===void 0?n._flatValue=r.toArray():n._flatValue}function Rp(n){return!!n&&typeof n.then=="function"}function o0(n){return!!n&&typeof n.subscribe=="function"}var Dl=class{};var pa=class extends Dl{injector;instance=null;constructor(e){super();let t=new Wr([...e.providers,{provide:Dl,useValue:this}],e.parent||Ko(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function a0(n,e,t=null){return new pa({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}var rw=(()=>{class n{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=ih(!1,t.type),r=i.length>0?a0([i],this._injector,""):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=jt({token:n,providedIn:"environment",factory:()=>new n(lt(Qn))})}return n})();function ga(n){return Bv(()=>{let e=cw(n),t=vt(Ve({},e),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection!==fp.Eager,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:e.standalone?r=>r.get(rw).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||ri.Emulated,styles:n.styles||Ui,_:null,schemas:n.schemas||null,tView:null,id:""});e.standalone&&Ks("NgStandalone"),lw(t);let i=n.dependencies;return t.directiveDefs=Dv(i,sw),t.pipeDefs=Dv(i,dy),t.id=uw(t),t})}function sw(n){return qr(n)||qc(n)}function ow(n,e){if(n==null)return Kc;let t={};for(let i in n)if(Object.hasOwn(n,i)){let r=n[i],s,o,a,c;Array.isArray(r)?(a=r[0],s=r[1],o=r[2]??s,c=r[3]||null):(s=r,o=r,a=Bl.None,c=null),t[s]=[i,a,c],e[s]=o}return t}function aw(n){if(n==null)return Kc;let e={};for(let t in n)Object.hasOwn(n,t)&&(e[n[t]]=t);return e}function cw(n){let e={};return{type:n.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputConfig:n.inputs||Kc,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||Ui,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:ow(n.inputs,e),outputs:aw(n.outputs),debugInfo:null}}function lw(n){n.features?.forEach(e=>e(n))}function Dv(n,e){return n?()=>{let t=typeof n=="function"?n():n,i=[];for(let r of t){let s=e(r);s!==null&&i.push(s)}return i}:null}function uw(n){let e=0,t=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,t,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let s of i.join("|"))e=Math.imul(31,e)+s.charCodeAt(0)<<0;return e+=2147483648,"c"+e}var c0=new et("");var Np=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=Ze(c0,{optional:!0})??[];injector=Ze(yi);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let s=Jc(this.injector,r);if(Rp(s))t.push(s);else if(o0(s)){let o=new Promise((a,c)=>{s.subscribe({complete:a,error:c})});t.push(o)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Fl({token:n,factory:n.\u0275fac})}return n})();function dw(n,e,t,i,r,s,o,a){if(t.firstCreatePass){n.mergedAttrs=Ol(n.mergedAttrs,n.attrs);let u=n.tView=Ep(2,n,r,s,o,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,n),u.queries=t.queries.embeddedTView(n))}a&&(n.flags|=a),Hs(n,!1);let c=fw(t,e,n,i);dl()&&Mp(t,e,c,n),qs(c,e);let l=U_(c,e,c,n);e[i+$t]=l,Tp(e,l),jT(l,n,e)}function Rl(n,e,t,i,r,s,o,a,c,l,u){let d=t+$t,f;if(e.firstCreatePass){if(f=zl(e,d,4,o||null,a||null),l!=null){let h=Mi(e.consts,l);f.localNames=[];for(let g=0;g<h.length;g+=2)f.localNames.push(h[g],-1)}}else f=e.data[d];return dw(f,n,e,t,i,r,s,c),l!=null&&wp(n,f,u),f}var fw=hw;function hw(n,e,t,i){return fl(!0),e[Pt].createComment("")}var Pp=new et("");var l0=new et("");function u0(){gf(n=>{let e="";throw new $e(-600,e)})}var pw=10;var Wl=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=Ze(Qr);afterRenderManager=Ze(g_);zonelessEnabled=Ze(sa);rootEffectScheduler=Ze(gl);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new Hn;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=Ze(Gs);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(Af(t=>!t))}constructor(){Ze(ss,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=Ze(Qn);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=yi.NULL){return this._injector.get(In).run(()=>{if(ht(it.BootstrapComponentStart),!this._injector.get(Np).done){let p="";throw new $e(405,p)}let a=qr(t),c=this._injector.get(Dl),l=new ha(a,c);this.componentTypes.push(t);let{hostElement:u,directives:d,bindings:f}=mw(i),h=u||l.selector,g=l.create(r,[],h,c.injector,d,f),M=g.location.nativeElement,m=g.injector.get(Pp,null);return m?.registerApplication(M),g.onDestroy(()=>{this.detachView(g.hostView),aa(this.components,g),m?.unregisterApplication(M)}),this._loadComponent(g),ht(it.BootstrapComponentEnd,g),g})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){ht(it.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(_p.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw ht(it.ChangeDetectionEnd),new $e(101,!1);let t=we(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,we(t),this.afterTick.next(),ht(it.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(ns,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<pw;){ht(it.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{ht(it.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!ta(r))continue;let s=i&&!this.zonelessEnabled?0:1;P_(r,s),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>ta(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;aa(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(t),this._injector.get(l0,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>aa(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new $e(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Fl({token:n,factory:n.\u0275fac})}return n})();function mw(n){return n===void 0||typeof n=="string"||n instanceof Element?{hostElement:n}:n}function aa(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function mr(n,e,t,i){let r=ut(),s=zs();if(Js(r,s,e)){let o=Wn(),a=Eh();zb(a,r,n,e,t,i)}return mr}var ip=class{destroy(e){}updateValue(e,t){}swap(e,t){let i=Math.min(e,t),r=Math.max(e,t),s=this.detach(r);if(r-i>1){let o=this.detach(i);this.attach(i,s),this.attach(r,o)}else this.attach(i,s)}move(e,t){this.attach(t,this.detach(e))}};function Oh(n,e,t,i,r){return n===t&&Object.is(e,i)?1:Object.is(r(n,e),r(t,i))?-1:0}function gw(n,e,t,i){let r,s,o=0,a=n.length-1,c=void 0;if(Array.isArray(e)){we(i);let l=e.length-1;for(we(null);o<=a&&o<=l;){let u=n.at(o),d=e[o],f=Oh(o,u,o,d,t);if(f!==0){f<0&&n.updateValue(o,d),o++;continue}let h=n.at(a),g=e[l],M=Oh(a,h,l,g,t);if(M!==0){M<0&&n.updateValue(a,g),a--,l--;continue}let m=t(o,u),p=t(a,h),x=t(o,d);if(Object.is(x,p)){let w=t(l,g);Object.is(w,m)?(n.swap(o,a),n.updateValue(a,g),l--,a--):n.move(a,o),n.updateValue(o,d),o++;continue}if(r??=new Nl,s??=Nv(n,o,a,t),rp(n,r,o,x))n.updateValue(o,d),o++,a++;else if(s.has(x))r.set(m,n.detach(o)),a--;else{let w=n.create(o,e[o]);n.attach(o,w),o++,a++}}for(;o<=l;)Rv(n,r,t,o,e[o]),o++}else if(e!=null){we(i);let l=e[Symbol.iterator]();we(null);let u=l.next();for(;!u.done&&o<=a;){let d=n.at(o),f=u.value,h=Oh(o,d,o,f,t);if(h!==0)h<0&&n.updateValue(o,f),o++,u=l.next();else{r??=new Nl,s??=Nv(n,o,a,t);let g=t(o,f);if(rp(n,r,o,g))n.updateValue(o,f),o++,a++,u=l.next();else if(!s.has(g))n.attach(o,n.create(o,f)),o++,a++,u=l.next();else{let M=t(o,d);r.set(M,n.detach(o)),a--}}}for(;!u.done;)Rv(n,r,t,n.length,u.value),u=l.next()}for(;o<=a;)n.destroy(n.detach(a--));r?.forEach(l=>{n.destroy(l)})}function rp(n,e,t,i){return e!==void 0&&e.has(i)?(n.attach(t,e.get(i)),e.delete(i),!0):!1}function Rv(n,e,t,i,r){if(rp(n,e,i,t(i,r)))n.updateValue(i,r);else{let s=n.create(i,r);n.attach(i,s)}}function Nv(n,e,t,i){let r=new Set;for(let s=e;s<=t;s++)r.add(i(s,n.at(s)));return r}var Nl=class{kvMap=new Map;_vMap=void 0;has(e){return this.kvMap.has(e)}delete(e){if(!this.has(e))return!1;let t=this.kvMap.get(e);return this._vMap!==void 0&&this._vMap.has(t)?(this.kvMap.set(e,this._vMap.get(t)),this._vMap.delete(t)):this.kvMap.delete(e),!0}get(e){return this.kvMap.get(e)}set(e,t){if(this.kvMap.has(e)){let i=this.kvMap.get(e);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,t)}else this.kvMap.set(e,t)}forEach(e){for(let[t,i]of this.kvMap)if(e(i,t),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),e(i,t)}}};function Qs(n,e,t,i,r,s,o,a){Ks("NgControlFlow");let c=ut(),l=Wn(),u=Mi(l.consts,s);return Rl(c,l,n,e,t,i,r,u,256,o,a),Lp}function Lp(n,e,t,i,r,s,o,a){return Ks("NgControlFlow"),yw(n,e,t,i,r,s,o,a),Lp}function yw(n,e,t,i,r,s,o,a){let c=ut(),l=Wn(),u=Mi(l.consts,s);Rl(c,l,n,e,t,i,r,u,512,o,a)}function eo(n,e){Ks("NgControlFlow");let t=ut(),i=zs(),r=t[i]!==ji?t[i]:-1,s=r!==-1?Pl(t,$t+r):void 0,o=0;if(Js(t,i,n)){let a=we(null);try{if(s!==void 0&&V_(s,o),n!==-1){let c=$t+n,l=Pl(t,c),u=cp(t[De],c),d=z_(l,u,t),f=Vl(t,u,e,{dehydratedView:d});Hl(l,f,o,la(u,d))}}finally{we(a)}}else if(s!==void 0){let a=B_(s,o);a!==void 0&&(a[It]=e)}}var sp=class{lContainer;$implicit;$index;constructor(e,t,i){this.lContainer=e,this.$implicit=t,this.$index=i}get $count(){return this.lContainer.length-Ct}};function jl(n,e){return e}var op=class{hasEmptyBlock;trackByFn;liveCollection;constructor(e,t,i){this.hasEmptyBlock=e,this.trackByFn=t,this.liveCollection=i}};function gr(n,e,t,i,r,s,o,a,c,l,u,d,f){Ks("NgControlFlow");let h=ut(),g=Wn(),M=c!==void 0,m=ut(),p=a?o.bind(m[Cn][It]):o,x=new op(M,p);m[$t+n]=x,Rl(h,g,n+1,e,t,i,r,Mi(g.consts,s),256),M&&Rl(h,g,n+2,c,l,u,d,Mi(g.consts,f),512)}var ap=class extends ip{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(e,t,i){super(),this.lContainer=e,this.hostLView=t,this.templateTNode=i}get length(){return this.lContainer.length-Ct}at(e){return this.getLView(e)[It].$implicit}attach(e,t){let i=t[Fs];this.needsIndexUpdate||=e!==this.length,Hl(this.lContainer,t,e,la(this.templateTNode,i)),vw(this.lContainer,e)}detach(e){return this.needsIndexUpdate||=e!==this.length-1,_w(this.lContainer,e),xw(this.lContainer,e)}create(e,t){let i=Xh(this.lContainer,this.templateTNode.tView.ssrId);return Vl(this.hostLView,this.templateTNode,new sp(this.lContainer,t,e),{dehydratedView:i})}destroy(e){kl(e[De],e)}updateValue(e,t){this.getLView(e)[It].$implicit=t}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let e=0;e<this.length;e++)this.getLView(e)[It].$index=e}getLView(e){return Mw(this.lContainer,e)}};function yr(n){let e=we(null),t=hr();try{let i=ut(),r=i[De],s=i[t],o=t+1,a=Pl(i,o);if(s.liveCollection===void 0){let l=cp(r,o);s.liveCollection=new ap(a,i,l)}else s.liveCollection.reset();let c=s.liveCollection;if(gw(c,n,s.trackByFn,e),c.updateIndexes(),s.hasEmptyBlock){let l=zs(),u=c.length===0;if(Js(i,l,u)){let d=t+2,f=Pl(i,d);if(u){let h=cp(r,d),g=z_(f,h,i),M=Vl(i,h,void 0,{dehydratedView:g});Hl(f,M,0,la(h,g))}else r.firstUpdatePass&&fT(f),V_(f,0)}}}finally{we(e)}}function Pl(n,e){return n[e]}function vw(n,e){if(n.length<=Ct)return;let t=Ct+e,i=n[t];i&&hb(i)}function _w(n,e){if(n.length<=Ct)return;let t=Ct+e,i=n[t];i&&fb(i)}function xw(n,e){return da(n,e)}function Mw(n,e){return B_(n,e)}function cp(n,e){return nl(n,e)}function $l(n,e,t){let i=ut(),r=zs();if(Js(i,r,e)){let s=Wn(),o=Eh();Ob(o,i,n,e,i[Pt],t)}return $l}function Pv(n,e,t,i,r){Ip(e,n,t,r?"class":"style",i)}function Je(n,e,t,i){let r=ut(),s=r[De],o=n+$t,a=s.firstCreatePass?Y_(o,r,2,e,Hb,Oy(),t,i):s.data[o];if(fr(a)){let c=r[vi].tracingService;if(c&&c.componentCreate){let l=s.data[a.directiveStart+a.componentOffset];return c.componentCreate(j_(l),()=>(Lv(n,e,r,a,i),Je))}}return Lv(n,e,r,a,i),Je}function Lv(n,e,t,i,r){if(w_(i,t,n,e,d0),tl(i)){let s=t[De];b_(s,t,i),l_(s,i,t)}r!=null&&wp(t,i)}function tt(){let n=Wn(),e=ni(),t=I_(e);return n.firstCreatePass&&Z_(n,t),gh(t)&&yh(),mh(),t.classesWithoutHost!=null&&bE(t)&&Pv(n,t,ut(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&TE(t)&&Pv(n,t,ut(),t.stylesWithoutHost,!1),tt}function vr(n,e,t,i){return Je(n,e,t,i),tt(),vr}function Op(n,e,t,i){let r=ut(),s=r[De],o=n+$t,a=s.firstCreatePass?AT(o,s,2,e,t,i):s.data[o];return w_(a,r,n,e,d0),i!=null&&wp(r,a),Op}function Fp(){let n=ni(),e=I_(n);return gh(e)&&yh(),mh(),Fp}function ql(n,e,t,i){return Op(n,e,t,i),Fp(),ql}var d0=(n,e,t,i,r)=>(fl(!0),u_(e[Pt],i,bh()));function Ei(){return ut()}var ya="en-US";var Sw=ya;function f0(n){typeof n=="string"&&(Sw=n.toLowerCase().replace(/_/g,"-"))}function wt(n,e,t){let i=ut(),r=Wn(),s=ni();return Ew(r,i,i[Pt],s,n,e,t),wt}function Ew(n,e,t,i,r,s,o){let a=!0,c=null;if((i.type&3||o)&&(c??=Ph(i,e,s),gT(i,n,e,o,t,r,s,c)&&(a=!1)),a){let l=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let d=0;d<u.length;d+=2){let f=u[d],h=u[d+1];c??=Ph(i,e,s),bv(i,e,f,h,r,c)}if(l&&l.length)for(let d of l)c??=Ph(i,e,s),bv(i,e,d,r,r,c)}}function dt(n=1){return Zy(n)}function Xl(n,e,t,i){return nw(n,JT(e,t,i)),Xl}function kp(n=1){al($y()+n)}function to(n){let e=By();return Cy(e,$t+n)}function yl(n,e){return n<<17|e<<2}function is(n){return n>>17&32767}function bw(n){return(n&2)==2}function Tw(n,e){return n&131071|e<<17}function lp(n){return n|2}function Ys(n){return(n&131068)>>2}function Fh(n,e){return n&-131069|e<<2}function ww(n){return(n&1)===1}function up(n){return n|1}function Iw(n,e,t,i,r,s){let o=s?e.classBindings:e.styleBindings,a=is(o),c=Ys(o);n[i]=t;let l=!1,u;if(Array.isArray(t)){let d=t;u=d[1],(u===null||Ls(d,u)>0)&&(l=!0)}else u=t;if(r)if(c!==0){let f=is(n[a+1]);n[i+1]=yl(f,a),f!==0&&(n[f+1]=Fh(n[f+1],i)),n[a+1]=Tw(n[a+1],i)}else n[i+1]=yl(a,0),a!==0&&(n[a+1]=Fh(n[a+1],i)),a=i;else n[i+1]=yl(c,0),a===0?a=i:n[c+1]=Fh(n[c+1],i),c=i;l&&(n[i+1]=lp(n[i+1])),Ov(n,u,i,!0),Ov(n,u,i,!1),Cw(e,u,n,i,s),o=yl(a,c),s?e.classBindings=o:e.styleBindings=o}function Cw(n,e,t,i,r){let s=r?n.residualClasses:n.residualStyles;s!=null&&typeof e=="string"&&Ls(s,e)>=0&&(t[i+1]=up(t[i+1]))}function Ov(n,e,t,i){let r=n[t+1],s=e===null,o=i?is(r):Ys(r),a=!1;for(;o!==0&&(a===!1||s);){let c=n[o],l=n[o+1];Aw(c,e)&&(a=!0,n[o+1]=i?up(l):lp(l)),o=i?is(l):Ys(l)}a&&(n[t+1]=i?lp(r):up(r))}function Aw(n,e){return n===null||e==null||(Array.isArray(n)?n[1]:n)===e?!0:Array.isArray(n)&&typeof e=="string"?Ls(n,e)>=0:!1}function as(n,e,t){return h0(n,e,t,!1),as}function bi(n,e){return h0(n,e,null,!0),bi}function h0(n,e,t,i){let r=ut(),s=Wn(),o=Hy(2);if(s.firstUpdatePass&&Rw(s,n,o,i),e!==ji&&Js(r,o,e)){let a=s.data[hr()];Fw(s,a,r,r[Pt],n,r[o+1]=kw(e,t),i,o)}}function Dw(n,e){return e>=n.expandoStartIndex}function Rw(n,e,t,i){let r=n.data;if(r[t+1]===null){let s=r[hr()],o=Dw(n,t);Uw(s,i)&&e===null&&!o&&(e=!1),e=Nw(r,s,e,i),Iw(r,s,e,t,o,i)}}function Nw(n,e,t,i){let r=jy(n),s=i?e.residualClasses:e.residualStyles;if(r===null)(i?e.classBindings:e.styleBindings)===0&&(t=kh(null,n,e,t,i),t=ma(t,e.attrs,i),s=null);else{let o=e.directiveStylingLast;if(o===-1||n[o]!==r)if(t=kh(r,n,e,t,i),s===null){let c=Pw(n,e,i);c!==void 0&&Array.isArray(c)&&(c=kh(null,n,e,c[1],i),c=ma(c,e.attrs,i),Lw(n,e,i,c))}else s=Ow(n,e,i)}return s!==void 0&&(i?e.residualClasses=s:e.residualStyles=s),t}function Pw(n,e,t){let i=t?e.classBindings:e.styleBindings;if(Ys(i)!==0)return n[is(i)]}function Lw(n,e,t,i){let r=t?e.classBindings:e.styleBindings;n[is(r)]=i}function Ow(n,e,t){let i,r=e.directiveEnd;for(let s=1+e.directiveStylingLast;s<r;s++){let o=n[s].hostAttrs;i=ma(i,o,t)}return ma(i,e.attrs,t)}function kh(n,e,t,i,r){let s=null,o=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<o&&(s=e[a],i=ma(i,s.hostAttrs,r),s!==n);)a++;return n!==null&&(t.directiveStylingLast=a),i}function ma(n,e,t){let i=t?1:2,r=-1;if(e!==null)for(let s=0;s<e.length;s++){let o=e[s];typeof o=="number"?r=o:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),vy(n,o,t?!0:e[++s]))}return n===void 0?null:n}function Fw(n,e,t,i,r,s,o,a){if(!(e.type&3))return;let c=n.data,l=c[a+1],u=ww(l)?Fv(c,e,t,r,Ys(l),o):void 0;if(!Ll(u)){Ll(s)||bw(l)&&(s=Fv(c,null,t,r,a,o));let d=uh(hr(),t);Ib(i,o,d,r,s)}}function Fv(n,e,t,i,r,s){let o=e===null,a;for(;r>0;){let c=n[r],l=Array.isArray(c),u=l?c[1]:c,d=u===null,f=t[r+1];f===ji&&(f=d?Ui:void 0);let h=d?Zc(f,i):u===i?f:void 0;if(l&&!Ll(h)&&(h=Zc(c,i)),Ll(h)&&(a=h,o))return a;let g=n[r+1];r=o?is(g):Ys(g)}if(e!==null){let c=s?e.residualClasses:e.residualStyles;c!=null&&(a=Zc(c,i))}return a}function Ll(n){return n!==void 0}function kw(n,e){return n==null||n===""||(typeof e=="string"?n=wl(n)+e:typeof n=="object"&&(n=Gc(wl(n)))),n}function Uw(n,e){return(n.flags&(e?8:16))!==0}function Dt(n,e=""){let t=ut(),i=Wn(),r=n+$t,s=i.firstCreatePass?zl(i,r,1,e,null):i.data[r],o=Bw(i,t,s,e);t[r]=o,dl()&&Mp(i,t,o,s),Hs(s,!1)}var Bw=(n,e,t,i)=>(fl(!0),XE(e[Pt],i));function Vw(n,e,t,i=""){return Js(n,zs(),t)?e+Kf(t)+i:ji}function cs(n){return ls("",n),cs}function ls(n,e,t){let i=ut(),r=Vw(i,n,e,t);return r!==ji&&Hw(i,hr(),r),ls}function Hw(n,e,t){let i=uh(e,n);YE(n[Pt],i,t)}var p0=(()=>{class n{applicationErrorHandler=Ze(Qr);appRef=Ze(Wl);taskService=Ze(Gs);ngZone=Ze(In);zonelessEnabled=Ze(sa);tracing=Ze(ss,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new dn;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get($o):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(Ze(Ah,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?tv:Th;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get($o+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Fl({token:n,factory:n.\u0275fac})}return n})();function m0(){return[{provide:jr,useExisting:p0},{provide:In,useClass:qo},{provide:sa,useValue:!0}]}function zw(){return typeof $localize<"u"&&$localize.locale||ya}var Up=new et("",{factory:()=>Ze(Up,{optional:!0,skipSelf:!0})||zw()});function no(n,e){return Ho(n,e?.equal)}var g0=class n extends Error{_brand;constructor(e){super(e)}static IDLE=new n("IDLE");static LOADING=new n("LOADING")};function y0(n,e){return r0(e)}function Zw(n,e){return s0(e)}var x0=(y0.required=Zw,y0);var Kw=1e4;var uH=Kw-1e3;var Bp=new et(""),Jw=new et("");function va(n){return!n.moduleRef}function Qw(n){let e=va(n)?n.r3Injector:n.moduleRef.injector,t=e.get(In);return t.run(()=>{va(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(Qr),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:i})}),va(n)){let s=()=>e.destroy(),o=n.platformInjector.get(Bp);o.add(s),e.onDestroy(()=>{r.unsubscribe(),o.delete(s)})}else{let s=()=>n.moduleRef.destroy(),o=n.platformInjector.get(Bp);o.add(s),n.moduleRef.onDestroy(()=>{aa(n.allPlatformModules,n.moduleRef),r.unsubscribe(),o.delete(s)})}return tI(i,t,()=>{let s=e.get(Gs),o=s.add(),a=e.get(Np);return a.runInitializers(),a.donePromise.then(()=>{let c=e.get(Up,ya);if(f0(c||ya),!e.get(Jw,!0))return va(n)?e.get(Wl):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(va(n)){let u=e.get(Wl);return n.rootComponent!==void 0&&u.bootstrap(n.rootComponent),u}else return eI?.(n.moduleRef,n.allPlatformModules),n.moduleRef}).finally(()=>{s.remove(o)})})})}var eI;function tI(n,e,t){try{let i=t();return Rp(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n(i)),i}}var Yl=null;function nI(n=[],e){return yi.create({name:e,providers:[{provide:Zo,useValue:"platform"},{provide:Bp,useValue:new Set([()=>Yl=null])},...n]})}function iI(n=[]){if(Yl)return Yl;let e=nI(n);return Yl=e,u0(),rI(e),e}function rI(n){let e=n.get(hl,null);Jc(n,()=>{e?.forEach(t=>t())})}function M0(n){let{rootComponent:e,appProviders:t,platformProviders:i,platformRef:r}=n;ht(it.BootstrapApplicationStart);try{let s=r?.injector??iI(i),o=[m0(),rv,...t||[]],a=new pa({providers:o,parent:s,debugName:"",runEnvironmentInitializers:!1});return Qw({r3Injector:a.injector,platformInjector:s,rootComponent:e})}catch(s){return Promise.reject(s)}finally{ht(it.BootstrapApplicationEnd)}}var S0=null;function Ma(){return S0}function Vp(n){S0??=n}var xa=class{};function Hp(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,s]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()!==e)continue;let o=s;try{o=decodeURIComponent(s)}catch{}return o.length>1&&o[0]==='"'&&o[o.length-1]==='"'&&(o=o.slice(1,-1)),o}return null}var E0="browser";var Sa=class{_doc;constructor(e){this._doc=e}manager},Zl=(()=>{class n extends Sa{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,s){return t.addEventListener(i,r,s),()=>this.removeEventListener(t,i,r,s)}removeEventListener(t,i,r,s){return t.removeEventListener(i,r,s)}static \u0275fac=function(i){return new(i||n)(lt(Dn))};static \u0275prov=jt({token:n,factory:n.\u0275fac})}return n})(),Ql=new et(""),jp=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(o=>{o.manager=this});let r=t.filter(o=>!(o instanceof Zl));this._plugins=r.slice().reverse();let s=t.find(o=>o instanceof Zl);s&&this._plugins.push(s)}addEventListener(t,i,r,s){return this._findPluginFor(i).addEventListener(t,i,r,s)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(s=>s.supports(t)),!i)throw new $e(-5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||n)(lt(Ql),lt(In))};static \u0275prov=jt({token:n,factory:n.\u0275fac})}return n})(),zp="ng-app-id";function b0(n){for(let e of n)e.remove()}function T0(n,e){let t=e.createElement("style");return t.textContent=n,t}function sI(n,e,t,i){let r=n.head?.querySelectorAll(`style[${zp}="${e}"],link[${zp}="${e}"]`);if(!r||r.length===0)return!1;for(let s of r)s.removeAttribute(zp),s instanceof HTMLLinkElement?i.set(s.href.slice(s.href.lastIndexOf("/")+1),{usage:0,elements:[s]}):s.textContent&&t.set(s.textContent,{usage:0,elements:[s]});return!0}function Wp(n,e){let t=e.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",n),t}var $p=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,i,r,s={}){this.doc=t,this.appId=i,this.nonce=r,sI(t,i,this.inline,this.external)&&this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,T0);i?.forEach(r=>this.addUsage(r,this.external,Wp))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let s=i.get(t);s?s.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(o=>this.addElement(o,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(b0(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])b0(t);this.hosts.clear()}addHost(t){if(!this.hosts.has(t)){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,T0(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,Wp(i,this.doc)))}}removeHost(t){this.hosts.delete(t);for(let i of[...this.inline.values(),...this.external.values()]){let r=[];for(let s of i.elements)s.parentNode===t?s.remove():r.push(s);i.elements=r}}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),t.appendChild(i)}static \u0275fac=function(i){return new(i||n)(lt(Dn),lt(ia),lt(pl,8),lt(ra))};static \u0275prov=jt({token:n,factory:n.\u0275fac})}return n})(),Gp={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},qp=/%COMP%/g;var I0="%COMP%",oI=`_nghost-${I0}`,aI=`_ngcontent-${I0}`,cI=!0,lI=new et("",{factory:()=>cI}),uI=new et("");function dI(n){return aI.replace(qp,n)}function fI(n){return oI.replace(qp,n)}function C0(n,e){return e.map(t=>t.replace(qp,n))}var Xp=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;cssVarNamespace;constructor(t,i,r,s,o,a,c=null,l=null,u=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=s,this.doc=o,this.ngZone=a,this.nonce=c,this.tracingService=l,this.cssVarNamespace=u??"",this.defaultRenderer=new Ea(t,o,a,this.tracingService,this.cssVarNamespace)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(t,i);return r instanceof Jl?r.applyToHost(t):r instanceof ba&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,s=r.get(i.id);if(!s){let o=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,d=this.tracingService;switch(i.encapsulation){case ri.Emulated:s=new Jl(c,l,i,this.appId,u,o,a,d,this.cssVarNamespace);break;case ri.ShadowDom:return new Kl(c,t,i,o,a,this.nonce,d,this.cssVarNamespace,l);case ri.ExperimentalIsolatedShadowDom:return new Kl(c,t,i,o,a,this.nonce,d,this.cssVarNamespace);default:s=new ba(c,l,i,u,o,a,d,this.cssVarNamespace);break}r.set(i.id,s)}return s}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||n)(lt(jp),lt(os),lt(ia),lt(lI),lt(Dn),lt(In),lt(pl),lt(ss,8),lt(uI,8))};static \u0275prov=jt({token:n,factory:n.\u0275fac})}return n})(),Ea=class{eventManager;doc;ngZone;tracingService;cssVarNamespace;data=Object.create(null);throwOnSyntheticProps=!0;constructor(e,t,i,r,s=""){this.eventManager=e,this.doc=t,this.ngZone=i,this.tracingService=r,this.cssVarNamespace=s}destroy(){}destroyNode=null;createElement(e,t){return t?this.doc.createElementNS(Gp[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(w0(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){if(e){let r=w0(e)?e.content:e;if(i!=null&&i.parentNode!==r)throw new $e(-5106,!1);r.insertBefore(t,i)}}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new $e(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let s=Gp[r];s?e.setAttributeNS(s,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=Gp[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){let s=t.startsWith("--");s&&(t=t.replace("%NS%",this.cssVarNamespace)),s||r&(Wi.DashCase|Wi.Important)?e.style.setProperty(t,i,r&Wi.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){let r=t.startsWith("--");r&&(t=t.replace("%NS%",this.cssVarNamespace)),r||i&Wi.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i,r){if(typeof e=="string"&&(e=Ma().getGlobalEventTarget(this.doc,e),!e))throw new $e(-5102,!1);let s=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(s=this.tracingService.wrapEventListener(e,t,s)),this.eventManager.addEventListener(e,t,s,r)}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;e(t)===!1&&t.preventDefault()}}};function w0(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var Kl=class extends Ea{hostEl;sharedStylesHost;shadowRoot;constructor(e,t,i,r,s,o,a,c,l){super(e,r,s,a,c),this.hostEl=t,this.sharedStylesHost=l,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let u=i.styles;u=C0(i.id,u).map(f=>f.replace(/%NS%/g,c));for(let f of u){let h=document.createElement("style");o&&h.setAttribute("nonce",o),h.textContent=f,this.shadowRoot.appendChild(h)}let d=i.getExternalStyles?.();if(d)for(let f of d){let h=Wp(f,r);o&&h.setAttribute("nonce",o),this.shadowRoot.appendChild(h)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},ba=class extends Ea{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(e,t,i,r,s,o,a,c,l){super(e,s,o,a,c),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let u=i.styles,d=l?C0(l,u):u;this.styles=d.map(f=>f.replace(/%NS%/g,c)),this.styleUrls=i.getExternalStyles?.(l)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&vp.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Jl=class extends ba{contentAttr;hostAttr;constructor(e,t,i,r,s,o,a,c,l){let u=r+"-"+i.id;super(e,t,i,s,o,a,c,l,u),this.contentAttr=dI(u),this.hostAttr=fI(u)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}};var eu=class n extends xa{supportsDOMEvents=!0;static makeCurrent(){Vp(new n)}onAndCancel(e,t,i,r){return e.addEventListener(t,i,r),()=>{e.removeEventListener(t,i,r)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=hI();return t==null?null:pI(t)}resetBaseElement(){Ta=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return Hp(document.cookie,e)}},Ta=null;function hI(){return Ta=Ta||document.head.querySelector("base"),Ta?Ta.getAttribute("href"):null}function pI(n){return new URL(n,document.baseURI).pathname}var A0=["alt","control","meta","shift"],mI={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},gI={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},D0=(()=>{class n extends Sa{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r,s){let o=n.parseEventName(i),a=n.eventCallback(o.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>Ma().onAndCancel(t,o.domEventName,a,s))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let s=n._normalizeKey(i.pop()),o="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),o="code."),A0.forEach(l=>{let u=i.indexOf(l);u>-1&&(i.splice(u,1),o+=l+".")}),o+=s,i.length!=0||s.length===0)return null;let c={};return c.domEventName=r,c.fullKey=o,c}static matchEventFullKeyCode(t,i){let r=mI[t.key]||t.key,s="";return i.indexOf("code.")>-1&&(r=t.code,s="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),A0.forEach(o=>{if(o!==r){let a=gI[o];a(t)&&(s+=o+".")}}),s+=r,s===i)}static eventCallback(t,i,r){return s=>{n.matchEventFullKeyCode(s,t)&&r.runGuarded(()=>i(s))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||n)(lt(Dn))};static \u0275prov=jt({token:n,factory:n.\u0275fac})}return n})();async function Yp(n,e,t){let i=Ve({rootComponent:n},yI(e,t));return M0(i)}function yI(n,e){return{platformRef:e?.platformRef,appProviders:[...SI,...n?.providers??[]],platformProviders:MI}}function vI(){eu.makeCurrent()}function _I(){return new Hi}function xI(){return hp(document),document}var MI=[{provide:ra,useValue:E0},{provide:hl,useValue:vI,multi:!0},{provide:Dn,useFactory:xI}];var SI=[{provide:Zo,useValue:"root"},{provide:Hi,useFactory:_I},{provide:Ql,useClass:Zl,multi:!0},{provide:Ql,useClass:D0,multi:!0},Xp,{provide:os,useClass:$p},{provide:$p,useExisting:os},jp,{provide:ns,useExisting:Xp},[]];var R0={providers:[Ch()]};var us=Math.sqrt(3),Zp=[{q:1,r:0},{q:0,r:1},{q:-1,r:1},{q:-1,r:0},{q:0,r:-1},{q:1,r:-1}],jn=(n,e)=>`${n},${e}`;function _r(n,e){return{x:us*(n+e/2),z:1.5*e}}function N0(n,e){let t=e/1.5,i=n/us-t/2,r=-i-t,s=Math.round(i),o=Math.round(t),a=Math.round(r),c=Math.abs(s-i),l=Math.abs(o-t),u=Math.abs(a-r);return c>l&&c>u?s=-o-a:l>u&&(o=-s-a),{q:s,r:o}}function P0(n,e){let{q:t,r:i}=n,r=(e%6+6)%6;for(let s=0;s<r;s++)[t,i]=[-i,t+i];return{q:t,r:i}}function L0(n,e,t=0){let i=n*73856093^e*19349663^t*83492791;return()=>{i|=0,i=i+1831565813|0;let r=Math.imul(i^i>>>15,1|i);return r=r+Math.imul(r^r>>>7,61|r)^r,((r^r>>>14)>>>0)/4294967296}}var $n={planicie:{label:"Plan\xEDcie",color:"#7fa650",height:.3},floresta:{label:"Floresta",color:"#4f7f3a",height:.3},colina:{label:"Colina",color:"#9a9a55",height:.36},montanha:{label:"Montanha",color:"#8a8580",height:.4},agua:{label:"\xC1gua",color:"#3f7fb5",height:.16},estrada:{label:"Estrada",color:"#b89a68",height:.3},ruinas:{label:"Ru\xEDnas",color:"#8f9488",height:.3},vila:{label:"Vila",color:"#a8905c",height:.3}},tu=["#d9483b","#3b7dd9","#3fae5a","#e0b030","#9a55c9","#e8e4d8"],io={heroi:{label:"Her\xF3i",color:"#3b7dd9"},inimigo:{label:"Inimigo",color:"#7a1f1f"},busca:{label:"Busca",color:"#e0b030"},ameaca:{label:"Amea\xE7a",color:"#8a3fb0"},pessoa:{label:"Pessoa",color:"#2fa3a3"}};var O0=[{id:"100A",side:"A",cells:[[-1,0],[0,0],[-1,1],[0,1]]},{id:"101A",side:"A",cells:[[-1,0],[0,0],[-1,1],[0,1]]},{id:"200A",side:"A",cells:[[0,-2],[1,-2],[0,-1],[1,-1],[0,0],[0,1],[-1,1],[-1,2]]},{id:"201A",side:"A",cells:[[1,-2],[0,-1],[-1,0],[1,-1],[0,0],[-1,1],[1,0],[0,1]]},{id:"202A",side:"A",cells:[[0,-1],[1,-1],[0,0],[1,0],[-2,1],[-1,1],[-2,2],[-1,2]]},{id:"203A",side:"A",cells:[[-1,0],[-2,1],[2,-2],[1,-1],[0,0],[-1,1],[2,-1],[1,0],[0,1]]},{id:"204A",side:"A",cells:[[1,-1],[0,0],[-1,1],[-2,2],[2,-1],[1,0],[0,1],[-1,2]]},{id:"205A",side:"A",cells:[[2,-1],[1,0],[1,-1],[0,0],[0,-1],[-1,0],[-1,-1],[-2,0]]},{id:"206A",side:"A",cells:[[-2,0],[-1,0],[-2,1],[-1,1],[1,-1],[0,0],[1,0],[0,1]]},{id:"207A",side:"A",cells:[[0,-2],[-1,-1],[0,-1],[-1,0],[1,-1],[0,0],[1,0],[0,1]]},{id:"208A",side:"A",cells:[[-1,-1],[-2,0],[-1,0],[-2,1],[0,0],[1,0],[0,1],[1,1]]},{id:"209A",side:"A",cells:[[-1,-1],[-2,0],[-1,0],[1,-1],[0,0],[-1,1],[1,0],[0,1]]},{id:"300A",side:"A",cells:[[0,-2],[1,-2],[0,-1],[-1,0],[1,-1],[0,0],[-1,1],[2,-1],[1,0],[0,1],[2,0],[1,1]]},{id:"301A",side:"A",cells:[[0,-2],[-1,-1],[1,-2],[0,-1],[-1,0],[-2,1],[1,-1],[0,0],[-1,1],[2,-1],[1,0],[2,0]]},{id:"302A",side:"A",cells:[[1,-2],[0,-1],[-1,0],[-2,1],[2,-2],[1,-1],[0,0],[-1,1],[1,0],[0,1],[1,1],[0,2]]},{id:"303A",side:"A",cells:[[0,-2],[-1,-1],[-2,0],[1,-2],[0,-1],[-1,0],[-2,1],[1,-1],[0,0],[1,0],[0,1],[1,1]]},{id:"304A",side:"A",cells:[[0,-1],[-1,0],[1,-1],[0,0],[-1,1],[-2,2],[2,-1],[1,0],[0,1],[-1,2],[3,-1],[2,0]]},{id:"305A",side:"A",cells:[[1,-2],[0,-1],[-1,0],[-2,1],[1,-1],[0,0],[-1,1],[-2,2],[1,0],[0,1],[1,1],[0,2]]},{id:"306A",side:"A",cells:[[0,-2],[-1,-1],[-2,0],[1,-2],[0,-1],[-1,0],[-2,1],[1,-1],[0,0],[-1,1],[1,0],[0,1]]},{id:"307A",side:"A",cells:[[1,-2],[0,-1],[-1,0],[-2,1],[1,-1],[0,0],[-1,1],[-2,2],[1,0],[0,1],[2,0],[1,1]]},{id:"308A",side:"A",cells:[[0,-2],[-1,-1],[1,-2],[0,-1],[-1,0],[-2,1],[1,-1],[0,0],[-1,1],[-2,2],[2,-1],[1,0]]},{id:"400A",side:"A",cells:[[1,-3],[0,-2],[1,-2],[0,-1],[-1,0],[-2,1],[1,-1],[0,0],[-1,1],[-2,2],[2,-1],[1,0],[0,1],[-1,2],[1,1],[0,2]]},{id:"100B",side:"B",cells:[[-1,0],[0,0],[-1,1],[0,1]]},{id:"101B",side:"B",cells:[[-1,0],[0,0],[-1,1],[0,1]]},{id:"200B",side:"B",cells:[[-1,0],[-2,1],[1,-1],[0,0],[-1,1],[2,-1],[1,0],[2,0]]},{id:"201B",side:"B",cells:[[0,-1],[-1,0],[1,-1],[0,0],[-1,1],[2,-1],[1,0],[0,1]]},{id:"202B",side:"B",cells:[[0,-1],[1,-1],[0,0],[1,0],[-2,1],[-1,1],[-2,2],[-1,2]]},{id:"203B",side:"B",cells:[[1,-2],[0,-1],[-1,0],[2,-2],[1,-1],[0,0],[-1,1],[0,1],[-1,2]]},{id:"204B",side:"B",cells:[[2,-2],[1,-1],[0,0],[-1,1],[2,-1],[1,0],[0,1],[-1,2]]},{id:"205B",side:"B",cells:[[1,-2],[0,-1],[1,-1],[0,0],[1,0],[0,1],[1,1],[0,2]]},{id:"206B",side:"B",cells:[[0,-1],[-1,0],[1,-1],[0,0],[-1,1],[0,1],[-1,2],[0,2]]},{id:"207B",side:"B",cells:[[0,-1],[-1,0],[1,-1],[0,0],[1,0],[0,1],[2,0],[1,1]]},{id:"208B",side:"B",cells:[[-1,-1],[0,-1],[-1,0],[0,0],[0,1],[-1,2],[1,1],[0,2]]},{id:"209B",side:"B",cells:[[0,-1],[-1,0],[1,-1],[0,0],[-1,1],[0,1],[1,1],[0,2]]},{id:"300B",side:"B",cells:[[0,-2],[-1,-1],[1,-2],[0,-1],[-1,0],[1,-1],[0,0],[-1,1],[2,-1],[1,0],[0,1],[2,0]]},{id:"301B",side:"B",cells:[[0,-2],[1,-2],[0,-1],[1,-1],[0,0],[-1,1],[2,-1],[1,0],[0,1],[-1,2],[2,0],[1,1]]},{id:"302B",side:"B",cells:[[-1,-1],[-2,0],[0,-1],[-1,0],[2,-2],[1,-1],[0,0],[-1,1],[2,-1],[1,0],[0,1],[-1,2]]},{id:"303B",side:"B",cells:[[-1,-1],[0,-1],[-1,0],[1,-1],[0,0],[2,-1],[1,0],[0,1],[-1,2],[2,0],[1,1],[0,2]]},{id:"304B",side:"B",cells:[[1,-3],[0,-2],[1,-2],[0,-1],[-1,0],[-2,1],[1,-1],[0,0],[-1,1],[-2,2],[1,0],[0,1]]},{id:"305B",side:"B",cells:[[-1,-1],[-2,0],[0,-1],[-1,0],[1,-1],[0,0],[-1,1],[-2,2],[2,-1],[1,0],[0,1],[-1,2]]},{id:"306B",side:"B",cells:[[-1,-1],[-2,0],[0,-1],[-1,0],[-2,1],[1,-1],[0,0],[-1,1],[-2,2],[1,0],[0,1],[-1,2]]},{id:"307B",side:"B",cells:[[0,-2],[-1,-1],[0,-1],[-1,0],[1,-1],[0,0],[-1,1],[-2,2],[2,-1],[1,0],[0,1],[-1,2]]},{id:"308B",side:"B",cells:[[1,-2],[0,-1],[1,-1],[0,0],[-1,1],[-2,2],[2,-1],[1,0],[0,1],[-1,2],[2,0],[1,1]]},{id:"400B",side:"B",cells:[[-1,-1],[-2,0],[1,-2],[0,-1],[-1,0],[-2,1],[1,-1],[0,0],[-1,1],[-2,2],[2,-1],[1,0],[0,1],[-1,2],[3,-1],[2,0]]}];var wa=[{id:"avulso",name:"Hex\xE1gono",side:"avulso",cells:[{q:0,r:0,terrain:"planicie",space:0}]},...O0.map(n=>({id:n.id,name:n.id,side:n.side,cells:n.cells.map(([e,t],i)=>({q:e,r:t,terrain:"planicie",space:i}))}))],nu=new Map(wa.map(n=>[n.id,n]));var F0="jtm-mapa-v1",k0="jtm-presets-v1",U0={version:1,nextUid:1,tiles:[],markers:[]};function B0(n){let e=n;return!!e&&e.version===1&&Array.isArray(e.tiles)&&Array.isArray(e.markers)&&e.tiles.every(t=>nu.has(t.defId)&&Number.isInteger(t.q)&&Number.isInteger(t.r))&&e.markers.every(t=>t.kind in io&&Number.isInteger(t.q)&&Number.isInteger(t.r))}function V0(n){if(!n||typeof n!="object")return null;let e={};for(let[t,i]of Object.entries(n)){let r=nu.get(t);if(!r||!i||typeof i!="object")continue;let s={};for(let[o,a]of Object.entries(i)){let c=Number(o);Number.isInteger(c)&&c>=0&&c<r.cells.length&&typeof a=="string"&&a in $n&&(s[c]=a)}Object.keys(s).length&&(e[t]=s)}return e}function EI(){try{return V0(JSON.parse(localStorage.getItem(k0)??"null"))??{}}catch{return{}}}function bI(){try{let n=localStorage.getItem(F0),e=n?JSON.parse(n):null;if(B0(e))return e}catch{}return U0}var ro=class n{state=Jt(bI());presets=Jt(EI());mode=Jt("peca");tileId=Jt(wa[0].id);rotation=Jt(0);paintTerrain=Jt("floresta");markerKind=Jt("heroi");heroColor=Jt(tu[0]);history=[];canUndo=Jt(!1);occupied=no(()=>{let e=new Map;for(let t of this.state().tiles)for(let i of this.cellsOf(t.defId,t.q,t.r,t.rot)){let r=t.terrains?.[i.index]??i.cell.terrain;e.set(jn(i.q,i.r),{tile:t,cell:i.cell,index:i.index,terrain:r,q:i.q,r:i.r})}return e});constructor(){es(()=>{let e=this.state();try{localStorage.setItem(F0,JSON.stringify(e))}catch{}}),es(()=>{let e=this.presets();try{localStorage.setItem(k0,JSON.stringify(e))}catch{}})}savePresetsFromMap(){let e=Ve({},this.presets()),t=0;for(let i of this.state().tiles)i.terrains&&Object.keys(i.terrains).length&&(e[i.defId]=Ve({},i.terrains),t++);return t&&this.presets.set(e),t}exportPresets(){return JSON.stringify({version:1,presets:this.presets()},null,2)}importPresets(e){try{let t=JSON.parse(e),i=t?.version===1?V0(t.presets):null;return i?(this.presets.set(Ve(Ve({},this.presets()),i)),Object.keys(i).length):-1}catch{return-1}}clearPresets(){this.presets.set({})}cellsOf(e,t,i,r){let s=nu.get(e);return s?s.cells.map((o,a)=>{let c=P0(o,r);return{q:t+c.q,r:i+c.r,cell:o,index:a}}):[]}canPlace(e,t,i,r){let s=this.occupied();return this.cellsOf(e,t,i,r).every(o=>!s.has(jn(o.q,o.r)))}heightAt(e,t){let i=this.occupied().get(jn(e,t));return i?$n[i.terrain].height:0}commit(e){this.history.push(this.state()),this.history.length>100&&this.history.shift(),this.canUndo.set(!0),this.state.set(e)}applyAt(e,t){let i=this.state();switch(this.mode()){case"peca":{let r=this.tileId(),s=this.rotation();if(!this.canPlace(r,e,t,s))return!1;let o=this.presets()[r];return this.commit(vt(Ve({},i),{nextUid:i.nextUid+1,tiles:[...i.tiles,Ve({uid:i.nextUid,defId:r,q:e,r:t,rot:s},o?{terrains:Ve({},o)}:{})]})),!0}case"terreno":{let r=this.occupied().get(jn(e,t)),s=this.paintTerrain();return!r||r.terrain===s?!1:(this.commit(vt(Ve({},i),{tiles:i.tiles.map(o=>o.uid===r.tile.uid?vt(Ve({},o),{terrains:vt(Ve({},o.terrains),{[r.index]:s})}):o)})),!0)}case"marcador":{let r=this.markerKind(),s=r==="heroi"?this.heroColor():io[r].color;return this.commit(vt(Ve({},i),{nextUid:i.nextUid+1,markers:[...i.markers,{uid:i.nextUid,kind:r,q:e,r:t,color:s}]})),!0}case"apagar":{for(let s=i.markers.length-1;s>=0;s--)if(i.markers[s].q===e&&i.markers[s].r===t)return this.commit(vt(Ve({},i),{markers:i.markers.filter((o,a)=>a!==s)})),!0;let r=this.occupied().get(jn(e,t));return r?(this.commit(vt(Ve({},i),{tiles:i.tiles.filter(s=>s.uid!==r.tile.uid)})),!0):!1}}}rotateTool(e=1){this.rotation.update(t=>((t+e)%6+6)%6)}undo(){let e=this.history.pop();e&&this.state.set(e),this.canUndo.set(this.history.length>0)}clear(){let e=this.state();(e.tiles.length||e.markers.length)&&this.commit(Ve({},U0))}exportJson(){return JSON.stringify(this.state(),null,2)}importJson(e){try{let t=JSON.parse(e);return B0(t)?(this.commit(t),!0):!1}catch{return!1}}edgeKind(e,t){let i=Zp[t],r=this.occupied().get(jn(e.q+i.q,e.r+i.r));return!r||r.tile.uid!==e.tile.uid?"contorno":r.cell.space!==e.cell.space?"espaco":null}static \u0275fac=function(t){return new(t||n)};static \u0275prov=jt({token:n,factory:n.\u0275fac,providedIn:"root"})};var Yn={LEFT:0,MIDDLE:1,RIGHT:2,ROTATE:0,DOLLY:1,PAN:2},li={ROTATE:0,PAN:1,DOLLY_PAN:2,DOLLY_ROTATE:3},yx=0,wm=1,vx=2;var cc=1,od=2,Do=3,Pr=0,gn=1,Ri=2,vs=0,Ro=1,Im=2,Cm=3,Am=4,_x=5;var _s=100,xx=101,Mx=102,Sx=103,Ex=104,bx=200,Tx=201,wx=202,Ix=203,Dm=204,Rm=205,Cx=206,Ax=207,Dx=208,Rx=209,Nx=210,Px=211,Lx=212,Ox=213,Fx=214,Iu=0,Cu=1,Au=2,xo=3,Du=4,Ru=5,Nu=6,Pu=7,Nm=0,kx=1,Ux=2,Ni=0,Pm=1,Lm=2,Om=3,Fm=4,km=5,Um=6,Bm=7;var Vm=300,xs=301,No=302,ad=303,cd=304,Lu=1e3,wi=1001,Ou=1002,Xt=1003,Bx=1004;var lc=1005;var mn=1006,ld=1007;var ir=1008;var En=1009,Hm=1010,zm=1011,Po=1012,ud=1013,ui=1014,di=1015,fi=1016,dd=1017,fd=1018,Ms=1020,Gm=35902,Wm=35899,jm=1021,$m=1022,hi=1023,Ji=1026,rr=1027,qm=1028,hd=1029,Lr=1030,pd=1031;var md=1033,uc=33776,dc=33777,fc=33778,hc=33779,gd=35840,yd=35841,vd=35842,_d=35843,xd=36196,Md=37492,Sd=37496,Ed=37488,bd=37489,pc=37490,Td=37491,wd=37808,Id=37809,Cd=37810,Ad=37811,Dd=37812,Rd=37813,Nd=37814,Pd=37815,Ld=37816,Od=37817,Fd=37818,kd=37819,Ud=37820,Bd=37821,Vd=36492,Hd=36494,zd=36495,Gd=36283,Wd=36284,mc=36285,jd=36286;var Oa=2300,Fu=2301,Tu=2302,vm=2303,_m=2400,xm=2401,Mm=2402;var Vx=3200;var $d=0,Hx=1,sr="",Pn="srgb",Fa="srgb-linear",ka="linear",ot="srgb";var wu=7680;var zx=519,Gx=512,Wx=513,jx=514,Xm=515,$x=516,qx=517,Ym=518,Zm=519,Xx=35044;var Km="300 es",Xn=2e3,Mo=2001;function TI(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function wI(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}function Ua(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function Yx(){let n=Ua("canvas");return n.style.display="block",n}var H0={},So=null;function Jm(...n){let e="THREE."+n.shift();So?So("log",e,...n):console.log(e,...n)}function Zx(n){let e=n[0];if(typeof e=="string"&&e.startsWith("TSL:")){let t=n[1];t&&t.isStackTrace?n[0]+=" "+t.getLocation():n[1]='Stack trace not available. Enable "THREE.Node.captureStackTrace" to capture stack traces.'}return n}function Ie(...n){n=Zx(n);let e="THREE."+n.shift();if(So)So("warn",e,...n);else{let t=n[0];t&&t.isStackTrace?console.warn(t.getError(e)):console.warn(e,...n)}}function Ne(...n){n=Zx(n);let e="THREE."+n.shift();if(So)So("error",e,...n);else{let t=n[0];t&&t.isStackTrace?console.error(t.getError(e)):console.error(e,...n)}}function Na(...n){let e=n.join(" ");e in H0||(H0[e]=!0,Ie(...n))}function Kx(n,e,t){return new Promise(function(i,r){function s(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(s,t);break;default:i()}}setTimeout(s,t)})}var Jx={[Iu]:Iu,[Au]:Nu,[Du]:Du,[xo]:Ru,[Cu]:Cu,[Nu]:Au,[Pu]:Pu,[Ru]:xo},ci=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){let i=this._listeners;return i===void 0?!1:i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){let i=this._listeners;if(i===void 0)return;let r=i[e];if(r!==void 0){let s=r.indexOf(t);s!==-1&&r.splice(s,1)}}dispatchEvent(e){let t=this._listeners;if(t===void 0)return;let i=t[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let s=0,o=r.length;s<o;s++)r[s].call(this,e);e.target=null}}},sn=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"],z0=1234567,Pa=Math.PI/180,Eo=180/Math.PI;function Lo(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return(sn[n&255]+sn[n>>8&255]+sn[n>>16&255]+sn[n>>24&255]+"-"+sn[e&255]+sn[e>>8&255]+"-"+sn[e>>16&15|64]+sn[e>>24&255]+"-"+sn[t&63|128]+sn[t>>8&255]+"-"+sn[t>>16&255]+sn[t>>24&255]+sn[i&255]+sn[i>>8&255]+sn[i>>16&255]+sn[i>>24&255]).toLowerCase()}function je(n,e,t){return Math.max(e,Math.min(t,n))}function Qm(n,e){return(n%e+e)%e}function II(n,e,t,i,r){return i+(n-e)*(r-i)/(t-e)}function CI(n,e,t){return n!==e?(t-n)/(e-n):0}function La(n,e,t){return(1-t)*n+t*e}function AI(n,e,t,i){return La(n,e,1-Math.exp(-t*i))}function DI(n,e=1){return e-Math.abs(Qm(n,e*2)-e)}function RI(n,e,t){return n<=e?0:n>=t?1:(n=(n-e)/(t-e),n*n*(3-2*n))}function NI(n,e,t){return n<=e?0:n>=t?1:(n=(n-e)/(t-e),n*n*n*(n*(n*6-15)+10))}function PI(n,e){return n+Math.floor(Math.random()*(e-n+1))}function LI(n,e){return n+Math.random()*(e-n)}function OI(n){return n*(.5-Math.random())}function FI(n){n!==void 0&&(z0=n);let e=z0+=1831565813;return e=Math.imul(e^e>>>15,e|1),e^=e+Math.imul(e^e>>>7,e|61),((e^e>>>14)>>>0)/4294967296}function kI(n){return n*Pa}function UI(n){return n*Eo}function BI(n){return n>0&&Number.isInteger(n)&&2**Math.round(Math.log2(n))===n}function VI(n){return Math.pow(2,Math.ceil(Math.log(n)/Math.LN2))}function eg(n){return Math.pow(2,Math.floor(Math.log(n)/Math.LN2))}function HI(n,e,t,i,r){let s=Math.cos,o=Math.sin,a=s(t/2),c=o(t/2),l=s((e+i)/2),u=o((e+i)/2),d=s((e-i)/2),f=o((e-i)/2),h=s((i-e)/2),g=o((i-e)/2);switch(r){case"XYX":n.set(a*u,c*d,c*f,a*l);break;case"YZY":n.set(c*f,a*u,c*d,a*l);break;case"ZXZ":n.set(c*d,c*f,a*u,a*l);break;case"XZX":n.set(a*u,c*g,c*h,a*l);break;case"YXY":n.set(c*h,a*u,c*g,a*l);break;case"ZYZ":n.set(c*g,c*h,a*u,a*l);break;default:Ie("MathUtils: .setQuaternionFromProperEuler() encountered an unknown order: "+r)}}function tg(n){switch(n.constructor){case Uint32Array:return 4294967295;case Uint16Array:return 65535;case Uint8Array:case Uint8ClampedArray:return 255;case Int32Array:return 2147483647;case Int16Array:return 32767;case Int8Array:return 127;default:return}}function Qx(n,e){if(e.constructor===Float32Array)return n;let t=tg(e);if(t===void 0)throw new Error("THREE.MathUtils: Invalid component type.");return Math.max(n/t,-1)}function eM(n,e){if(e.constructor===Float32Array)return n;let t=tg(e);if(t===void 0)throw new Error("THREE.MathUtils: Invalid component type.");return Math.round(n*t)}var ng={DEG2RAD:Pa,RAD2DEG:Eo,generateUUID:Lo,clamp:je,euclideanModulo:Qm,mapLinear:II,inverseLerp:CI,lerp:La,damp:AI,pingpong:DI,smoothstep:RI,smootherstep:NI,randInt:PI,randFloat:LI,randFloatSpread:OI,seededRandom:FI,degToRad:kI,radToDeg:UI,isPowerOfTwo:BI,ceilPowerOfTwo:VI,floorPowerOfTwo:eg,setQuaternionFromProperEuler:HI,normalize:eM,denormalize:Qx},Te=class{static{this.prototype.isVector2=!0}constructor(e=0,t=0){this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("THREE.Vector2: index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("THREE.Vector2: index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=je(this.x,e.x,t.x),this.y=je(this.y,e.y,t.y),this}clampScalar(e,t){return this.x=je(this.x,e,t),this.y=je(this.y,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(je(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(je(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),s=this.x-e.x,o=this.y-e.y;return this.x=s*i-o*r+e.x,this.y=s*r+o*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},Ln=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,s,o,a){let c=i[r+0],l=i[r+1],u=i[r+2],d=i[r+3],f=s[o+0],h=s[o+1],g=s[o+2],M=s[o+3];if(d!==M||c!==f||l!==h||u!==g){let m=c*f+l*h+u*g+d*M;m<0&&(f=-f,h=-h,g=-g,M=-M,m=-m);let p=1-a;if(m<.9995){let x=Math.acos(m),w=Math.sin(x);p=Math.sin(p*x)/w,a=Math.sin(a*x)/w,c=c*p+f*a,l=l*p+h*a,u=u*p+g*a,d=d*p+M*a}else{c=c*p+f*a,l=l*p+h*a,u=u*p+g*a,d=d*p+M*a;let x=1/Math.sqrt(c*c+l*l+u*u+d*d);c*=x,l*=x,u*=x,d*=x}}e[t]=c,e[t+1]=l,e[t+2]=u,e[t+3]=d}static multiplyQuaternionsFlat(e,t,i,r,s,o){let a=i[r],c=i[r+1],l=i[r+2],u=i[r+3],d=s[o],f=s[o+1],h=s[o+2],g=s[o+3];return e[t]=a*g+u*d+c*h-l*f,e[t+1]=c*g+u*f+l*d-a*h,e[t+2]=l*g+u*h+a*f-c*d,e[t+3]=u*g-a*d-c*f-l*h,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,s=e._z,o=e._order,a=Math.cos,c=Math.sin,l=a(i/2),u=a(r/2),d=a(s/2),f=c(i/2),h=c(r/2),g=c(s/2);switch(o){case"XYZ":this._x=f*u*d+l*h*g,this._y=l*h*d-f*u*g,this._z=l*u*g+f*h*d,this._w=l*u*d-f*h*g;break;case"YXZ":this._x=f*u*d+l*h*g,this._y=l*h*d-f*u*g,this._z=l*u*g-f*h*d,this._w=l*u*d+f*h*g;break;case"ZXY":this._x=f*u*d-l*h*g,this._y=l*h*d+f*u*g,this._z=l*u*g+f*h*d,this._w=l*u*d-f*h*g;break;case"ZYX":this._x=f*u*d-l*h*g,this._y=l*h*d+f*u*g,this._z=l*u*g-f*h*d,this._w=l*u*d+f*h*g;break;case"YZX":this._x=f*u*d+l*h*g,this._y=l*h*d+f*u*g,this._z=l*u*g-f*h*d,this._w=l*u*d-f*h*g;break;case"XZY":this._x=f*u*d-l*h*g,this._y=l*h*d-f*u*g,this._z=l*u*g+f*h*d,this._w=l*u*d+f*h*g;break;default:Ie("Quaternion: .setFromEuler() encountered an unknown order: "+o)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],s=t[8],o=t[1],a=t[5],c=t[9],l=t[2],u=t[6],d=t[10],f=i+a+d;if(f>0){let h=.5/Math.sqrt(f+1);this._w=.25/h,this._x=(u-c)*h,this._y=(s-l)*h,this._z=(o-r)*h}else if(i>a&&i>d){let h=2*Math.sqrt(1+i-a-d);this._w=(u-c)/h,this._x=.25*h,this._y=(r+o)/h,this._z=(s+l)/h}else if(a>d){let h=2*Math.sqrt(1+a-i-d);this._w=(s-l)/h,this._x=(r+o)/h,this._y=.25*h,this._z=(c+u)/h}else{let h=2*Math.sqrt(1+d-i-a);this._w=(o-r)/h,this._x=(s+l)/h,this._y=(c+u)/h,this._z=.25*h}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<1e-8?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(je(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=t._x,c=t._y,l=t._z,u=t._w;return this._x=i*u+o*a+r*l-s*c,this._y=r*u+o*c+s*a-i*l,this._z=s*u+o*l+i*c-r*a,this._w=o*u-i*a-r*c-s*l,this._onChangeCallback(),this}slerp(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=this.dot(e);a<0&&(i=-i,r=-r,s=-s,o=-o,a=-a);let c=1-t;if(a<.9995){let l=Math.acos(a),u=Math.sin(l);c=Math.sin(c*l)/u,t=Math.sin(t*l)/u,this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this._onChangeCallback()}else this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this.normalize();return this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),s=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),s*Math.sin(t),s*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},O=class{static{this.prototype.isVector3=!0}constructor(e=0,t=0,i=0){this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("THREE.Vector3: index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("THREE.Vector3: index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(G0.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(G0.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[3]*i+s[6]*r,this.y=s[1]*t+s[4]*i+s[7]*r,this.z=s[2]*t+s[5]*i+s[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=e.elements,o=1/(s[3]*t+s[7]*i+s[11]*r+s[15]);return this.x=(s[0]*t+s[4]*i+s[8]*r+s[12])*o,this.y=(s[1]*t+s[5]*i+s[9]*r+s[13])*o,this.z=(s[2]*t+s[6]*i+s[10]*r+s[14])*o,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,s=e.x,o=e.y,a=e.z,c=e.w,l=2*(o*r-a*i),u=2*(a*t-s*r),d=2*(s*i-o*t);return this.x=t+c*l+o*d-a*u,this.y=i+c*u+a*l-s*d,this.z=r+c*d+s*u-o*l,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r,this.y=s[1]*t+s[5]*i+s[9]*r,this.z=s[2]*t+s[6]*i+s[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=je(this.x,e.x,t.x),this.y=je(this.y,e.y,t.y),this.z=je(this.z,e.z,t.z),this}clampScalar(e,t){return this.x=je(this.x,e,t),this.y=je(this.y,e,t),this.z=je(this.z,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(je(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,s=e.z,o=t.x,a=t.y,c=t.z;return this.x=r*c-s*a,this.y=s*o-i*c,this.z=i*a-r*o,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return Kp.copy(this).projectOnVector(e),this.sub(Kp)}reflect(e){return this.sub(Kp.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(je(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},Kp=new O,G0=new Ln,Le=class{static{this.prototype.isMatrix3=!0}constructor(e,t,i,r,s,o,a,c,l){this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l)}set(e,t,i,r,s,o,a,c,l){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=s,u[5]=c,u[6]=i,u[7]=o,u[8]=l,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[3],c=i[6],l=i[1],u=i[4],d=i[7],f=i[2],h=i[5],g=i[8],M=r[0],m=r[3],p=r[6],x=r[1],w=r[4],E=r[7],I=r[2],T=r[5],y=r[8];return s[0]=o*M+a*x+c*I,s[3]=o*m+a*w+c*T,s[6]=o*p+a*E+c*y,s[1]=l*M+u*x+d*I,s[4]=l*m+u*w+d*T,s[7]=l*p+u*E+d*y,s[2]=f*M+h*x+g*I,s[5]=f*m+h*w+g*T,s[8]=f*p+h*E+g*y,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8];return t*o*u-t*a*l-i*s*u+i*a*c+r*s*l-r*o*c}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=u*o-a*l,f=a*c-u*s,h=l*s-o*c,g=t*d+i*f+r*h;if(g===0)return this.set(0,0,0,0,0,0,0,0,0);let M=1/g;return e[0]=d*M,e[1]=(r*l-u*i)*M,e[2]=(a*i-r*o)*M,e[3]=f*M,e[4]=(u*t-r*c)*M,e[5]=(r*s-a*t)*M,e[6]=h*M,e[7]=(i*c-l*t)*M,e[8]=(o*t-i*s)*M,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,s,o,a){let c=Math.cos(s),l=Math.sin(s);return this.set(i*c,i*l,-i*(c*o+l*a)+o+e,-r*l,r*c,-r*(-l*o+c*a)+a+t,0,0,1),this}scale(e,t){return Na("Matrix3: .scale() is deprecated. Use .makeScale() instead."),this.premultiply(Jp.makeScale(e,t)),this}rotate(e){return Na("Matrix3: .rotate() is deprecated. Use .makeRotation() instead."),this.premultiply(Jp.makeRotation(-e)),this}translate(e,t){return Na("Matrix3: .translate() is deprecated. Use .makeTranslation() instead."),this.premultiply(Jp.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},Jp=new Le,W0=new Le().set(.4123908,.3575843,.1804808,.212639,.7151687,.0721923,.0193308,.1191948,.9505322),j0=new Le().set(3.2409699,-1.5373832,-.4986108,-.9692436,1.8759675,.0415551,.0556301,-.203977,1.0569715);function zI(){let n={enabled:!0,workingColorSpace:Fa,spaces:{},convert:function(r,s,o){return this.enabled===!1||s===o||!s||!o||(this.spaces[s].transfer===ot&&(r.r=Ki(r.r),r.g=Ki(r.g),r.b=Ki(r.b)),this.spaces[s].primaries!==this.spaces[o].primaries&&(r.applyMatrix3(this.spaces[s].toXYZ),r.applyMatrix3(this.spaces[o].fromXYZ)),this.spaces[o].transfer===ot&&(r.r=_o(r.r),r.g=_o(r.g),r.b=_o(r.b))),r},workingToColorSpace:function(r,s){return this.convert(r,this.workingColorSpace,s)},colorSpaceToWorking:function(r,s){return this.convert(r,s,this.workingColorSpace)},getPrimaries:function(r){return this.spaces[r].primaries},getTransfer:function(r){return r===sr?ka:this.spaces[r].transfer},getToneMappingMode:function(r){return this.spaces[r].outputColorSpaceConfig.toneMappingMode||"standard"},getLuminanceCoefficients:function(r,s=this.workingColorSpace){return r.fromArray(this.spaces[s].luminanceCoefficients)},define:function(r){Object.assign(this.spaces,r)},_getMatrix:function(r,s,o){return r.copy(this.spaces[o].fromXYZ).multiply(this.spaces[s].toXYZ)},_getDrawingBufferColorSpace:function(r){return this.spaces[r].outputColorSpaceConfig.drawingBufferColorSpace},_getUnpackColorSpace:function(r=this.workingColorSpace){return this.spaces[r].workingColorSpaceConfig.unpackColorSpace}},e=[.64,.33,.3,.6,.15,.06],t=[.2126,.7152,.0722],i=[.3127,.329];return n.define({[Fa]:{primaries:e,whitePoint:i,transfer:ka,toXYZ:W0,fromXYZ:j0,luminanceCoefficients:t,workingColorSpaceConfig:{unpackColorSpace:Pn},outputColorSpaceConfig:{drawingBufferColorSpace:Pn}},[Pn]:{primaries:e,whitePoint:i,transfer:ot,toXYZ:W0,fromXYZ:j0,luminanceCoefficients:t,outputColorSpaceConfig:{drawingBufferColorSpace:Pn}}}),n}var Ke=zI();function Ki(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function _o(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var so,ku=class{static getDataURL(e,t="image/png"){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let i;if(e instanceof HTMLCanvasElement)i=e;else{so===void 0&&(so=Ua("canvas")),so.width=e.width,so.height=e.height;let r=so.getContext("2d");e instanceof ImageData?r.putImageData(e,0,0):r.drawImage(e,0,0,e.width,e.height),i=so}return i.toDataURL(t)}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=Ua("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),s=r.data;for(let o=0;o<s.length;o++)s[o]=Ki(s[o]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(Ki(t[i]/255)*255):t[i]=Ki(t[i]);return{data:t,width:e.width,height:e.height}}else return Ie("ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},GI=0,bo=class{constructor(e=null){this.isTextureSource=!0,Object.defineProperty(this,"id",{value:GI++}),this.uuid=Lo(),this.data=e,this.dataReady=!0,this.version=0}getSize(e){let t=this.data;return typeof HTMLVideoElement<"u"&&t instanceof HTMLVideoElement?e.set(t.videoWidth,t.videoHeight,0):typeof VideoFrame<"u"&&t instanceof VideoFrame?e.set(t.displayWidth,t.displayHeight,0):t!==null?e.set(t.width||0,t.height||0,t.depth||0):e.set(0,0,0),e}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let s;if(Array.isArray(r)){s=[];for(let o=0,a=r.length;o<a;o++)r[o].isDataTexture?s.push(Qp(r[o].image)):s.push(Qp(r[o]))}else s=Qp(r);i.url=s}return t||(e.images[this.uuid]=i),i}};function Qp(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?ku.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(Ie("Texture: Unable to serialize Texture."),{})}var WI=0,em=new O,Sn=class n extends ci{constructor(e=n.DEFAULT_IMAGE,t=n.DEFAULT_MAPPING,i=wi,r=wi,s=mn,o=ir,a=hi,c=En,l=n.DEFAULT_ANISOTROPY,u=sr){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:WI++}),this.uuid=Lo(),this.name="",this.source=new bo(e),this.mipmaps=[],this.mapping=t,this.channel=0,this.wrapS=i,this.wrapT=r,this.magFilter=s,this.minFilter=o,this.anisotropy=l,this.format=a,this.internalFormat=null,this.type=c,this.offset=new Te(0,0),this.repeat=new Te(1,1),this.center=new Te(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new Le,this.generateMipmaps=!0,this.mipmapsAutoUpdate=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=u,this.userData={},this.updateRanges=[],this.version=0,this.onUpdate=null,this.renderTarget=null,this.isRenderTargetTexture=!1,this.isArrayTexture=!!(e&&e.depth&&e.depth>1),this.pmremVersion=0,this.isPMREMTexture=!1,this.normalized=!1}get width(){return this.source.getSize(em).x}get height(){return this.source.getSize(em).y}get depth(){return this.source.getSize(em).z}get image(){return this.source.data}set image(e){this.source.data=e}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}clone(){return new this.constructor().copy(this)}copy(e){return this.name=e.name,this.source=e.source,this.mipmaps=e.mipmaps.slice(0),this.mapping=e.mapping,this.channel=e.channel,this.wrapS=e.wrapS,this.wrapT=e.wrapT,this.magFilter=e.magFilter,this.minFilter=e.minFilter,this.anisotropy=e.anisotropy,this.format=e.format,this.internalFormat=e.internalFormat,this.type=e.type,this.normalized=e.normalized,this.offset.copy(e.offset),this.repeat.copy(e.repeat),this.center.copy(e.center),this.rotation=e.rotation,this.matrixAutoUpdate=e.matrixAutoUpdate,this.matrix.copy(e.matrix),this.generateMipmaps=e.generateMipmaps,this.mipmapsAutoUpdate=e.mipmapsAutoUpdate,this.premultiplyAlpha=e.premultiplyAlpha,this.flipY=e.flipY,this.unpackAlignment=e.unpackAlignment,this.colorSpace=e.colorSpace,this.renderTarget=e.renderTarget,this.isRenderTargetTexture=e.isRenderTargetTexture,this.isPMREMTexture=e.isPMREMTexture,this.isArrayTexture=e.isArrayTexture,this.userData=JSON.parse(JSON.stringify(e.userData)),this.needsUpdate=!0,this}setValues(e){for(let t in e){let i=e[t];if(i===void 0){Ie(`Texture.setValues(): parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){Ie(`Texture.setValues(): property '${t}' does not exist.`);continue}r&&i&&r.isVector2&&i.isVector2||r&&i&&r.isVector3&&i.isVector3||r&&i&&r.isMatrix3&&i.isMatrix3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.textures[this.uuid]!==void 0)return e.textures[this.uuid];let i={metadata:{version:4.7,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(e).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,normalized:this.normalized,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,mipmapsAutoUpdate:this.mipmapsAutoUpdate,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(i.userData=this.userData),t||(e.textures[this.uuid]=i),i}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(e){if(this.mapping!==Vm)return e;if(e.applyMatrix3(this.matrix),e.x<0||e.x>1)switch(this.wrapS){case Lu:e.x=e.x-Math.floor(e.x);break;case wi:e.x=e.x<0?0:1;break;case Ou:Math.abs(Math.floor(e.x)%2)===1?e.x=Math.ceil(e.x)-e.x:e.x=e.x-Math.floor(e.x);break}if(e.y<0||e.y>1)switch(this.wrapT){case Lu:e.y=e.y-Math.floor(e.y);break;case wi:e.y=e.y<0?0:1;break;case Ou:Math.abs(Math.floor(e.y)%2)===1?e.y=Math.ceil(e.y)-e.y:e.y=e.y-Math.floor(e.y);break}return this.flipY&&(e.y=1-e.y),e}set needsUpdate(e){e===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(e){e===!0&&this.pmremVersion++}};Sn.DEFAULT_IMAGE=null;Sn.DEFAULT_MAPPING=Vm;Sn.DEFAULT_ANISOTROPY=1;var Et=class{static{this.prototype.isVector4=!0}constructor(e=0,t=0,i=0,r=1){this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("THREE.Vector4: index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("THREE.Vector4: index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=this.w,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r+o[12]*s,this.y=o[1]*t+o[5]*i+o[9]*r+o[13]*s,this.z=o[2]*t+o[6]*i+o[10]*r+o[14]*s,this.w=o[3]*t+o[7]*i+o[11]*r+o[15]*s,this}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this.w/=e.w,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,s,c=e.elements,l=c[0],u=c[4],d=c[8],f=c[1],h=c[5],g=c[9],M=c[2],m=c[6],p=c[10];if(Math.abs(u-f)<.01&&Math.abs(d-M)<.01&&Math.abs(g-m)<.01){if(Math.abs(u+f)<.1&&Math.abs(d+M)<.1&&Math.abs(g+m)<.1&&Math.abs(l+h+p-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let w=(l+1)/2,E=(h+1)/2,I=(p+1)/2,T=(u+f)/4,y=(d+M)/4,S=(g+m)/4;return w>E&&w>I?w<.01?(i=0,r=.707106781,s=.707106781):(i=Math.sqrt(w),r=T/i,s=y/i):E>I?E<.01?(i=.707106781,r=0,s=.707106781):(r=Math.sqrt(E),i=T/r,s=S/r):I<.01?(i=.707106781,r=.707106781,s=0):(s=Math.sqrt(I),i=y/s,r=S/s),this.set(i,r,s,t),this}let x=Math.sqrt((m-g)*(m-g)+(d-M)*(d-M)+(f-u)*(f-u));return Math.abs(x)<.001&&(x=1),this.x=(m-g)/x,this.y=(d-M)/x,this.z=(f-u)/x,this.w=Math.acos((l+h+p-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=je(this.x,e.x,t.x),this.y=je(this.y,e.y,t.y),this.z=je(this.z,e.z,t.z),this.w=je(this.w,e.w,t.w),this}clampScalar(e,t){return this.x=je(this.x,e,t),this.y=je(this.y,e,t),this.z=je(this.z,e,t),this.w=je(this.w,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(je(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},Uu=class extends ci{constructor(e=1,t=1,i={}){super(),i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:mn,depthBuffer:!0,stencilBuffer:!1,resolveColorBuffer:!0,resolveDepthBuffer:!0,resolveStencilBuffer:!0,storeMultisampledColorBuffer:!0,storeMultisampledDepthBuffer:!0,storeMultisampledStencilBuffer:!0,depthTexture:null,samples:0,count:1,depth:1,multiview:!1,useArrayDepthTexture:!1},i),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=i.depth,this.scissor=new Et(0,0,e,t),this.scissorTest=!1,this.viewport=new Et(0,0,e,t),this.textures=[];let r=i.count;for(let s=0;s<r;s++)this.textures[s]=new Sn({width:e,height:t,depth:i.depth}),this.textures[s].isRenderTargetTexture=!0,this.textures[s].renderTarget=this;this._setTextureOptions(i),this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveColorBuffer=i.resolveColorBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this.storeMultisampledColorBuffer=i.storeMultisampledColorBuffer,this.storeMultisampledDepthBuffer=i.storeMultisampledDepthBuffer,this.storeMultisampledStencilBuffer=i.storeMultisampledStencilBuffer,this._depthTexture=null,this.depthTexture=i.depthTexture,this.samples=i.samples,this.multiview=i.multiview,this.useArrayDepthTexture=i.useArrayDepthTexture}_setTextureOptions(e={}){let t={minFilter:mn,generateMipmaps:!1,flipY:!1,internalFormat:null};e.mapping!==void 0&&(t.mapping=e.mapping),e.wrapS!==void 0&&(t.wrapS=e.wrapS),e.wrapT!==void 0&&(t.wrapT=e.wrapT),e.wrapR!==void 0&&(t.wrapR=e.wrapR),e.magFilter!==void 0&&(t.magFilter=e.magFilter),e.minFilter!==void 0&&(t.minFilter=e.minFilter),e.format!==void 0&&(t.format=e.format),e.type!==void 0&&(t.type=e.type),e.anisotropy!==void 0&&(t.anisotropy=e.anisotropy),e.colorSpace!==void 0&&(t.colorSpace=e.colorSpace),e.flipY!==void 0&&(t.flipY=e.flipY),e.generateMipmaps!==void 0&&(t.generateMipmaps=e.generateMipmaps),e.mipmapsAutoUpdate!==void 0&&(t.mipmapsAutoUpdate=e.mipmapsAutoUpdate),e.internalFormat!==void 0&&(t.internalFormat=e.internalFormat);for(let i=0;i<this.textures.length;i++)this.textures[i].setValues(t)}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}set depthTexture(e){this._depthTexture!==null&&this._depthTexture.renderTarget===this&&(this._depthTexture.renderTarget=null),e!==null&&e.renderTarget===null&&(e.renderTarget=this),this._depthTexture=e}get depthTexture(){return this._depthTexture}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,s=this.textures.length;r<s;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i,this.textures[r].isData3DTexture!==!0&&(this.textures[r].isArrayTexture=this.textures[r].image.depth>1);this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let t=0,i=e.textures.length;t<i;t++){this.textures[t]=e.textures[t].clone(),this.textures[t].isRenderTargetTexture=!0,this.textures[t].renderTarget=this;let r=Object.assign({},e.textures[t].image);this.textures[t].source=new bo(r)}if(this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveColorBuffer=e.resolveColorBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,this.storeMultisampledColorBuffer=e.storeMultisampledColorBuffer,this.storeMultisampledDepthBuffer=e.storeMultisampledDepthBuffer,this.storeMultisampledStencilBuffer=e.storeMultisampledStencilBuffer,e.depthTexture!==null)if(e.depthTexture.renderTarget===e){let t=e.depthTexture.clone();t.renderTarget=null,this.depthTexture=t}else this.depthTexture=e.depthTexture;else this.depthTexture=null;return this.samples=e.samples,this.multiview=e.multiview,this.useArrayDepthTexture=e.useArrayDepthTexture,this}dispose(){this.dispatchEvent({type:"dispose"})}},On=class extends Uu{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},Ba=class extends Sn{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=Xt,this.minFilter=Xt,this.wrapR=wi,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}copy(e){return super.copy(e),this.wrapR=e.wrapR,this}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var Bu=class extends Sn{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=Xt,this.minFilter=Xt,this.wrapR=wi,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}copy(e){return super.copy(e),this.wrapR=e.wrapR,this}};var _t=class n{static{this.prototype.isMatrix4=!0}constructor(e,t,i,r,s,o,a,c,l,u,d,f,h,g,M,m){this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l,u,d,f,h,g,M,m)}set(e,t,i,r,s,o,a,c,l,u,d,f,h,g,M,m){let p=this.elements;return p[0]=e,p[4]=t,p[8]=i,p[12]=r,p[1]=s,p[5]=o,p[9]=a,p[13]=c,p[2]=l,p[6]=u,p[10]=d,p[14]=f,p[3]=h,p[7]=g,p[11]=M,p[15]=m,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return this.determinantAffine()===0?(e.set(1,0,0),t.set(0,1,0),i.set(0,0,1),this):(e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this)}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){if(e.determinantAffine()===0)return this.identity();let t=this.elements,i=e.elements,r=1/oo.setFromMatrixColumn(e,0).length(),s=1/oo.setFromMatrixColumn(e,1).length(),o=1/oo.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*s,t[5]=i[5]*s,t[6]=i[6]*s,t[7]=0,t[8]=i[8]*o,t[9]=i[9]*o,t[10]=i[10]*o,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,s=e.z,o=Math.cos(i),a=Math.sin(i),c=Math.cos(r),l=Math.sin(r),u=Math.cos(s),d=Math.sin(s);if(e.order==="XYZ"){let f=o*u,h=o*d,g=a*u,M=a*d;t[0]=c*u,t[4]=-c*d,t[8]=l,t[1]=h+g*l,t[5]=f-M*l,t[9]=-a*c,t[2]=M-f*l,t[6]=g+h*l,t[10]=o*c}else if(e.order==="YXZ"){let f=c*u,h=c*d,g=l*u,M=l*d;t[0]=f+M*a,t[4]=g*a-h,t[8]=o*l,t[1]=o*d,t[5]=o*u,t[9]=-a,t[2]=h*a-g,t[6]=M+f*a,t[10]=o*c}else if(e.order==="ZXY"){let f=c*u,h=c*d,g=l*u,M=l*d;t[0]=f-M*a,t[4]=-o*d,t[8]=g+h*a,t[1]=h+g*a,t[5]=o*u,t[9]=M-f*a,t[2]=-o*l,t[6]=a,t[10]=o*c}else if(e.order==="ZYX"){let f=o*u,h=o*d,g=a*u,M=a*d;t[0]=c*u,t[4]=g*l-h,t[8]=f*l+M,t[1]=c*d,t[5]=M*l+f,t[9]=h*l-g,t[2]=-l,t[6]=a*c,t[10]=o*c}else if(e.order==="YZX"){let f=o*c,h=o*l,g=a*c,M=a*l;t[0]=c*u,t[4]=M-f*d,t[8]=g*d+h,t[1]=d,t[5]=o*u,t[9]=-a*u,t[2]=-l*u,t[6]=h*d+g,t[10]=f-M*d}else if(e.order==="XZY"){let f=o*c,h=o*l,g=a*c,M=a*l;t[0]=c*u,t[4]=-d,t[8]=l*u,t[1]=f*d+M,t[5]=o*u,t[9]=h*d-g,t[2]=g*d-h,t[6]=a*u,t[10]=M*d+f}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose(jI,e,$I)}lookAt(e,t,i){let r=this.elements;return Rn.subVectors(e,t),Rn.lengthSq()===0&&(Rn.z=1),Rn.normalize(),xr.crossVectors(i,Rn),xr.lengthSq()===0&&(Math.abs(i.z)===1?Rn.x+=1e-4:Rn.z+=1e-4,Rn.normalize(),xr.crossVectors(i,Rn)),xr.normalize(),iu.crossVectors(Rn,xr),r[0]=xr.x,r[4]=iu.x,r[8]=Rn.x,r[1]=xr.y,r[5]=iu.y,r[9]=Rn.y,r[2]=xr.z,r[6]=iu.z,r[10]=Rn.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[4],c=i[8],l=i[12],u=i[1],d=i[5],f=i[9],h=i[13],g=i[2],M=i[6],m=i[10],p=i[14],x=i[3],w=i[7],E=i[11],I=i[15],T=r[0],y=r[4],S=r[8],N=r[12],A=r[1],L=r[5],F=r[9],G=r[13],D=r[2],z=r[6],$=r[10],H=r[14],te=r[3],j=r[7],J=r[11],Q=r[15];return s[0]=o*T+a*A+c*D+l*te,s[4]=o*y+a*L+c*z+l*j,s[8]=o*S+a*F+c*$+l*J,s[12]=o*N+a*G+c*H+l*Q,s[1]=u*T+d*A+f*D+h*te,s[5]=u*y+d*L+f*z+h*j,s[9]=u*S+d*F+f*$+h*J,s[13]=u*N+d*G+f*H+h*Q,s[2]=g*T+M*A+m*D+p*te,s[6]=g*y+M*L+m*z+p*j,s[10]=g*S+M*F+m*$+p*J,s[14]=g*N+M*G+m*H+p*Q,s[3]=x*T+w*A+E*D+I*te,s[7]=x*y+w*L+E*z+I*j,s[11]=x*S+w*F+E*$+I*J,s[15]=x*N+w*G+E*H+I*Q,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[12],o=e[1],a=e[5],c=e[9],l=e[13],u=e[2],d=e[6],f=e[10],h=e[14],g=e[3],M=e[7],m=e[11],p=e[15],x=c*h-l*f,w=a*h-l*d,E=a*f-c*d,I=o*h-l*u,T=o*f-c*u,y=o*d-a*u;return t*(M*x-m*w+p*E)-i*(g*x-m*I+p*T)+r*(g*w-M*I+p*y)-s*(g*E-M*T+m*y)}determinantAffine(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[1],o=e[5],a=e[9],c=e[2],l=e[6],u=e[10];return t*(o*u-a*l)-i*(s*u-a*c)+r*(s*l-o*c)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=e[9],f=e[10],h=e[11],g=e[12],M=e[13],m=e[14],p=e[15],x=t*a-i*o,w=t*c-r*o,E=t*l-s*o,I=i*c-r*a,T=i*l-s*a,y=r*l-s*c,S=u*M-d*g,N=u*m-f*g,A=u*p-h*g,L=d*m-f*M,F=d*p-h*M,G=f*p-h*m,D=x*G-w*F+E*L+I*A-T*N+y*S;if(D===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let z=1/D;return e[0]=(a*G-c*F+l*L)*z,e[1]=(r*F-i*G-s*L)*z,e[2]=(M*y-m*T+p*I)*z,e[3]=(f*T-d*y-h*I)*z,e[4]=(c*A-o*G-l*N)*z,e[5]=(t*G-r*A+s*N)*z,e[6]=(m*E-g*y-p*w)*z,e[7]=(u*y-f*E+h*w)*z,e[8]=(o*F-a*A+l*S)*z,e[9]=(i*A-t*F-s*S)*z,e[10]=(g*T-M*E+p*x)*z,e[11]=(d*E-u*T-h*x)*z,e[12]=(a*N-o*L-c*S)*z,e[13]=(t*L-i*N+r*S)*z,e[14]=(M*w-g*I-m*x)*z,e[15]=(u*I-d*w+f*x)*z,this}scale(e){let t=this.elements,i=e.x,r=e.y,s=e.z;return t[0]*=i,t[4]*=r,t[8]*=s,t[1]*=i,t[5]*=r,t[9]*=s,t[2]*=i,t[6]*=r,t[10]*=s,t[3]*=i,t[7]*=r,t[11]*=s,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),s=1-i,o=e.x,a=e.y,c=e.z,l=s*o,u=s*a;return this.set(l*o+i,l*a-r*c,l*c+r*a,0,l*a+r*c,u*a+i,u*c-r*o,0,l*c-r*a,u*c+r*o,s*c*c+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,s,o){return this.set(1,i,s,0,e,1,o,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,s=t._x,o=t._y,a=t._z,c=t._w,l=s+s,u=o+o,d=a+a,f=s*l,h=s*u,g=s*d,M=o*u,m=o*d,p=a*d,x=c*l,w=c*u,E=c*d,I=i.x,T=i.y,y=i.z;return r[0]=(1-(M+p))*I,r[1]=(h+E)*I,r[2]=(g-w)*I,r[3]=0,r[4]=(h-E)*T,r[5]=(1-(f+p))*T,r[6]=(m+x)*T,r[7]=0,r[8]=(g+w)*y,r[9]=(m-x)*y,r[10]=(1-(f+M))*y,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements;e.x=r[12],e.y=r[13],e.z=r[14];let s=this.determinantAffine();if(s===0)return i.set(1,1,1),t.identity(),this;let o=oo.set(r[0],r[1],r[2]).length(),a=oo.set(r[4],r[5],r[6]).length(),c=oo.set(r[8],r[9],r[10]).length();s<0&&(o=-o),si.copy(this);let l=1/o,u=1/a,d=1/c;return si.elements[0]*=l,si.elements[1]*=l,si.elements[2]*=l,si.elements[4]*=u,si.elements[5]*=u,si.elements[6]*=u,si.elements[8]*=d,si.elements[9]*=d,si.elements[10]*=d,t.setFromRotationMatrix(si),i.x=o,i.y=a,i.z=c,this}makePerspective(e,t,i,r,s,o,a=Xn,c=!1){let l=this.elements,u=2*s/(t-e),d=2*s/(i-r),f=(t+e)/(t-e),h=(i+r)/(i-r),g,M;if(c)g=s/(o-s),M=o*s/(o-s);else if(a===Xn)g=-(o+s)/(o-s),M=-2*o*s/(o-s);else if(a===Mo)g=-o/(o-s),M=-o*s/(o-s);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=f,l[12]=0,l[1]=0,l[5]=d,l[9]=h,l[13]=0,l[2]=0,l[6]=0,l[10]=g,l[14]=M,l[3]=0,l[7]=0,l[11]=-1,l[15]=0,this}makeOrthographic(e,t,i,r,s,o,a=Xn,c=!1){let l=this.elements,u=2/(t-e),d=2/(i-r),f=-(t+e)/(t-e),h=-(i+r)/(i-r),g,M;if(c)g=1/(o-s),M=o/(o-s);else if(a===Xn)g=-2/(o-s),M=-(o+s)/(o-s);else if(a===Mo)g=-1/(o-s),M=-s/(o-s);else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=0,l[12]=f,l[1]=0,l[5]=d,l[9]=0,l[13]=h,l[2]=0,l[6]=0,l[10]=g,l[14]=M,l[3]=0,l[7]=0,l[11]=0,l[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},oo=new O,si=new _t,jI=new O(0,0,0),$I=new O(1,1,1),xr=new O,iu=new O,Rn=new O,$0=new _t,q0=new Ln,Qi=class n{constructor(e=0,t=0,i=0,r=n.DEFAULT_ORDER){this.isEuler=!0,this._x=e,this._y=t,this._z=i,this._order=r}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get order(){return this._order}set order(e){this._order=e,this._onChangeCallback()}set(e,t,i,r=this._order){return this._x=e,this._y=t,this._z=i,this._order=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(e){return this._x=e._x,this._y=e._y,this._z=e._z,this._order=e._order,this._onChangeCallback(),this}setFromRotationMatrix(e,t=this._order,i=!0){let r=e.elements,s=r[0],o=r[4],a=r[8],c=r[1],l=r[5],u=r[9],d=r[2],f=r[6],h=r[10];switch(t){case"XYZ":this._y=Math.asin(je(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(-u,h),this._z=Math.atan2(-o,s)):(this._x=Math.atan2(f,l),this._z=0);break;case"YXZ":this._x=Math.asin(-je(u,-1,1)),Math.abs(u)<.9999999?(this._y=Math.atan2(a,h),this._z=Math.atan2(c,l)):(this._y=Math.atan2(-d,s),this._z=0);break;case"ZXY":this._x=Math.asin(je(f,-1,1)),Math.abs(f)<.9999999?(this._y=Math.atan2(-d,h),this._z=Math.atan2(-o,l)):(this._y=0,this._z=Math.atan2(c,s));break;case"ZYX":this._y=Math.asin(-je(d,-1,1)),Math.abs(d)<.9999999?(this._x=Math.atan2(f,h),this._z=Math.atan2(c,s)):(this._x=0,this._z=Math.atan2(-o,l));break;case"YZX":this._z=Math.asin(je(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-u,l),this._y=Math.atan2(-d,s)):(this._x=0,this._y=Math.atan2(a,h));break;case"XZY":this._z=Math.asin(-je(o,-1,1)),Math.abs(o)<.9999999?(this._x=Math.atan2(f,l),this._y=Math.atan2(a,s)):(this._x=Math.atan2(-u,h),this._y=0);break;default:Ie("Euler: .setFromRotationMatrix() encountered an unknown order: "+t)}return this._order=t,i===!0&&this._onChangeCallback(),this}setFromQuaternion(e,t,i){return $0.makeRotationFromQuaternion(e),this.setFromRotationMatrix($0,t,i)}setFromVector3(e,t=this._order){return this.set(e.x,e.y,e.z,t)}reorder(e){return q0.setFromEuler(this),this.setFromQuaternion(q0,e)}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._order===this._order}fromArray(e){return this._x=e[0],this._y=e[1],this._z=e[2],e[3]!==void 0&&(this._order=e[3]),this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._order,e}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}};Qi.DEFAULT_ORDER="XYZ";var To=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},qI=0,X0=new O,ao=new Ln,$i=new _t,ru=new O,Ia=new O,XI=new O,YI=new Ln,Y0=new O(1,0,0),Z0=new O(0,1,0),K0=new O(0,0,1),J0={type:"added"},ZI={type:"removed"},co={type:"childadded",child:null},tm={type:"childremoved",child:null},Qt=class n extends ci{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:qI++}),this.uuid=Lo(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let e=new O,t=new Qi,i=new Ln,r=new O(1,1,1);function s(){i.setFromEuler(t,!1)}function o(){t.setFromQuaternion(i,void 0,!1)}t._onChange(s),i._onChange(o),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:e},rotation:{configurable:!0,enumerable:!0,value:t},quaternion:{configurable:!0,enumerable:!0,value:i},scale:{configurable:!0,enumerable:!0,value:r},modelViewMatrix:{value:new _t},normalMatrix:{value:new Le}}),this.matrix=new _t,this.matrixWorld=new _t,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new To,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.customDepthMaterial=void 0,this.customDistanceMaterial=void 0,this.static=!1,this.userData={},this.pivot=null}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(e){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(e),this.matrix.decompose(this.position,this.quaternion,this.scale),this.pivot!==null&&this.position.copy(this.pivot).applyMatrix4(this.matrix).sub(this.pivot)}applyQuaternion(e){return this.quaternion.premultiply(e),this}setRotationFromAxisAngle(e,t){this.quaternion.setFromAxisAngle(e,t)}setRotationFromEuler(e){this.quaternion.setFromEuler(e,!0)}setRotationFromMatrix(e){this.quaternion.setFromRotationMatrix(e)}setRotationFromQuaternion(e){this.quaternion.copy(e)}rotateOnAxis(e,t){return ao.setFromAxisAngle(e,t),this.quaternion.multiply(ao),this}rotateOnWorldAxis(e,t){return ao.setFromAxisAngle(e,t),this.quaternion.premultiply(ao),this}rotateX(e){return this.rotateOnAxis(Y0,e)}rotateY(e){return this.rotateOnAxis(Z0,e)}rotateZ(e){return this.rotateOnAxis(K0,e)}translateOnAxis(e,t){return X0.copy(e).applyQuaternion(this.quaternion),this.position.add(X0.multiplyScalar(t)),this}translateX(e){return this.translateOnAxis(Y0,e)}translateY(e){return this.translateOnAxis(Z0,e)}translateZ(e){return this.translateOnAxis(K0,e)}localToWorld(e){return this.updateWorldMatrix(!0,!1),e.applyMatrix4(this.matrixWorld)}worldToLocal(e){return this.updateWorldMatrix(!0,!1),e.applyMatrix4($i.copy(this.matrixWorld).invert())}lookAt(e,t,i){e.isVector3?ru.copy(e):ru.set(e,t,i);let r=this.parent;this.updateWorldMatrix(!0,!1),Ia.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?$i.lookAt(Ia,ru,this.up):$i.lookAt(ru,Ia,this.up),this.quaternion.setFromRotationMatrix($i),r&&($i.extractRotation(r.matrixWorld),ao.setFromRotationMatrix($i),this.quaternion.premultiply(ao.invert()))}add(e){if(arguments.length>1){for(let t=0;t<arguments.length;t++)this.add(arguments[t]);return this}return e===this?(Ne("Object3D.add: object can't be added as a child of itself.",e),this):(e&&e.isObject3D?(e.removeFromParent(),e.parent=this,this.children.push(e),e.dispatchEvent(J0),co.child=e,this.dispatchEvent(co),co.child=null):Ne("Object3D.add: object not an instance of THREE.Object3D.",e),this)}remove(e){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.remove(arguments[i]);return this}let t=this.children.indexOf(e);return t!==-1&&(e.parent=null,this.children.splice(t,1),e.dispatchEvent(ZI),tm.child=e,this.dispatchEvent(tm),tm.child=null),this}removeFromParent(){let e=this.parent;return e!==null&&e.remove(this),this}clear(){return this.remove(...this.children)}attach(e){return this.updateWorldMatrix(!0,!1),$i.copy(this.matrixWorld).invert(),e.parent!==null&&(e.parent.updateWorldMatrix(!0,!1),$i.multiply(e.parent.matrixWorld)),e.applyMatrix4($i),e.removeFromParent(),e.parent=this,this.children.push(e),e.updateWorldMatrix(!1,!0),e.dispatchEvent(J0),co.child=e,this.dispatchEvent(co),co.child=null,this}getObjectById(e){return this.getObjectByProperty("id",e)}getObjectByName(e){return this.getObjectByProperty("name",e)}getObjectByProperty(e,t){if(this[e]===t)return this;for(let i=0,r=this.children.length;i<r;i++){let o=this.children[i].getObjectByProperty(e,t);if(o!==void 0)return o}}getObjectsByProperty(e,t,i=[]){this[e]===t&&i.push(this);let r=this.children;for(let s=0,o=r.length;s<o;s++)r[s].getObjectsByProperty(e,t,i);return i}getWorldPosition(e){return this.updateWorldMatrix(!0,!1),e.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(e){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Ia,e,XI),e}getWorldScale(e){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Ia,YI,e),e}getWorldDirection(e){this.updateWorldMatrix(!0,!1);let t=this.matrixWorld.elements;return e.set(t[8],t[9],t[10]).normalize()}raycast(){}intersectsFrustum(){return!0}traverse(e){e(this);let t=this.children;for(let i=0,r=t.length;i<r;i++)t[i].traverse(e)}traverseVisible(e){if(this.visible===!1)return;e(this);let t=this.children;for(let i=0,r=t.length;i<r;i++)t[i].traverseVisible(e)}traverseAncestors(e){let t=this.parent;t!==null&&(e(t),t.traverseAncestors(e))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale);let e=this.pivot;if(e!==null){let t=e.x,i=e.y,r=e.z,s=this.matrix.elements;s[12]+=t-s[0]*t-s[4]*i-s[8]*r,s[13]+=i-s[1]*t-s[5]*i-s[9]*r,s[14]+=r-s[2]*t-s[6]*i-s[10]*r}this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(e){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||e)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,e=!0);let t=this.children;for(let i=0,r=t.length;i<r;i++)t[i].updateMatrixWorld(e)}updateWorldMatrix(e,t,i=!1){let r=this.parent;if(e===!0&&r!==null&&r.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||i)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,i=!0),t===!0){let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].updateWorldMatrix(!1,!0,i)}}toJSON(e){let t=e===void 0||typeof e=="string",i={};t&&(e={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},i.metadata={version:4.7,type:"Object",generator:"Object3D.toJSON"});let r={};r.uuid=this.uuid,r.type=this.type,r.name=this.name,r.castShadow=this.castShadow,r.receiveShadow=this.receiveShadow,r.visible=this.visible,r.frustumCulled=this.frustumCulled,r.renderOrder=this.renderOrder,r.static=this.static,r.matrixAutoUpdate=this.matrixAutoUpdate,Object.keys(this.userData).length>0&&(r.userData=this.userData),r.layers=this.layers.mask,r.matrix=this.matrix.toArray(),r.up=this.up.toArray(),this.pivot!==null&&(r.pivot=this.pivot.toArray()),this.morphTargetDictionary!==void 0&&(r.morphTargetDictionary=Object.assign({},this.morphTargetDictionary)),this.morphTargetInfluences!==void 0&&(r.morphTargetInfluences=this.morphTargetInfluences.slice()),this.isInstancedMesh&&(r.type="InstancedMesh",r.count=this.count,r.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(r.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(r.type="BatchedMesh",r.perObjectFrustumCulled=this.perObjectFrustumCulled,r.sortObjects=this.sortObjects,r.drawRanges=this._drawRanges,r.reservedRanges=this._reservedRanges,r.geometryInfo=this._geometryInfo.map(a=>vt(Ve({},a),{boundingBox:a.boundingBox?a.boundingBox.toJSON():void 0,boundingSphere:a.boundingSphere?a.boundingSphere.toJSON():void 0})),r.instanceInfo=this._instanceInfo.map(a=>Ve({},a)),r.availableInstanceIds=this._availableInstanceIds.slice(),r.availableGeometryIds=this._availableGeometryIds.slice(),r.nextIndexStart=this._nextIndexStart,r.nextVertexStart=this._nextVertexStart,r.geometryCount=this._geometryCount,r.maxInstanceCount=this._maxInstanceCount,r.maxVertexCount=this._maxVertexCount,r.maxIndexCount=this._maxIndexCount,r.geometryInitialized=this._geometryInitialized,r.matricesTexture=this._matricesTexture.toJSON(e),r.indirectTexture=this._indirectTexture.toJSON(e),this._colorsTexture!==null&&(r.colorsTexture=this._colorsTexture.toJSON(e)),this.boundingSphere!==null&&(r.boundingSphere=this.boundingSphere.toJSON()),this.boundingBox!==null&&(r.boundingBox=this.boundingBox.toJSON()));function s(a,c){return a[c.uuid]===void 0&&(a[c.uuid]=c.toJSON(e)),c.uuid}if(this.isScene)this.background&&(this.background.isColor?r.background=this.background.toJSON():this.background.isTexture&&(r.background=this.background.toJSON(e).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(r.environment=this.environment.toJSON(e).uuid);else if(this.isMesh||this.isLine||this.isPoints){r.geometry=s(e.geometries,this.geometry);let a=this.geometry.parameters;if(a!==void 0&&a.shapes!==void 0){let c=a.shapes;if(Array.isArray(c))for(let l=0,u=c.length;l<u;l++){let d=c[l];s(e.shapes,d)}else s(e.shapes,c)}}if(this.isSkinnedMesh&&(r.bindMode=this.bindMode,r.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(s(e.skeletons,this.skeleton),r.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let a=[];for(let c=0,l=this.material.length;c<l;c++)a.push(s(e.materials,this.material[c]));r.material=a}else r.material=s(e.materials,this.material);if(this.children.length>0){r.children=[];for(let a=0;a<this.children.length;a++)r.children.push(this.children[a].toJSON(e).object)}if(this.animations.length>0){r.animations=[];for(let a=0;a<this.animations.length;a++){let c=this.animations[a];r.animations.push(s(e.animations,c))}}if(t){let a=o(e.geometries),c=o(e.materials),l=o(e.textures),u=o(e.images),d=o(e.shapes),f=o(e.skeletons),h=o(e.animations),g=o(e.nodes);a.length>0&&(i.geometries=a),c.length>0&&(i.materials=c),l.length>0&&(i.textures=l),u.length>0&&(i.images=u),d.length>0&&(i.shapes=d),f.length>0&&(i.skeletons=f),h.length>0&&(i.animations=h),g.length>0&&(i.nodes=g)}return i.object=r,i;function o(a){let c=[];for(let l in a){let u=a[l];delete u.metadata,c.push(u)}return c}}clone(e){return new this.constructor().copy(this,e)}copy(e,t=!0){if(this.name=e.name,this.up.copy(e.up),this.position.copy(e.position),this.rotation.order=e.rotation.order,this.quaternion.copy(e.quaternion),this.scale.copy(e.scale),this.pivot=e.pivot!==null?e.pivot.clone():null,this.matrix.copy(e.matrix),this.matrixWorld.copy(e.matrixWorld),this.matrixAutoUpdate=e.matrixAutoUpdate,this.matrixWorldAutoUpdate=e.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=e.matrixWorldNeedsUpdate,this.layers.mask=e.layers.mask,this.visible=e.visible,this.castShadow=e.castShadow,this.receiveShadow=e.receiveShadow,this.frustumCulled=e.frustumCulled,this.renderOrder=e.renderOrder,this.static=e.static,this.animations=e.animations.slice(),this.userData=JSON.parse(JSON.stringify(e.userData)),t===!0)for(let i=0;i<e.children.length;i++){let r=e.children[i];this.add(r.clone())}return this}dispose(){this.dispatchEvent({type:"dispose"})}};Qt.DEFAULT_UP=new O(0,1,0);Qt.DEFAULT_MATRIX_AUTO_UPDATE=!0;Qt.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0;var pn=class extends Qt{constructor(){super(),this.isGroup=!0,this.type="Group"}},KI={type:"move"},wo=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new pn,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new pn,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new O,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new O),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new pn,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new O,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new O,this._grip.eventsEnabled=!1),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,s=null,o=null,a=this._targetRay,c=this._grip,l=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(l&&e.hand){o=!0;for(let M of e.hand.values()){let m=t.getJointPose(M,i),p=this._getHandJoint(l,M);m!==null&&(p.matrix.fromArray(m.transform.matrix),p.matrix.decompose(p.position,p.quaternion,p.scale),p.matrixWorldNeedsUpdate=!0,p.jointRadius=m.radius),p.visible=m!==null}let u=l.joints["index-finger-tip"],d=l.joints["thumb-tip"],f=u.position.distanceTo(d.position),h=.02,g=.005;l.inputState.pinching&&f>h+g?(l.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!l.inputState.pinching&&f<=h-g&&(l.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else c!==null&&e.gripSpace&&(s=t.getPose(e.gripSpace,i),s!==null&&(c.matrix.fromArray(s.transform.matrix),c.matrix.decompose(c.position,c.quaternion,c.scale),c.matrixWorldNeedsUpdate=!0,s.linearVelocity?(c.hasLinearVelocity=!0,c.linearVelocity.copy(s.linearVelocity)):c.hasLinearVelocity=!1,s.angularVelocity?(c.hasAngularVelocity=!0,c.angularVelocity.copy(s.angularVelocity)):c.hasAngularVelocity=!1,c.eventsEnabled&&c.dispatchEvent({type:"gripUpdated",data:e,target:this})));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&s!==null&&(r=s),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.quaternion,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(KI)))}return a!==null&&(a.visible=r!==null),c!==null&&(c.visible=s!==null),l!==null&&(l.visible=o!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new pn;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}},tM={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},Mr={h:0,s:0,l:0},su={h:0,s:0,l:0};function nm(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var He=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=Pn){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,Ke.colorSpaceToWorking(this,t),this}setRGB(e,t,i,r=Ke.workingColorSpace){return this.r=e,this.g=t,this.b=i,Ke.colorSpaceToWorking(this,r),this}setHSL(e,t,i,r=Ke.workingColorSpace){if(e=Qm(e,1),t=je(t,0,1),i=je(i,0,1),t===0)this.r=this.g=this.b=i;else{let s=i<=.5?i*(1+t):i+t-i*t,o=2*i-s;this.r=nm(o,s,e+1/3),this.g=nm(o,s,e),this.b=nm(o,s,e-1/3)}return Ke.colorSpaceToWorking(this,r),this}setStyle(e,t=Pn){function i(s){s!==void 0&&parseFloat(s)<1&&Ie("Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let s,o=r[1],a=r[2];switch(o){case"rgb":case"rgba":if(s=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d+(?:\.\d+)?|\.\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(255,parseInt(s[1],10))/255,Math.min(255,parseInt(s[2],10))/255,Math.min(255,parseInt(s[3],10))/255,t);if(s=/^\s*(\d+(?:\.\d+)?|\.\d+)\%\s*,\s*(\d+(?:\.\d+)?|\.\d+)\%\s*,\s*(\d+(?:\.\d+)?|\.\d+)\%\s*(?:,\s*(\d+(?:\.\d+)?|\.\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(100,parseFloat(s[1]))/100,Math.min(100,parseFloat(s[2]))/100,Math.min(100,parseFloat(s[3]))/100,t);break;case"hsl":case"hsla":if(s=/^\s*([+-]?(?:\d+(?:\.\d+)?|\.\d+))\s*,\s*(\d+(?:\.\d+)?|\.\d+)\%\s*,\s*(\d+(?:\.\d+)?|\.\d+)\%\s*(?:,\s*(\d+(?:\.\d+)?|\.\d+)\s*)?$/.exec(a))return i(s[4]),this.setHSL(parseFloat(s[1])/360,parseFloat(s[2])/100,parseFloat(s[3])/100,t);break;default:Ie("Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let s=r[1],o=s.length;if(o===3)return this.setRGB(parseInt(s.charAt(0),16)/15,parseInt(s.charAt(1),16)/15,parseInt(s.charAt(2),16)/15,t);if(o===6)return this.setHex(parseInt(s,16),t);Ie("Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=Pn){let i=tM[e.toLowerCase()];return i!==void 0?this.setHex(i,t):Ie("Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=Ki(e.r),this.g=Ki(e.g),this.b=Ki(e.b),this}copyLinearToSRGB(e){return this.r=_o(e.r),this.g=_o(e.g),this.b=_o(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=Pn){return Ke.workingToColorSpace(on.copy(this),e),Math.round(je(on.r*255,0,255))*65536+Math.round(je(on.g*255,0,255))*256+Math.round(je(on.b*255,0,255))}getHexString(e=Pn){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=Ke.workingColorSpace){Ke.workingToColorSpace(on.copy(this),t);let i=on.r,r=on.g,s=on.b,o=Math.max(i,r,s),a=Math.min(i,r,s),c,l,u=(a+o)/2;if(a===o)c=0,l=0;else{let d=o-a;switch(l=u<=.5?d/(o+a):d/(2-o-a),o){case i:c=(r-s)/d+(r<s?6:0);break;case r:c=(s-i)/d+2;break;case s:c=(i-r)/d+4;break}c/=6}return e.h=c,e.s=l,e.l=u,e}getRGB(e,t=Ke.workingColorSpace){return Ke.workingToColorSpace(on.copy(this),t),e.r=on.r,e.g=on.g,e.b=on.b,e}getStyle(e=Pn){Ke.workingToColorSpace(on.copy(this),e);let t=on.r,i=on.g,r=on.b;return e!==Pn?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(Mr),this.setHSL(Mr.h+e,Mr.s+t,Mr.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(Mr),e.getHSL(su);let i=La(Mr.h,su.h,t),r=La(Mr.s,su.s,t),s=La(Mr.l,su.l,t);return this.setHSL(i,r,s),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,s=e.elements;return this.r=s[0]*t+s[3]*i+s[6]*r,this.g=s[1]*t+s[4]*i+s[7]*r,this.b=s[2]*t+s[5]*i+s[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},on=new He;He.NAMES=tM;var Va=class n{constructor(e,t=1,i=1e3){this.isFog=!0,this.name="",this.color=new He(e),this.near=t,this.far=i}clone(){return new n(this.color,this.near,this.far)}toJSON(){return{type:"Fog",name:this.name,color:this.color.getHex(),near:this.near,far:this.far}}},Ha=class extends Qt{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new Qi,this.environmentIntensity=1,this.environmentRotation=new Qi,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),this.background=e.background!==null?e.background.clone():null,this.environment=e.environment!==null?e.environment.clone():null,this.fog=e.fog!==null?e.fog.clone():null,this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),this.overrideMaterial=e.overrideMaterial!==null?e.overrideMaterial.clone():null,this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),t.object.backgroundBlurriness=this.backgroundBlurriness,t.object.backgroundIntensity=this.backgroundIntensity,t.object.backgroundRotation=this.backgroundRotation.toArray(),t.object.environmentIntensity=this.environmentIntensity,t.object.environmentRotation=this.environmentRotation.toArray(),t}},oi=new O,qi=new O,im=new O,Xi=new O,lo=new O,uo=new O,Q0=new O,rm=new O,sm=new O,om=new O,am=new Et,cm=new Et,lm=new Et,Tr=class n{constructor(e=new O,t=new O,i=new O){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),oi.subVectors(e,t),r.cross(oi);let s=r.lengthSq();return s>0?r.multiplyScalar(1/Math.sqrt(s)):r.set(0,0,0)}static getBarycoord(e,t,i,r,s){oi.subVectors(r,t),qi.subVectors(i,t),im.subVectors(e,t);let o=oi.dot(oi),a=oi.dot(qi),c=oi.dot(im),l=qi.dot(qi),u=qi.dot(im),d=o*l-a*a;if(d===0)return s.set(0,0,0),null;let f=1/d,h=(l*c-a*u)*f,g=(o*u-a*c)*f;return s.set(1-h-g,g,h)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,Xi)===null?!1:Xi.x>=0&&Xi.y>=0&&Xi.x+Xi.y<=1}static getInterpolation(e,t,i,r,s,o,a,c){return this.getBarycoord(e,t,i,r,Xi)===null?(c.x=0,c.y=0,"z"in c&&(c.z=0),"w"in c&&(c.w=0),null):(c.setScalar(0),c.addScaledVector(s,Xi.x),c.addScaledVector(o,Xi.y),c.addScaledVector(a,Xi.z),c)}static getInterpolatedAttribute(e,t,i,r,s,o){return am.setScalar(0),cm.setScalar(0),lm.setScalar(0),am.fromBufferAttribute(e,t),cm.fromBufferAttribute(e,i),lm.fromBufferAttribute(e,r),o.setScalar(0),o.addScaledVector(am,s.x),o.addScaledVector(cm,s.y),o.addScaledVector(lm,s.z),o}static isFrontFacing(e,t,i,r){return oi.subVectors(i,t),qi.subVectors(e,t),oi.cross(qi).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return oi.subVectors(this.c,this.b),qi.subVectors(this.a,this.b),oi.cross(qi).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,s){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,s)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,s=this.c,o,a;lo.subVectors(r,i),uo.subVectors(s,i),rm.subVectors(e,i);let c=lo.dot(rm),l=uo.dot(rm);if(c<=0&&l<=0)return t.copy(i);sm.subVectors(e,r);let u=lo.dot(sm),d=uo.dot(sm);if(u>=0&&d<=u)return t.copy(r);let f=c*d-u*l;if(f<=0&&c>=0&&u<=0&&c-u>0)return o=c/(c-u),t.copy(i).addScaledVector(lo,o);om.subVectors(e,s);let h=lo.dot(om),g=uo.dot(om);if(g>=0&&h<=g)return t.copy(s);let M=h*l-c*g;if(M<=0&&l>=0&&g<=0)return a=l/(l-g),t.copy(i).addScaledVector(uo,a);let m=u*g-h*d;if(m<=0&&d-u>=0&&h-g>=0)return Q0.subVectors(s,r),a=(d-u)/(d-u+(h-g)),t.copy(r).addScaledVector(Q0,a);let p=1/(m+M+f);return o=M*p,a=f*p,t.copy(i).addScaledVector(lo,o).addScaledVector(uo,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},wr=class{constructor(e=new O(1/0,1/0,1/0),t=new O(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint(ai.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint(ai.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=ai.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let s=i.getAttribute("position");if(t===!0&&s!==void 0&&e.isInstancedMesh!==!0&&e.isBatchedMesh!==!0)for(let o=0,a=s.count;o<a;o++)e.isMesh===!0?e.getVertexPosition(o,ai):ai.fromBufferAttribute(s,o),ai.applyMatrix4(e.matrixWorld),this.expandByPoint(ai);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),ou.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),ou.copy(i.boundingBox)),ou.applyMatrix4(e.matrixWorld),this.union(ou)}let r=e.children;for(let s=0,o=r.length;s<o;s++)this.expandByObject(r[s],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,ai),ai.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(Ca),au.subVectors(this.max,Ca),fo.subVectors(e.a,Ca),ho.subVectors(e.b,Ca),po.subVectors(e.c,Ca),Sr.subVectors(ho,fo),Er.subVectors(po,ho),ds.subVectors(fo,po);let t=[0,-Sr.z,Sr.y,0,-Er.z,Er.y,0,-ds.z,ds.y,Sr.z,0,-Sr.x,Er.z,0,-Er.x,ds.z,0,-ds.x,-Sr.y,Sr.x,0,-Er.y,Er.x,0,-ds.y,ds.x,0];return!um(t,fo,ho,po,au)||(t=[1,0,0,0,1,0,0,0,1],!um(t,fo,ho,po,au))?!1:(cu.crossVectors(Sr,Er),t=[cu.x,cu.y,cu.z],um(t,fo,ho,po,au))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,ai).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize(ai).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(Yi[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),Yi[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),Yi[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),Yi[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),Yi[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),Yi[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),Yi[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),Yi[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(Yi),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}toJSON(){return{min:this.min.toArray(),max:this.max.toArray()}}fromJSON(e){return this.min.fromArray(e.min),this.max.fromArray(e.max),this}},Yi=[new O,new O,new O,new O,new O,new O,new O,new O],ai=new O,ou=new wr,fo=new O,ho=new O,po=new O,Sr=new O,Er=new O,ds=new O,Ca=new O,au=new O,cu=new O,fs=new O;function um(n,e,t,i,r){for(let s=0,o=n.length-3;s<=o;s+=3){fs.fromArray(n,s);let a=r.x*Math.abs(fs.x)+r.y*Math.abs(fs.y)+r.z*Math.abs(fs.z),c=e.dot(fs),l=t.dot(fs),u=i.dot(fs);if(Math.max(-Math.max(c,l,u),Math.min(c,l,u))>a)return!1}return!0}var Ft=new O,lu=new Te,JI=0,Ii=class extends ci{constructor(e,t,i=!1){if(super(),Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,Object.defineProperty(this,"id",{value:JI++}),this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=Xx,this.updateRanges=[],this.gpuType=di,this.version=0,this._normalizationScale=e!==void 0?tg(e):void 0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this._normalizationScale=e._normalizationScale,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,s=this.itemSize;r<s;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)lu.fromBufferAttribute(this,t),lu.applyMatrix3(e),this.setXY(t,lu.x,lu.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)Ft.fromBufferAttribute(this,t),Ft.applyMatrix3(e),this.setXYZ(t,Ft.x,Ft.y,Ft.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)Ft.fromBufferAttribute(this,t),Ft.applyMatrix4(e),this.setXYZ(t,Ft.x,Ft.y,Ft.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)Ft.fromBufferAttribute(this,t),Ft.applyNormalMatrix(e),this.setXYZ(t,Ft.x,Ft.y,Ft.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)Ft.fromBufferAttribute(this,t),Ft.transformDirection(e),this.setXYZ(t,Ft.x,Ft.y,Ft.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=this._denormalize(i)),i}setComponent(e,t,i){return this.normalized&&(i=this._normalize(i)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=this._denormalize(t)),t}setX(e,t){return this.normalized&&(t=this._normalize(t)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=this._denormalize(t)),t}setY(e,t){return this.normalized&&(t=this._normalize(t)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=this._denormalize(t)),t}setZ(e,t){return this.normalized&&(t=this._normalize(t)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=this._denormalize(t)),t}setW(e,t){return this.normalized&&(t=this._normalize(t)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=this._normalize(t),i=this._normalize(i)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=this._normalize(t),i=this._normalize(i),r=this._normalize(r)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e*=this.itemSize,this.normalized&&(t=this._normalize(t),i=this._normalize(i),r=this._normalize(r),s=this._normalize(s)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=s,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return e.name=this.name,e.usage=this.usage,e.gpuType=this.gpuType,e}dispose(){this.dispatchEvent({type:"dispose"})}_denormalize(e){let t=this._normalizationScale;return t!==void 0?Math.max(e/t,-1):Qx(e,this.array)}_normalize(e){let t=this._normalizationScale;return t!==void 0?Math.round(e*t):eM(e,this.array)}};var za=class extends Ii{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var Ga=class extends Ii{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var rt=class extends Ii{constructor(e,t,i){super(new Float32Array(e),t,i)}},QI=new wr,Aa=new O,dm=new O,ms=class{constructor(e=new O,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):QI.setFromPoints(e).getCenter(i);let r=0;for(let s=0,o=e.length;s<o;s++)r=Math.max(r,i.distanceToSquared(e[s]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;Aa.subVectors(e,this.center);let t=Aa.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(Aa,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(dm.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(Aa.copy(e.center).add(dm)),this.expandByPoint(Aa.copy(e.center).sub(dm))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}toJSON(){return{radius:this.radius,center:this.center.toArray()}}fromJSON(e){return this.radius=e.radius,this.center.fromArray(e.center),this}},eC=0,qn=new _t,fm=new Qt,mo=new O,Nn=new wr,Da=new wr,qt=new O,an=class n extends ci{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:eC++}),this.uuid=Lo(),this.name="",this.type="BufferGeometry",this.index=null,this.indirect=null,this.indirectOffset=0,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={},this._transformed=!1}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new(TI(e)?Ga:za)(e,1):this.index=e,this}setIndirect(e,t=0){return this.indirect=e,this.indirectOffset=t,this}getIndirect(){return this.indirect}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let s=new Le().getNormalMatrix(e);i.applyNormalMatrix(s),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this._transformed=!0,this}applyQuaternion(e){return qn.makeRotationFromQuaternion(e),this.applyMatrix4(qn),this}rotateX(e){return qn.makeRotationX(e),this.applyMatrix4(qn),this}rotateY(e){return qn.makeRotationY(e),this.applyMatrix4(qn),this}rotateZ(e){return qn.makeRotationZ(e),this.applyMatrix4(qn),this}translate(e,t,i){return qn.makeTranslation(e,t,i),this.applyMatrix4(qn),this}scale(e,t,i){return qn.makeScale(e,t,i),this.applyMatrix4(qn),this}lookAt(e){return fm.lookAt(e),fm.updateMatrix(),this.applyMatrix4(fm.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(mo).negate(),this.translate(mo.x,mo.y,mo.z),this}setFromPoints(e){let t=this.getAttribute("position");if(t===void 0){let i=[];for(let r=0,s=e.length;r<s;r++){let o=e[r];i.push(o.x,o.y,o.z||0)}this.setAttribute("position",new rt(i,3))}else{let i=Math.min(e.length,t.count);for(let r=0;r<i;r++){let s=e[r];t.setXYZ(r,s.x,s.y,s.z||0)}e.length>t.count&&Ie("BufferGeometry: Buffer size too small for points data. Use .dispose() and create a new geometry."),t.needsUpdate=!0}return this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new wr);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Ne("BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new O(-1/0,-1/0,-1/0),new O(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let s=t[i];Nn.setFromBufferAttribute(s),this.morphTargetsRelative?(qt.addVectors(this.boundingBox.min,Nn.min),this.boundingBox.expandByPoint(qt),qt.addVectors(this.boundingBox.max,Nn.max),this.boundingBox.expandByPoint(qt)):(this.boundingBox.expandByPoint(Nn.min),this.boundingBox.expandByPoint(Nn.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&Ne('BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new ms);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Ne("BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new O,1/0);return}if(e){let i=this.boundingSphere.center;if(Nn.setFromBufferAttribute(e),t)for(let s=0,o=t.length;s<o;s++){let a=t[s];Da.setFromBufferAttribute(a),this.morphTargetsRelative?(qt.addVectors(Nn.min,Da.min),Nn.expandByPoint(qt),qt.addVectors(Nn.max,Da.max),Nn.expandByPoint(qt)):(Nn.expandByPoint(Da.min),Nn.expandByPoint(Da.max))}Nn.getCenter(i);let r=0;for(let s=0,o=e.count;s<o;s++)qt.fromBufferAttribute(e,s),r=Math.max(r,i.distanceToSquared(qt));if(t)for(let s=0,o=t.length;s<o;s++){let a=t[s],c=this.morphTargetsRelative;for(let l=0,u=a.count;l<u;l++)qt.fromBufferAttribute(a,l),c&&(mo.fromBufferAttribute(e,l),qt.add(mo)),r=Math.max(r,i.distanceToSquared(qt))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&Ne('BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){Ne("BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,s=t.uv,o=this.getAttribute("tangent");(o===void 0||o.count!==i.count)&&(o=new Ii(new Float32Array(4*i.count),4),this.setAttribute("tangent",o));let a=[],c=[],l=new Uint8Array(i.count);for(let N=0;N<i.count;N++)a[N]=new O,c[N]=new O;let u=new O,d=new O,f=new O,h=new Te,g=new Te,M=new Te,m=new O,p=new O;function x(N,A,L){l[N]=l[A]=l[L]=1,u.fromBufferAttribute(i,N),d.fromBufferAttribute(i,A),f.fromBufferAttribute(i,L),h.fromBufferAttribute(s,N),g.fromBufferAttribute(s,A),M.fromBufferAttribute(s,L),d.sub(u),f.sub(u),g.sub(h),M.sub(h);let F=1/(g.x*M.y-M.x*g.y);isFinite(F)&&(m.copy(d).multiplyScalar(M.y).addScaledVector(f,-g.y).multiplyScalar(F),p.copy(f).multiplyScalar(g.x).addScaledVector(d,-M.x).multiplyScalar(F),a[N].add(m),a[A].add(m),a[L].add(m),c[N].add(p),c[A].add(p),c[L].add(p))}let w=this.groups;w.length===0&&(w=[{start:0,count:e.count}]);for(let N=0,A=w.length;N<A;++N){let L=w[N],F=L.start,G=L.count;for(let D=F,z=F+G;D<z;D+=3)x(e.getX(D+0),e.getX(D+1),e.getX(D+2))}let E=new O,I=new O,T=new O,y=new O;function S(N){T.fromBufferAttribute(r,N),y.copy(T);let A=a[N];E.copy(A),E.sub(T.multiplyScalar(T.dot(A))).normalize(),I.crossVectors(y,A);let F=I.dot(c[N])<0?-1:1;o.setXYZW(N,E.x,E.y,E.z,F)}for(let N=0,A=i.count;N<A;++N)l[N]&&S(N);this._transformed=!0}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0||i.count!==t.count)i=new Ii(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let f=0,h=i.count;f<h;f++)i.setXYZ(f,0,0,0);let r=new O,s=new O,o=new O,a=new O,c=new O,l=new O,u=new O,d=new O;if(e)for(let f=0,h=e.count;f<h;f+=3){let g=e.getX(f+0),M=e.getX(f+1),m=e.getX(f+2);r.fromBufferAttribute(t,g),s.fromBufferAttribute(t,M),o.fromBufferAttribute(t,m),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),a.fromBufferAttribute(i,g),c.fromBufferAttribute(i,M),l.fromBufferAttribute(i,m),a.add(u),c.add(u),l.add(u),i.setXYZ(g,a.x,a.y,a.z),i.setXYZ(M,c.x,c.y,c.z),i.setXYZ(m,l.x,l.y,l.z)}else for(let f=0,h=t.count;f<h;f+=3)r.fromBufferAttribute(t,f+0),s.fromBufferAttribute(t,f+1),o.fromBufferAttribute(t,f+2),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),i.setXYZ(f+0,u.x,u.y,u.z),i.setXYZ(f+1,u.x,u.y,u.z),i.setXYZ(f+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)qt.fromBufferAttribute(e,t),qt.normalize(),e.setXYZ(t,qt.x,qt.y,qt.z)}toNonIndexed(){function e(a,c){let l=a.array,u=a.itemSize,d=a.normalized,f=new l.constructor(c.length*u),h=0,g=0;for(let M=0,m=c.length;M<m;M++){a.isInterleavedBufferAttribute?h=c[M]*a.data.stride+a.offset:h=c[M]*u;for(let p=0;p<u;p++)f[g++]=l[h++]}return new Ii(f,u,d)}if(this.index===null)return Ie("BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let c=r[a],l=e(c,i);t.setAttribute(a,l)}let s=this.morphAttributes;for(let a in s){let c=[],l=s[a];for(let u=0,d=l.length;u<d;u++){let f=l[u],h=e(f,i);c.push(h)}t.morphAttributes[a]=c}t.morphTargetsRelative=this.morphTargetsRelative;let o=this.groups;for(let a=0,c=o.length;a<c;a++){let l=o[a];t.addGroup(l.start,l.count,l.materialIndex)}return t}toJSON(){let e={metadata:{version:4.7,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.parameters!==void 0&&this._transformed===!0?"BufferGeometry":this.type,e.name=this.name,Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0&&this._transformed!==!0){let c=this.parameters;for(let l in c)c[l]!==void 0&&(e[l]=c[l]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let c in i){let l=i[c];e.data.attributes[c]=l.toJSON(e.data)}let r={},s=!1;for(let c in this.morphAttributes){let l=this.morphAttributes[c],u=[];for(let d=0,f=l.length;d<f;d++){let h=l[d];u.push(h.toJSON(e.data))}u.length>0&&(r[c]=u,s=!0)}s&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let o=this.groups;o.length>0&&(e.data.groups=JSON.parse(JSON.stringify(o)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere=a.toJSON()),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone());let r=e.attributes;for(let l in r){let u=r[l];this.setAttribute(l,u.clone(t))}let s=e.morphAttributes;for(let l in s){let u=[],d=s[l];for(let f=0,h=d.length;f<h;f++)u.push(d[f].clone(t));this.morphAttributes[l]=u}this.morphTargetsRelative=e.morphTargetsRelative;let o=e.groups;for(let l=0,u=o.length;l<u;l++){let d=o[l];this.addGroup(d.start,d.count,d.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let c=e.boundingSphere;return c!==null&&(this.boundingSphere=c.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this._transformed=e._transformed,this}dispose(){this.dispatchEvent({type:"dispose"})}};var hm=new O,tC=new O,nC=new Le,fn=class{constructor(e=new O(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=hm.subVectors(i,t).cross(tC.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t,i=!0){let r=e.delta(hm),s=this.normal.dot(r);if(s===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let o=-(e.start.dot(this.normal)+this.constant)/s;return i===!0&&(o<0||o>1)?null:t.copy(e.start).addScaledVector(r,o)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||nC.getNormalMatrix(e),r=this.coplanarPoint(hm).applyMatrix4(e),s=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(s),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}toJSON(){return{normal:this.normal.toArray(),constant:this.constant}}fromJSON(e){return this.normal.fromArray(e.normal),this.constant=e.constant,this}},iC=0,er=class extends ci{constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:iC++}),this.uuid=Lo(),this.name="",this.type="Material",this.blending=Ro,this.side=Pr,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=Dm,this.blendDst=Rm,this.blendEquation=_s,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new He(0,0,0),this.blendAlpha=0,this.depthFunc=xo,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=zx,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=wu,this.stencilZFail=wu,this.stencilZPass=wu,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.allowOverride=!0,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){Ie(`Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){Ie(`Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector2&&i&&i.isVector2||r&&r.isEuler&&i&&i.isEuler||r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.7,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,i.blending=this.blending,i.side=this.side,i.shadowSide=this.shadowSide,i.vertexColors=this.vertexColors,i.opacity=this.opacity,i.transparent=this.transparent,i.blendSrc=this.blendSrc,i.blendDst=this.blendDst,i.blendEquation=this.blendEquation,i.blendSrcAlpha=this.blendSrcAlpha,i.blendDstAlpha=this.blendDstAlpha,i.blendEquationAlpha=this.blendEquationAlpha,i.blendColor=this.blendColor.getHex(),i.blendAlpha=this.blendAlpha,i.depthFunc=this.depthFunc,i.depthTest=this.depthTest,i.depthWrite=this.depthWrite,i.colorWrite=this.colorWrite,i.clipIntersection=this.clipIntersection,i.clipShadows=this.clipShadows,i.stencilWriteMask=this.stencilWriteMask,i.stencilFunc=this.stencilFunc,i.stencilRef=this.stencilRef,i.stencilFuncMask=this.stencilFuncMask,i.stencilFail=this.stencilFail,i.stencilZFail=this.stencilZFail,i.stencilZPass=this.stencilZPass,i.stencilWrite=this.stencilWrite,i.polygonOffset=this.polygonOffset,i.polygonOffsetFactor=this.polygonOffsetFactor,i.polygonOffsetUnits=this.polygonOffsetUnits,i.dithering=this.dithering,i.alphaTest=this.alphaTest,i.alphaHash=this.alphaHash,i.alphaToCoverage=this.alphaToCoverage,i.premultipliedAlpha=this.premultipliedAlpha,i.forceSinglePass=this.forceSinglePass,i.allowOverride=this.allowOverride,i.visible=this.visible,i.toneMapped=this.toneMapped,i.name=this.name,this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.diffuseRoughness!==void 0&&(i.diffuseRoughness=this.diffuseRoughness),this.diffuseRoughnessMap&&this.diffuseRoughnessMap.isTexture&&(i.diffuseRoughnessMap=this.diffuseRoughnessMap.toJSON(e).uuid),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.sheenColorMap&&this.sheenColorMap.isTexture&&(i.sheenColorMap=this.sheenColorMap.toJSON(e).uuid),this.sheenRoughnessMap&&this.sheenRoughnessMap.isTexture&&(i.sheenRoughnessMap=this.sheenRoughnessMap.toJSON(e).uuid),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.retroreflectivity!==void 0&&(i.retroreflectivity=this.retroreflectivity),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessMinimum!==void 0&&(i.iridescenceThicknessMinimum=this.iridescenceThicknessMinimum),this.iridescenceThicknessMaximum!==void 0&&(i.iridescenceThicknessMaximum=this.iridescenceThicknessMaximum),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),Array.isArray(this.clippingPlanes)&&this.clippingPlanes.length>0&&(i.clippingPlanes=this.clippingPlanes.map(s=>s.toJSON())),this.rotation!==void 0&&(i.rotation=this.rotation),this.depthPacking!==void 0&&(i.depthPacking=this.depthPacking),this.linewidth!==void 0&&(i.linewidth=this.linewidth),this.linecap!==void 0&&(i.linecap=this.linecap),this.linejoin!==void 0&&(i.linejoin=this.linejoin),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.wireframe!==void 0&&(i.wireframe=this.wireframe),this.wireframeLinewidth!==void 0&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!==void 0&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!==void 0&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading!==void 0&&(i.flatShading=this.flatShading),this.fog!==void 0&&(i.fog=this.fog),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(s){let o=[];for(let a in s){let c=s[a];delete c.metadata,o.push(c)}return o}if(t){let s=r(e.textures),o=r(e.images);s.length>0&&(i.textures=s),o.length>0&&(i.images=o)}return i}fromJSON(e,t){if(e.uuid!==void 0&&(this.uuid=e.uuid),e.name!==void 0&&(this.name=e.name),e.color!==void 0&&this.color!==void 0&&this.color.setHex(e.color),e.roughness!==void 0&&(this.roughness=e.roughness),e.metalness!==void 0&&(this.metalness=e.metalness),e.sheen!==void 0&&(this.sheen=e.sheen),e.sheenColor!==void 0&&(this.sheenColor=new He().setHex(e.sheenColor)),e.sheenRoughness!==void 0&&(this.sheenRoughness=e.sheenRoughness),e.emissive!==void 0&&this.emissive!==void 0&&this.emissive.setHex(e.emissive),e.specular!==void 0&&this.specular!==void 0&&this.specular.setHex(e.specular),e.specularIntensity!==void 0&&(this.specularIntensity=e.specularIntensity),e.specularColor!==void 0&&this.specularColor!==void 0&&this.specularColor.setHex(e.specularColor),e.shininess!==void 0&&(this.shininess=e.shininess),e.clearcoat!==void 0&&(this.clearcoat=e.clearcoat),e.clearcoatRoughness!==void 0&&(this.clearcoatRoughness=e.clearcoatRoughness),e.diffuseRoughness!==void 0&&(this.diffuseRoughness=e.diffuseRoughness),e.dispersion!==void 0&&(this.dispersion=e.dispersion),e.retroreflectivity!==void 0&&(this.retroreflectivity=e.retroreflectivity),e.iridescence!==void 0&&(this.iridescence=e.iridescence),e.iridescenceIOR!==void 0&&(this.iridescenceIOR=e.iridescenceIOR),e.iridescenceThicknessMinimum!==void 0&&(this.iridescenceThicknessMinimum=e.iridescenceThicknessMinimum),e.iridescenceThicknessMaximum!==void 0&&(this.iridescenceThicknessMaximum=e.iridescenceThicknessMaximum),e.iridescenceThicknessRange!==void 0&&(this.iridescenceThicknessMinimum=e.iridescenceThicknessRange[0],this.iridescenceThicknessMaximum=e.iridescenceThicknessRange[1]),e.transmission!==void 0&&(this.transmission=e.transmission),e.thickness!==void 0&&(this.thickness=e.thickness),e.attenuationDistance!==void 0&&(this.attenuationDistance=e.attenuationDistance),e.attenuationColor!==void 0&&this.attenuationColor!==void 0&&this.attenuationColor.setHex(e.attenuationColor),e.anisotropy!==void 0&&(this.anisotropy=e.anisotropy),e.anisotropyRotation!==void 0&&(this.anisotropyRotation=e.anisotropyRotation),e.fog!==void 0&&(this.fog=e.fog),e.flatShading!==void 0&&(this.flatShading=e.flatShading),e.blending!==void 0&&(this.blending=e.blending),e.combine!==void 0&&(this.combine=e.combine),e.side!==void 0&&(this.side=e.side),e.shadowSide!==void 0&&(this.shadowSide=e.shadowSide),e.opacity!==void 0&&(this.opacity=e.opacity),e.transparent!==void 0&&(this.transparent=e.transparent),e.alphaTest!==void 0&&(this.alphaTest=e.alphaTest),e.alphaHash!==void 0&&(this.alphaHash=e.alphaHash),e.depthFunc!==void 0&&(this.depthFunc=e.depthFunc),e.depthTest!==void 0&&(this.depthTest=e.depthTest),e.depthWrite!==void 0&&(this.depthWrite=e.depthWrite),e.colorWrite!==void 0&&(this.colorWrite=e.colorWrite),e.clippingPlanes!==void 0&&(this.clippingPlanes=e.clippingPlanes.map(i=>new fn().fromJSON(i))),e.clipIntersection!==void 0&&(this.clipIntersection=e.clipIntersection),e.clipShadows!==void 0&&(this.clipShadows=e.clipShadows),e.depthPacking!==void 0&&(this.depthPacking=e.depthPacking),e.blendSrc!==void 0&&(this.blendSrc=e.blendSrc),e.blendDst!==void 0&&(this.blendDst=e.blendDst),e.blendEquation!==void 0&&(this.blendEquation=e.blendEquation),e.blendSrcAlpha!==void 0&&(this.blendSrcAlpha=e.blendSrcAlpha),e.blendDstAlpha!==void 0&&(this.blendDstAlpha=e.blendDstAlpha),e.blendEquationAlpha!==void 0&&(this.blendEquationAlpha=e.blendEquationAlpha),e.blendColor!==void 0&&this.blendColor!==void 0&&this.blendColor.setHex(e.blendColor),e.blendAlpha!==void 0&&(this.blendAlpha=e.blendAlpha),e.stencilWriteMask!==void 0&&(this.stencilWriteMask=e.stencilWriteMask),e.stencilFunc!==void 0&&(this.stencilFunc=e.stencilFunc),e.stencilRef!==void 0&&(this.stencilRef=e.stencilRef),e.stencilFuncMask!==void 0&&(this.stencilFuncMask=e.stencilFuncMask),e.stencilFail!==void 0&&(this.stencilFail=e.stencilFail),e.stencilZFail!==void 0&&(this.stencilZFail=e.stencilZFail),e.stencilZPass!==void 0&&(this.stencilZPass=e.stencilZPass),e.stencilWrite!==void 0&&(this.stencilWrite=e.stencilWrite),e.wireframe!==void 0&&(this.wireframe=e.wireframe),e.wireframeLinewidth!==void 0&&(this.wireframeLinewidth=e.wireframeLinewidth),e.wireframeLinecap!==void 0&&(this.wireframeLinecap=e.wireframeLinecap),e.wireframeLinejoin!==void 0&&(this.wireframeLinejoin=e.wireframeLinejoin),e.rotation!==void 0&&(this.rotation=e.rotation),e.linewidth!==void 0&&(this.linewidth=e.linewidth),e.linecap!==void 0&&(this.linecap=e.linecap),e.linejoin!==void 0&&(this.linejoin=e.linejoin),e.dashSize!==void 0&&(this.dashSize=e.dashSize),e.gapSize!==void 0&&(this.gapSize=e.gapSize),e.scale!==void 0&&(this.scale=e.scale),e.polygonOffset!==void 0&&(this.polygonOffset=e.polygonOffset),e.polygonOffsetFactor!==void 0&&(this.polygonOffsetFactor=e.polygonOffsetFactor),e.polygonOffsetUnits!==void 0&&(this.polygonOffsetUnits=e.polygonOffsetUnits),e.dithering!==void 0&&(this.dithering=e.dithering),e.alphaToCoverage!==void 0&&(this.alphaToCoverage=e.alphaToCoverage),e.premultipliedAlpha!==void 0&&(this.premultipliedAlpha=e.premultipliedAlpha),e.forceSinglePass!==void 0&&(this.forceSinglePass=e.forceSinglePass),e.allowOverride!==void 0&&(this.allowOverride=e.allowOverride),e.visible!==void 0&&(this.visible=e.visible),e.toneMapped!==void 0&&(this.toneMapped=e.toneMapped),e.userData!==void 0&&(this.userData=e.userData),e.vertexColors!==void 0&&(typeof e.vertexColors=="number"?this.vertexColors=e.vertexColors>0:this.vertexColors=e.vertexColors),e.size!==void 0&&(this.size=e.size),e.sizeAttenuation!==void 0&&(this.sizeAttenuation=e.sizeAttenuation),e.map!==void 0&&(this.map=t[e.map]||null),e.matcap!==void 0&&(this.matcap=t[e.matcap]||null),e.alphaMap!==void 0&&(this.alphaMap=t[e.alphaMap]||null),e.bumpMap!==void 0&&(this.bumpMap=t[e.bumpMap]||null),e.bumpScale!==void 0&&(this.bumpScale=e.bumpScale),e.normalMap!==void 0&&(this.normalMap=t[e.normalMap]||null),e.normalMapType!==void 0&&(this.normalMapType=e.normalMapType),e.normalScale!==void 0){let i=e.normalScale;Array.isArray(i)===!1&&(i=[i,i]),this.normalScale=new Te().fromArray(i)}return e.displacementMap!==void 0&&(this.displacementMap=t[e.displacementMap]||null),e.displacementScale!==void 0&&(this.displacementScale=e.displacementScale),e.displacementBias!==void 0&&(this.displacementBias=e.displacementBias),e.roughnessMap!==void 0&&(this.roughnessMap=t[e.roughnessMap]||null),e.metalnessMap!==void 0&&(this.metalnessMap=t[e.metalnessMap]||null),e.emissiveMap!==void 0&&(this.emissiveMap=t[e.emissiveMap]||null),e.emissiveIntensity!==void 0&&(this.emissiveIntensity=e.emissiveIntensity),e.specularMap!==void 0&&(this.specularMap=t[e.specularMap]||null),e.specularIntensityMap!==void 0&&(this.specularIntensityMap=t[e.specularIntensityMap]||null),e.specularColorMap!==void 0&&(this.specularColorMap=t[e.specularColorMap]||null),e.envMap!==void 0&&(this.envMap=t[e.envMap]||null),e.envMapRotation!==void 0&&this.envMapRotation.fromArray(e.envMapRotation),e.envMapIntensity!==void 0&&(this.envMapIntensity=e.envMapIntensity),e.reflectivity!==void 0&&(this.reflectivity=e.reflectivity),e.refractionRatio!==void 0&&(this.refractionRatio=e.refractionRatio),e.lightMap!==void 0&&(this.lightMap=t[e.lightMap]||null),e.lightMapIntensity!==void 0&&(this.lightMapIntensity=e.lightMapIntensity),e.aoMap!==void 0&&(this.aoMap=t[e.aoMap]||null),e.aoMapIntensity!==void 0&&(this.aoMapIntensity=e.aoMapIntensity),e.gradientMap!==void 0&&(this.gradientMap=t[e.gradientMap]||null),e.clearcoatMap!==void 0&&(this.clearcoatMap=t[e.clearcoatMap]||null),e.clearcoatRoughnessMap!==void 0&&(this.clearcoatRoughnessMap=t[e.clearcoatRoughnessMap]||null),e.clearcoatNormalMap!==void 0&&(this.clearcoatNormalMap=t[e.clearcoatNormalMap]||null),e.clearcoatNormalScale!==void 0&&(this.clearcoatNormalScale=new Te().fromArray(e.clearcoatNormalScale)),e.diffuseRoughnessMap!==void 0&&(this.diffuseRoughnessMap=t[e.diffuseRoughnessMap]||null),e.iridescenceMap!==void 0&&(this.iridescenceMap=t[e.iridescenceMap]||null),e.iridescenceThicknessMap!==void 0&&(this.iridescenceThicknessMap=t[e.iridescenceThicknessMap]||null),e.transmissionMap!==void 0&&(this.transmissionMap=t[e.transmissionMap]||null),e.thicknessMap!==void 0&&(this.thicknessMap=t[e.thicknessMap]||null),e.anisotropyMap!==void 0&&(this.anisotropyMap=t[e.anisotropyMap]||null),e.sheenColorMap!==void 0&&(this.sheenColorMap=t[e.sheenColorMap]||null),e.sheenRoughnessMap!==void 0&&(this.sheenRoughnessMap=t[e.sheenRoughnessMap]||null),this}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let s=0;s!==r;++s)i[s]=t[s].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.allowOverride=e.allowOverride,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}};var Zi=new O,pm=new O,uu=new O,du=new O,Ir=class{constructor(e=new O,t=new O(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,Zi)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=Zi.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(Zi.copy(this.origin).addScaledVector(this.direction,t),Zi.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){pm.copy(e).add(t).multiplyScalar(.5),uu.copy(t).sub(e).normalize(),du.copy(this.origin).sub(pm);let s=e.distanceTo(t)*.5,o=-this.direction.dot(uu),a=du.dot(this.direction),c=-du.dot(uu),l=du.lengthSq(),u=Math.abs(1-o*o),d,f,h,g;if(u>0)if(d=o*c-a,f=o*a-c,g=s*u,d>=0)if(f>=-g)if(f<=g){let M=1/u;d*=M,f*=M,h=d*(d+o*f+2*a)+f*(o*d+f+2*c)+l}else f=s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*c)+l;else f=-s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*c)+l;else f<=-g?(d=Math.max(0,-(-o*s+a)),f=d>0?-s:Math.min(Math.max(-s,-c),s),h=-d*d+f*(f+2*c)+l):f<=g?(d=0,f=Math.min(Math.max(-s,-c),s),h=f*(f+2*c)+l):(d=Math.max(0,-(o*s+a)),f=d>0?s:Math.min(Math.max(-s,-c),s),h=-d*d+f*(f+2*c)+l);else f=o>0?-s:s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*c)+l;return i&&i.copy(this.origin).addScaledVector(this.direction,d),r&&r.copy(pm).addScaledVector(uu,f),h}intersectSphere(e,t){if(e.radius<0)return null;Zi.subVectors(e.center,this.origin);let i=Zi.dot(this.direction),r=Zi.dot(Zi)-i*i,s=e.radius*e.radius;if(r>s)return null;let o=Math.sqrt(s-r),a=i-o,c=i+o;return c<0?null:a<0?this.at(c,t):this.at(a,t)}intersectsSphere(e){return e.radius<0?!1:this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,s,o,a,c,l=1/this.direction.x,u=1/this.direction.y,d=1/this.direction.z,f=this.origin;return l>=0?(i=(e.min.x-f.x)*l,r=(e.max.x-f.x)*l):(i=(e.max.x-f.x)*l,r=(e.min.x-f.x)*l),u>=0?(s=(e.min.y-f.y)*u,o=(e.max.y-f.y)*u):(s=(e.max.y-f.y)*u,o=(e.min.y-f.y)*u),i>o||s>r||((s>i||isNaN(i))&&(i=s),(o<r||isNaN(r))&&(r=o),d>=0?(a=(e.min.z-f.z)*d,c=(e.max.z-f.z)*d):(a=(e.max.z-f.z)*d,c=(e.min.z-f.z)*d),i>c||a>r)||((a>i||i!==i)&&(i=a),(c<r||r!==r)&&(r=c),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,Zi)!==null}intersectTriangle(e,t,i,r,s){let o=this.origin,a=this.direction,c=a.x,l=a.y,u=a.z,d=e.x-o.x,f=e.y-o.y,h=e.z-o.z,g=t.x-o.x,M=t.y-o.y,m=t.z-o.z,p=i.x-o.x,x=i.y-o.y,w=i.z-o.z,E=Math.abs(c),I=Math.abs(l),T=Math.abs(u),y,S,N,A,L,F,G,D,z,$,H,te;if(E>=I&&E>=T?(N=c,F=d,z=g,te=p,c>=0?(y=l,S=u,A=f,L=h,G=M,D=m,$=x,H=w):(y=u,S=l,A=h,L=f,G=m,D=M,$=w,H=x)):I>=T?(N=l,F=f,z=M,te=x,l>=0?(y=u,S=c,A=h,L=d,G=m,D=g,$=w,H=p):(y=c,S=u,A=d,L=h,G=g,D=m,$=p,H=w)):(N=u,F=h,z=m,te=w,u>=0?(y=c,S=l,A=d,L=f,G=g,D=M,$=p,H=x):(y=l,S=c,A=f,L=d,G=M,D=g,$=x,H=p)),N===0)return null;let j=y/N,J=S/N,Q=1/N,Me=A-j*F,Fe=L-J*F,Y=G-j*z,pe=D-J*z,ve=$-j*te,Ce=H-J*te,me=ve*pe-Ce*Y,Ue=Me*Ce-Fe*ve,Rt=Y*Fe-pe*Me;if(r){if(me<0||Ue<0||Rt<0)return null}else if((me<0||Ue<0||Rt<0)&&(me>0||Ue>0||Rt>0))return null;let Re=me+Ue+Rt;if(Re===0)return null;let at=Q*(me*F+Ue*z+Rt*te);return(Re>0?at<0:at>0)?null:this.at(at/Re,s)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},gs=class extends er{constructor(e){super(),this.isMeshBasicMaterial=!0,this.type="MeshBasicMaterial",this.color=new He(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new Qi,this.combine=Nm,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}},ex=new _t,hs=new Ir,fu=new ms,tx=new O,hu=new O,pu=new O,mu=new O,mm=new O,gu=new O,nx=new O,yu=new O,en=class extends Qt{constructor(e=new an,t=new gs){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.count=1,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0?this.morphTargetInfluences=e.morphTargetInfluences.slice():this.morphTargetInfluences=void 0,e.morphTargetDictionary!==void 0?this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary):this.morphTargetDictionary=void 0,this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,s=i.morphAttributes.position,o=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(s&&a){gu.set(0,0,0);for(let c=0,l=s.length;c<l;c++){let u=a[c],d=s[c];u!==0&&(mm.fromBufferAttribute(d,e),o?gu.addScaledVector(mm,u):gu.addScaledVector(mm.sub(t),u))}t.add(gu)}return t}intersectsFrustum(e){return e.intersectsObject(this)}raycast(e,t){let i=this.geometry,r=this.material,s=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),fu.copy(i.boundingSphere),fu.applyMatrix4(s),hs.copy(e.ray).recast(e.near),!(fu.containsPoint(hs.origin)===!1&&(hs.intersectSphere(fu,tx)===null||hs.origin.distanceToSquared(tx)>(e.far-e.near)**2))&&(ex.copy(s).invert(),hs.copy(e.ray).applyMatrix4(ex),!(i.boundingBox!==null&&hs.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,hs)))}_computeIntersections(e,t,i){let r,s=this.geometry,o=this.material,a=s.index,c=s.attributes.position,l=s.attributes.uv,u=s.attributes.uv1,d=s.attributes.normal,f=s.groups,h=s.drawRange;if(a!==null)if(Array.isArray(o))for(let g=0,M=f.length;g<M;g++){let m=f[g],p=o[m.materialIndex],x=Math.max(m.start,h.start),w=Math.min(a.count,Math.min(m.start+m.count,h.start+h.count));for(let E=x,I=w;E<I;E+=3){let T=a.getX(E),y=a.getX(E+1),S=a.getX(E+2);r=vu(this,p,e,i,l,u,d,T,y,S),r&&(r.faceIndex=Math.floor(E/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),M=Math.min(a.count,h.start+h.count);for(let m=g,p=M;m<p;m+=3){let x=a.getX(m),w=a.getX(m+1),E=a.getX(m+2);r=vu(this,o,e,i,l,u,d,x,w,E),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}else if(c!==void 0)if(Array.isArray(o))for(let g=0,M=f.length;g<M;g++){let m=f[g],p=o[m.materialIndex],x=Math.max(m.start,h.start),w=Math.min(c.count,Math.min(m.start+m.count,h.start+h.count));for(let E=x,I=w;E<I;E+=3){let T=E,y=E+1,S=E+2;r=vu(this,p,e,i,l,u,d,T,y,S),r&&(r.faceIndex=Math.floor(E/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),M=Math.min(c.count,h.start+h.count);for(let m=g,p=M;m<p;m+=3){let x=m,w=m+1,E=m+2;r=vu(this,o,e,i,l,u,d,x,w,E),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}}};function rC(n,e,t,i,r,s,o,a){let c;if(e.side===gn?c=i.intersectTriangle(o,s,r,!0,a):c=i.intersectTriangle(r,s,o,e.side===Pr,a),c===null)return null;yu.copy(a),yu.applyMatrix4(n.matrixWorld);let l=t.ray.origin.distanceTo(yu);return l<t.near||l>t.far?null:{distance:l,point:yu.clone(),object:n}}function vu(n,e,t,i,r,s,o,a,c,l){n.getVertexPosition(a,hu),n.getVertexPosition(c,pu),n.getVertexPosition(l,mu);let u=rC(n,e,t,i,hu,pu,mu,nx);if(u){let d=new O;Tr.getBarycoord(nx,hu,pu,mu,d),r&&(u.uv=Tr.getInterpolatedAttribute(r,a,c,l,d,new Te)),s&&(u.uv1=Tr.getInterpolatedAttribute(s,a,c,l,d,new Te)),o&&(u.normal=Tr.getInterpolatedAttribute(o,a,c,l,d,new O),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let f={a,b:c,c:l,normal:new O,materialIndex:0};Tr.getNormal(hu,pu,mu,f.normal),u.face=f,u.barycoord=d}return u}var Vu=class extends Sn{constructor(e=null,t=1,i=1,r,s,o,a,c,l=Xt,u=Xt,d,f){super(null,o,a,c,l,u,r,s,d,f),this.isDataTexture=!0,this.image={data:e,width:t,height:i},this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var ps=new ms,sC=new Te(.5,.5),_u=new O,Cr=class{constructor(e=new fn,t=new fn,i=new fn,r=new fn,s=new fn,o=new fn){this.planes=[e,t,i,r,s,o]}set(e,t,i,r,s,o){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(s),a[5].copy(o),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=Xn,i=!1){let r=this.planes,s=e.elements,o=s[0],a=s[1],c=s[2],l=s[3],u=s[4],d=s[5],f=s[6],h=s[7],g=s[8],M=s[9],m=s[10],p=s[11],x=s[12],w=s[13],E=s[14],I=s[15];if(r[0].setComponents(l-o,h-u,p-g,I-x).normalize(),r[1].setComponents(l+o,h+u,p+g,I+x).normalize(),r[2].setComponents(l+a,h+d,p+M,I+w).normalize(),r[3].setComponents(l-a,h-d,p-M,I-w).normalize(),i)r[4].setComponents(c,f,m,E).normalize(),r[5].setComponents(l-c,h-f,p-m,I-E).normalize();else if(r[4].setComponents(l-c,h-f,p-m,I-E).normalize(),t===Xn)r[5].setComponents(l+c,h+f,p+m,I+E).normalize();else if(t===Mo)r[5].setComponents(c,f,m,E).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),ps.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),ps.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere(ps)}intersectsSprite(e){ps.center.set(0,0,0);let t=sC.distanceTo(e.center);return ps.radius=.7071067811865476+t,ps.applyMatrix4(e.matrixWorld),this.intersectsSphere(ps)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let s=0;s<6;s++)if(t[s].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(_u.x=r.normal.x>0?e.max.x:e.min.x,_u.y=r.normal.y>0?e.max.y:e.min.y,_u.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(_u)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}},ix=new _t,Hu=class n{constructor(){this.coordinateSystem=Xn,this._frustums=[],this._count=0}setFromArrayCamera(e){let t=e.cameras,i=this._frustums;for(let r=0;r<t.length;r++){let s=t[r];ix.multiplyMatrices(s.projectionMatrix,s.matrixWorldInverse),i[r]===void 0&&(i[r]=new Cr),i[r].setFromProjectionMatrix(ix,s.coordinateSystem,s.reversedDepth)}return this._count=t.length,this}getFrustum(e){return this._frustums[e]}intersectsObject(e){let t=this._frustums;for(let i=0;i<this._count;i++)if(t[i].intersectsObject(e))return!0;return!1}intersectsSprite(e){let t=this._frustums;for(let i=0;i<this._count;i++)if(t[i].intersectsSprite(e))return!0;return!1}intersectsSphere(e){let t=this._frustums;for(let i=0;i<this._count;i++)if(t[i].intersectsSphere(e))return!0;return!1}intersectsBox(e){let t=this._frustums;for(let i=0;i<this._count;i++)if(t[i].intersectsBox(e))return!0;return!1}containsPoint(e){let t=this._frustums;for(let i=0;i<this._count;i++)if(t[i].containsPoint(e))return!0;return!1}copy(e){this.coordinateSystem=e.coordinateSystem;let t=this._frustums,i=e._frustums;for(let r=0;r<e._count;r++)t[r]===void 0&&(t[r]=new Cr),t[r].copy(i[r]);return this._count=e._count,this}clone(){return new n().copy(this)}};var Io=class extends er{constructor(e){super(),this.isLineBasicMaterial=!0,this.type="LineBasicMaterial",this.color=new He(16777215),this.map=null,this.linewidth=1,this.linecap="round",this.linejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.linewidth=e.linewidth,this.linecap=e.linecap,this.linejoin=e.linejoin,this.fog=e.fog,this}},zu=new O,Gu=new O,rx=new _t,Ra=new Ir,xu=new ms,gm=new O,sx=new O,Wu=class extends Qt{constructor(e=new an,t=new Io){super(),this.isLine=!0,this.type="Line",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0?this.morphTargetInfluences=e.morphTargetInfluences.slice():this.morphTargetInfluences=void 0,e.morphTargetDictionary!==void 0?this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary):this.morphTargetDictionary=void 0,this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}computeLineDistances(){let e=this.geometry;if(e.index===null){let t=e.attributes.position,i=[0];for(let r=1,s=t.count;r<s;r++)zu.fromBufferAttribute(t,r-1),Gu.fromBufferAttribute(t,r),i[r]=i[r-1],i[r]+=zu.distanceTo(Gu);e.setAttribute("lineDistance",new rt(i,1))}else Ie("Line.computeLineDistances(): Computation only possible with non-indexed BufferGeometry.");return this}intersectsFrustum(e){return e.intersectsObject(this)}raycast(e,t){let i=this.geometry,r=this.matrixWorld,s=e.params.Line.threshold,o=i.drawRange;if(i.boundingSphere===null&&i.computeBoundingSphere(),xu.copy(i.boundingSphere),xu.applyMatrix4(r),xu.radius+=s,e.ray.intersectsSphere(xu)===!1)return;rx.copy(r).invert(),Ra.copy(e.ray).applyMatrix4(rx);let a=s/((this.scale.x+this.scale.y+this.scale.z)/3),c=a*a,l=this.isLineSegments?2:1,u=i.index,f=i.attributes.position;if(u!==null){let h=Math.max(0,o.start),g=Math.min(u.count,o.start+o.count);for(let M=h,m=g-1;M<m;M+=l){let p=u.getX(M),x=u.getX(M+1),w=Mu(this,e,Ra,c,p,x,M);w&&t.push(w)}if(this.isLineLoop){let M=u.getX(g-1),m=u.getX(h),p=Mu(this,e,Ra,c,M,m,g-1);p&&t.push(p)}}else{let h=Math.max(0,o.start),g=Math.min(f.count,o.start+o.count);for(let M=h,m=g-1;M<m;M+=l){let p=Mu(this,e,Ra,c,M,M+1,M);p&&t.push(p)}if(this.isLineLoop){let M=Mu(this,e,Ra,c,g-1,h,g-1);M&&t.push(M)}}}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}};function Mu(n,e,t,i,r,s,o){let a=n.geometry.attributes.position;if(zu.fromBufferAttribute(a,r),Gu.fromBufferAttribute(a,s),t.distanceSqToSegment(zu,Gu,gm,sx)>i)return;gm.applyMatrix4(n.matrixWorld);let l=e.ray.origin.distanceTo(gm);if(!(l<e.near||l>e.far))return{distance:l,point:sx.clone().applyMatrix4(n.matrixWorld),index:o,face:null,faceIndex:null,barycoord:null,object:n}}var ox=new O,ax=new O,Wa=class extends Wu{constructor(e,t){super(e,t),this.isLineSegments=!0,this.type="LineSegments"}computeLineDistances(){let e=this.geometry;if(e.index===null){let t=e.attributes.position,i=[];for(let r=0,s=t.count;r<s;r+=2)ox.fromBufferAttribute(t,r),ax.fromBufferAttribute(t,r+1),i[r]=r===0?0:i[r-1],i[r+1]=i[r]+ox.distanceTo(ax);e.setAttribute("lineDistance",new rt(i,1))}else Ie("LineSegments.computeLineDistances(): Computation only possible with non-indexed BufferGeometry.");return this}};var ja=class extends Sn{constructor(e=[],t=xs,i,r,s,o,a,c,l,u){super(e,t,i,r,s,o,a,c,l,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}};var tr=class extends Sn{constructor(e,t,i=ui,r,s,o,a=Xt,c=Xt,l,u=Ji,d=1){if(u!==Ji&&u!==rr)throw new Error("THREE.DepthTexture: format must be either THREE.DepthFormat or THREE.DepthStencilFormat");let f={width:e,height:t,depth:d};super(f,r,s,o,a,c,u,i,l),this.isDepthTexture=!0,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.source=new bo(Object.assign({},e.image)),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return t.compareFunction=this.compareFunction,t}},$a=class extends tr{constructor(e,t=ui,i=xs,r,s,o=Xt,a=Xt,c,l=Ji){let u={width:e,height:e,depth:1},d=[u,u,u,u,u,u];super(e,e,t,i,r,s,o,a,c,l),this.image=d,this.isCubeDepthTexture=!0,this.isCubeTexture=!0}get images(){return this.image}set images(e){this.image=e}},qa=class extends Sn{constructor(e=null){super(),this.sourceTexture=e,this.isExternalTexture=!0}copy(e){return super.copy(e),this.sourceTexture=e.sourceTexture,this}},Ar=class n extends an{constructor(e=1,t=1,i=1,r=1,s=1,o=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:s,depthSegments:o};let a=this;r=Math.floor(r),s=Math.floor(s),o=Math.floor(o);let c=[],l=[],u=[],d=[],f=0,h=0;g("z","y","x",-1,-1,i,t,e,o,s,0),g("z","y","x",1,-1,i,t,-e,o,s,1),g("x","z","y",1,1,e,i,t,r,o,2),g("x","z","y",1,-1,e,i,-t,r,o,3),g("x","y","z",1,-1,e,t,i,r,s,4),g("x","y","z",-1,-1,e,t,-i,r,s,5),this.setIndex(c),this.setAttribute("position",new rt(l,3)),this.setAttribute("normal",new rt(u,3)),this.setAttribute("uv",new rt(d,2));function g(M,m,p,x,w,E,I,T,y,S,N){let A=E/y,L=I/S,F=E/2,G=I/2,D=T/2,z=y+1,$=S+1,H=0,te=0,j=new O;for(let J=0;J<$;J++){let Q=J*L-G;for(let Me=0;Me<z;Me++){let Fe=Me*A-F;j[M]=Fe*x,j[m]=Q*w,j[p]=D,l.push(j.x,j.y,j.z),j[M]=0,j[m]=0,j[p]=T>0?1:-1,u.push(j.x,j.y,j.z),d.push(Me/y),d.push(1-J/S),H+=1}}for(let J=0;J<S;J++)for(let Q=0;Q<y;Q++){let Me=f+Q+z*J,Fe=f+Q+z*(J+1),Y=f+(Q+1)+z*(J+1),pe=f+(Q+1)+z*J;c.push(Me,Fe,pe),c.push(Fe,Y,pe),te+=6}a.addGroup(h,te,N),h+=te,f+=H}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};var Xa=class n extends an{constructor(e=1,t=32,i=0,r=Math.PI*2){super(),this.type="CircleGeometry",this.parameters={radius:e,segments:t,thetaStart:i,thetaLength:r},t=Math.max(3,t);let s=[],o=[],a=[],c=[],l=new O,u=new Te;o.push(0,0,0),a.push(0,0,1),c.push(.5,.5);for(let d=0,f=3;d<=t;d++,f+=3){let h=i+d/t*r;l.x=e*Math.cos(h),l.y=e*Math.sin(h),o.push(l.x,l.y,l.z),a.push(0,0,1),u.x=(o[f]/e+1)/2,u.y=(o[f+1]/e+1)/2,c.push(u.x,u.y)}for(let d=1;d<=t;d++)s.push(d,d+1,0);this.setIndex(s),this.setAttribute("position",new rt(o,3)),this.setAttribute("normal",new rt(a,3)),this.setAttribute("uv",new rt(c,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radius,e.segments,e.thetaStart,e.thetaLength)}},Ci=class n extends an{constructor(e=1,t=1,i=1,r=32,s=1,o=!1,a=0,c=Math.PI*2){super(),this.type="CylinderGeometry",this.parameters={radiusTop:e,radiusBottom:t,height:i,radialSegments:r,heightSegments:s,openEnded:o,thetaStart:a,thetaLength:c};let l=this;r=Math.floor(r),s=Math.floor(s);let u=[],d=[],f=[],h=[],g=0,M=[],m=i/2,p=0;x(),o===!1&&(e>0&&w(!0),t>0&&w(!1)),this.setIndex(u),this.setAttribute("position",new rt(d,3)),this.setAttribute("normal",new rt(f,3)),this.setAttribute("uv",new rt(h,2));function x(){let E=new O,I=new O,T=0,y=(t-e)/i;for(let S=0;S<=s;S++){let N=[],A=S/s,L=A*(t-e)+e;for(let F=0;F<=r;F++){let G=F/r,D=G*c+a,z=Math.sin(D),$=Math.cos(D);I.x=L*z,I.y=-A*i+m,I.z=L*$,d.push(I.x,I.y,I.z),E.set(z,y,$).normalize(),f.push(E.x,E.y,E.z),h.push(G,1-A),N.push(g++)}M.push(N)}for(let S=0;S<r;S++)for(let N=0;N<s;N++){let A=M[N][S],L=M[N+1][S],F=M[N+1][S+1],G=M[N][S+1];(e>0||N!==0)&&(u.push(A,L,G),T+=3),(t>0||N!==s-1)&&(u.push(L,F,G),T+=3)}l.addGroup(p,T,0),p+=T}function w(E){let I=g,T=new Te,y=new O,S=0,N=E===!0?e:t,A=E===!0?1:-1;for(let F=1;F<=r;F++)d.push(0,m*A,0),f.push(0,A,0),h.push(.5,.5),g++;let L=g;for(let F=0;F<=r;F++){let D=F/r*c+a,z=Math.cos(D),$=Math.sin(D);y.x=N*$,y.y=m*A,y.z=N*z,d.push(y.x,y.y,y.z),f.push(0,A,0),T.x=z*.5+.5,T.y=$*.5*A+.5,h.push(T.x,T.y),g++}for(let F=0;F<r;F++){let G=I+F,D=L+F;E===!0?u.push(D,D+1,G):u.push(D+1,D,G),S+=3}l.addGroup(p,S,E===!0?1:2),p+=S}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radiusTop,e.radiusBottom,e.height,e.radialSegments,e.heightSegments,e.openEnded,e.thetaStart,e.thetaLength)}},nr=class n extends Ci{constructor(e=1,t=1,i=32,r=1,s=!1,o=0,a=Math.PI*2){super(0,e,t,i,r,s,o,a),this.type="ConeGeometry",this.parameters={radius:e,height:t,radialSegments:i,heightSegments:r,openEnded:s,thetaStart:o,thetaLength:a}}static fromJSON(e){return new n(e.radius,e.height,e.radialSegments,e.heightSegments,e.openEnded,e.thetaStart,e.thetaLength)}},Ya=class n extends an{constructor(e=[],t=[],i=1,r=0){super(),this.type="PolyhedronGeometry",this.parameters={vertices:e,indices:t,radius:i,detail:r};let s=[],o=[];a(r),l(i),u(),this.setAttribute("position",new rt(s,3)),this.setAttribute("normal",new rt(s.slice(),3)),this.setAttribute("uv",new rt(o,2)),r===0?this.computeVertexNormals():this.normalizeNormals();function a(x){let w=new O,E=new O,I=new O;for(let T=0;T<t.length;T+=3)h(t[T+0],w),h(t[T+1],E),h(t[T+2],I),c(w,E,I,x)}function c(x,w,E,I){let T=I+1,y=[];for(let S=0;S<=T;S++){y[S]=[];let N=x.clone().lerp(E,S/T),A=w.clone().lerp(E,S/T),L=T-S;for(let F=0;F<=L;F++)F===0&&S===T?y[S][F]=N:y[S][F]=N.clone().lerp(A,F/L)}for(let S=0;S<T;S++)for(let N=0;N<2*(T-S)-1;N++){let A=Math.floor(N/2);N%2===0?(f(y[S][A+1]),f(y[S+1][A]),f(y[S][A])):(f(y[S][A+1]),f(y[S+1][A+1]),f(y[S+1][A]))}}function l(x){let w=new O;for(let E=0;E<s.length;E+=3)w.x=s[E+0],w.y=s[E+1],w.z=s[E+2],w.normalize().multiplyScalar(x),s[E+0]=w.x,s[E+1]=w.y,s[E+2]=w.z}function u(){let x=new O;for(let w=0;w<s.length;w+=3){x.x=s[w+0],x.y=s[w+1],x.z=s[w+2];let E=m(x)/2/Math.PI+.5,I=p(x)/Math.PI+.5;o.push(E,1-I)}g(),d()}function d(){for(let x=0;x<o.length;x+=6){let w=o[x+0],E=o[x+2],I=o[x+4],T=Math.max(w,E,I),y=Math.min(w,E,I);T>.9&&y<.1&&(w<.2&&(o[x+0]+=1),E<.2&&(o[x+2]+=1),I<.2&&(o[x+4]+=1))}}function f(x){s.push(x.x,x.y,x.z)}function h(x,w){let E=x*3;w.x=e[E+0],w.y=e[E+1],w.z=e[E+2]}function g(){let x=new O,w=new O,E=new O,I=new O,T=new Te,y=new Te,S=new Te;for(let N=0,A=0;N<s.length;N+=9,A+=6){x.set(s[N+0],s[N+1],s[N+2]),w.set(s[N+3],s[N+4],s[N+5]),E.set(s[N+6],s[N+7],s[N+8]),T.set(o[A+0],o[A+1]),y.set(o[A+2],o[A+3]),S.set(o[A+4],o[A+5]),I.copy(x).add(w).add(E).divideScalar(3);let L=m(I);M(T,A+0,x,L),M(y,A+2,w,L),M(S,A+4,E,L)}}function M(x,w,E,I){I<0&&x.x===1&&(o[w]=x.x-1),E.x===0&&E.z===0&&(o[w]=I/2/Math.PI+.5)}function m(x){return Math.atan2(x.z,-x.x)}function p(x){return Math.atan2(-x.y,Math.sqrt(x.x*x.x+x.z*x.z))}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.vertices,e.indices,e.radius,e.detail)}};var Za=class n extends Ya{constructor(e=1,t=0){let i=[1,0,0,-1,0,0,0,1,0,0,-1,0,0,0,1,0,0,-1],r=[0,2,4,0,4,3,0,3,5,0,5,2,1,2,5,1,5,3,1,3,4,1,4,2];super(i,r,e,t),this.type="OctahedronGeometry",this.parameters={radius:e,detail:t}}static fromJSON(e){return new n(e.radius,e.detail)}},Ka=class n extends an{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let s=e/2,o=t/2,a=Math.floor(i),c=Math.floor(r),l=a+1,u=c+1,d=e/a,f=t/c,h=[],g=[],M=[],m=[];for(let p=0;p<u;p++){let x=p*f-o;for(let w=0;w<l;w++){let E=w*d-s;g.push(E,-x,0),M.push(0,0,1),m.push(w/a),m.push(1-p/c)}}for(let p=0;p<c;p++)for(let x=0;x<a;x++){let w=x+l*p,E=x+l*(p+1),I=x+1+l*(p+1),T=x+1+l*p;h.push(w,E,T),h.push(E,I,T)}this.setIndex(h),this.setAttribute("position",new rt(g,3)),this.setAttribute("normal",new rt(M,3)),this.setAttribute("uv",new rt(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}};var ys=class n extends an{constructor(e=1,t=32,i=16,r=0,s=Math.PI*2,o=0,a=Math.PI){super(),this.type="SphereGeometry",this.parameters={radius:e,widthSegments:t,heightSegments:i,phiStart:r,phiLength:s,thetaStart:o,thetaLength:a},t=Math.max(3,Math.floor(t)),i=Math.max(2,Math.floor(i));let c=Math.min(o+a,Math.PI),l=0,u=[],d=new O,f=new O,h=[],g=[],M=[],m=[];for(let p=0;p<=i;p++){let x=[],w=p/i,E=o+w*a,I=e*Math.cos(E),T=Math.sqrt(e*e-I*I),y=0;p===0&&o===0?y=.5/t:p===i&&c===Math.PI&&(y=-.5/t);for(let S=0;S<=t;S++){let N=S/t,A=r+N*s;d.x=-T*Math.cos(A),d.y=I,d.z=T*Math.sin(A),g.push(d.x,d.y,d.z),f.copy(d).normalize(),M.push(f.x,f.y,f.z),m.push(N+y,1-w),x.push(l++)}u.push(x)}for(let p=0;p<i;p++)for(let x=0;x<t;x++){let w=u[p][x+1],E=u[p][x],I=u[p+1][x],T=u[p+1][x+1];(p!==0||o>0)&&h.push(w,E,T),(p!==i-1||c<Math.PI)&&h.push(E,I,T)}this.setIndex(h),this.setAttribute("position",new rt(g,3)),this.setAttribute("normal",new rt(M,3)),this.setAttribute("uv",new rt(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radius,e.widthSegments,e.heightSegments,e.phiStart,e.phiLength,e.thetaStart,e.thetaLength)}},Ja=class n extends Ya{constructor(e=1,t=0){let i=[1,1,1,-1,-1,1,-1,1,-1,1,-1,-1],r=[2,1,0,0,3,2,1,3,0,2,3,1];super(i,r,e,t),this.type="TetrahedronGeometry",this.parameters={radius:e,detail:t}}static fromJSON(e){return new n(e.radius,e.detail)}},Qa=class n extends an{constructor(e=1,t=.4,i=12,r=48,s=Math.PI*2,o=0,a=Math.PI*2){super(),this.type="TorusGeometry",this.parameters={radius:e,tube:t,radialSegments:i,tubularSegments:r,arc:s,thetaStart:o,thetaLength:a},i=Math.floor(i),r=Math.floor(r);let c=[],l=[],u=[],d=[],f=new O,h=new O,g=new O;for(let M=0;M<=i;M++){let m=o+M/i*a;for(let p=0;p<=r;p++){let x=p/r*s;h.x=(e+t*Math.cos(m))*Math.cos(x),h.y=(e+t*Math.cos(m))*Math.sin(x),h.z=t*Math.sin(m),l.push(h.x,h.y,h.z),f.x=e*Math.cos(x),f.y=e*Math.sin(x),g.subVectors(h,f).normalize(),u.push(g.x,g.y,g.z),d.push(p/r),d.push(M/i)}}for(let M=1;M<=i;M++)for(let m=1;m<=r;m++){let p=(r+1)*M+m-1,x=(r+1)*(M-1)+m-1,w=(r+1)*(M-1)+m,E=(r+1)*M+m;c.push(p,x,E),c.push(x,w,E)}this.setIndex(c),this.setAttribute("position",new rt(l,3)),this.setAttribute("normal",new rt(u,3)),this.setAttribute("uv",new rt(d,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radius,e.tube,e.radialSegments,e.tubularSegments,e.arc,e.thetaStart,e.thetaLength)}};function Ss(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];if(cx(r))r.isRenderTargetTexture?(Ie("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone();else if(Array.isArray(r))if(cx(r[0])){let s=[];for(let o=0,a=r.length;o<a;o++)s[o]=r[o].clone();e[t][i]=s}else e[t][i]=r.slice();else e[t][i]=r}}return e}function cn(n){let e={};for(let t=0;t<n.length;t++){let i=Ss(n[t]);for(let r in i)e[r]=i[r]}return e}function cx(n){return n&&(n.isColor||n.isMatrix3||n.isMatrix4||n.isVector2||n.isVector3||n.isVector4||n.isTexture||n.isQuaternion)}function oC(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function ig(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:Ke.workingColorSpace}var nM={clone:Ss,merge:cn},aC=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,cC=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,Ai=class extends er{constructor(e){super(),this.isShaderMaterial=!0,this.type="ShaderMaterial",this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=aC,this.fragmentShader=cC,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=Ss(e.uniforms),this.uniformsGroups=oC(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this.defaultAttributeValues=Object.assign({},e.defaultAttributeValues),this.index0AttributeName=e.index0AttributeName,this.uniformsNeedUpdate=e.uniformsNeedUpdate,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let o=this.uniforms[r].value;o&&o.isTexture?t.uniforms[r]={type:"t",value:o.toJSON(e).uuid}:o&&o.isColor?t.uniforms[r]={type:"c",value:o.getHex()}:o&&o.isVector2?t.uniforms[r]={type:"v2",value:o.toArray()}:o&&o.isVector3?t.uniforms[r]={type:"v3",value:o.toArray()}:o&&o.isVector4?t.uniforms[r]={type:"v4",value:o.toArray()}:o&&o.isMatrix3?t.uniforms[r]={type:"m3",value:o.toArray()}:o&&o.isMatrix4?t.uniforms[r]={type:"m4",value:o.toArray()}:t.uniforms[r]={value:o}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}fromJSON(e,t){if(super.fromJSON(e,t),e.uniforms!==void 0)for(let i in e.uniforms){let r=e.uniforms[i];switch(this.uniforms[i]={},r.type){case"t":this.uniforms[i].value=t[r.value]||null;break;case"c":this.uniforms[i].value=new He().setHex(r.value);break;case"v2":this.uniforms[i].value=new Te().fromArray(r.value);break;case"v3":this.uniforms[i].value=new O().fromArray(r.value);break;case"v4":this.uniforms[i].value=new Et().fromArray(r.value);break;case"m3":this.uniforms[i].value=new Le().fromArray(r.value);break;case"m4":this.uniforms[i].value=new _t().fromArray(r.value);break;default:this.uniforms[i].value=r.value}}if(e.defines!==void 0&&(this.defines=e.defines),e.vertexShader!==void 0&&(this.vertexShader=e.vertexShader),e.fragmentShader!==void 0&&(this.fragmentShader=e.fragmentShader),e.glslVersion!==void 0&&(this.glslVersion=e.glslVersion),e.extensions!==void 0)for(let i in e.extensions)this.extensions[i]=e.extensions[i];return e.lights!==void 0&&(this.lights=e.lights),e.clipping!==void 0&&(this.clipping=e.clipping),this}},ju=class extends Ai{constructor(e){super(e),this.isRawShaderMaterial=!0,this.type="RawShaderMaterial"}},Di=class extends er{constructor(e){super(),this.isMeshStandardMaterial=!0,this.type="MeshStandardMaterial",this.defines={STANDARD:""},this.color=new He(16777215),this.roughness=1,this.metalness=0,this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.emissive=new He(0),this.emissiveIntensity=1,this.emissiveMap=null,this.bumpMap=null,this.bumpScale=1,this.normalMap=null,this.normalMapType=$d,this.normalScale=new Te(1,1),this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.roughnessMap=null,this.metalnessMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new Qi,this.envMapIntensity=1,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.flatShading=!1,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.defines={STANDARD:""},this.color.copy(e.color),this.roughness=e.roughness,this.metalness=e.metalness,this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.emissive.copy(e.emissive),this.emissiveMap=e.emissiveMap,this.emissiveIntensity=e.emissiveIntensity,this.bumpMap=e.bumpMap,this.bumpScale=e.bumpScale,this.normalMap=e.normalMap,this.normalMapType=e.normalMapType,this.normalScale.copy(e.normalScale),this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.roughnessMap=e.roughnessMap,this.metalnessMap=e.metalnessMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.envMapIntensity=e.envMapIntensity,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.flatShading=e.flatShading,this.fog=e.fog,this}};var $u=class extends er{constructor(e){super(),this.isMeshDepthMaterial=!0,this.type="MeshDepthMaterial",this.depthPacking=Vx,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},qu=class extends er{constructor(e){super(),this.isMeshDistanceMaterial=!0,this.type="MeshDistanceMaterial",this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}};function go(n,e){return!n||n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}function Su(n){return n!==void 0&&n.inTangents!==void 0&&n.outTangents!==void 0}var Dr=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],s=t[i-1];n:{e:{let o;t:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<s)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(s=r,r=t[++i],e<r)break e}o=t.length;break t}if(!(e>=s)){let a=t[1];e<a&&(i=2,s=a);for(let c=i-2;;){if(s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===c)break;if(r=s,s=t[--i-1],e>=s)break e}o=i,i=0;break t}break n}for(;i<o;){let a=i+o>>>1;e<t[a]?o=a:i=a+1}if(r=t[i],s=t[i-1],s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,s,r)}return this.interpolate_(i,s,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,s=e*r;for(let o=0;o!==r;++o)t[o]=i[s+o];return t}interpolate_(){throw new Error("THREE.Interpolant: Call to abstract method.")}intervalChanged_(){}},Xu=class extends Dr{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:_m,endingEnd:_m}}intervalChanged_(e,t,i){let r=this.parameterPositions,s=e-2,o=e+1,a=r[s],c=r[o];if(a===void 0)switch(this.getSettings_().endingStart){case xm:s=e,a=2*t-i;break;case Mm:s=r.length-2,a=t+r[s]-r[s+1];break;default:s=e,a=i}if(c===void 0)switch(this.getSettings_().endingEnd){case xm:o=e,c=2*i-t;break;case Mm:o=1,c=i+r[1]-r[0];break;default:o=e-1,c=t}let l=(i-t)*.5,u=this.valueSize;this._weightPrev=l/(t-a),this._weightNext=l/(c-i),this._offsetPrev=s*u,this._offsetNext=o*u}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this._offsetPrev,d=this._offsetNext,f=this._weightPrev,h=this._weightNext,g=(i-t)/(r-t),M=g*g,m=M*g,p=-f*m+2*f*M-f*g,x=(1+f)*m+(-1.5-2*f)*M+(-.5+f)*g+1,w=(-1-h)*m+(1.5+h)*M+.5*g,E=h*m-h*M;for(let I=0;I!==a;++I)s[I]=p*o[u+I]+x*o[l+I]+w*o[c+I]+E*o[d+I];return s}},Yu=class extends Dr{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=(i-t)/(r-t),d=1-u;for(let f=0;f!==a;++f)s[f]=o[l+f]*d+o[c+f]*u;return s}},Zu=class extends Dr{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},Ku=class extends Dr{interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this.inTangents,d=this.outTangents;if(!u||!d){let g=(i-t)/(r-t),M=1-g;for(let m=0;m!==a;++m)s[m]=o[l+m]*M+o[c+m]*g;return s}let f=a*2,h=e-1;for(let g=0;g!==a;++g){let M=o[l+g],m=o[c+g],p=h*f+g*2,x=d[p],w=d[p+1],E=e*f+g*2,I=u[E],T=u[E+1],y=uC(i,t,x,I,r);s[g]=iM(y,M,w,T,m)}return s}};function iM(n,e,t,i,r){let s=1-n;return s*s*s*e+3*s*s*n*t+3*s*n*n*i+n*n*n*r}function lC(n,e,t,i,r){let s=1-n;return 3*s*s*(t-e)+6*s*n*(i-t)+3*n*n*(r-i)}function uC(n,e,t,i,r){let s=(n-e)/(r-e);for(let o=0;o<8;o++){let a=iM(s,e,t,i,r)-n;if(Math.abs(a)<1e-10)break;let c=lC(s,e,t,i,r);if(Math.abs(c)<1e-10)break;s=Math.max(0,Math.min(1,s-a/c))}return s}var Fn=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=go(t,this.TimeBufferType),this.values=go(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:go(e.times,Array),values:go(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r),Su(e.settings)&&(i.settings={inTangents:go(e.settings.inTangents,Array),outTangents:go(e.settings.outTangents,Array)})}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new Zu(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new Yu(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new Xu(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodBezier(e){let t=new Ku(this.times,this.values,this.getValueSize(),e);return this.settings&&(t.inTangents=this.settings.inTangents,t.outTangents=this.settings.outTangents),t}setInterpolation(e){let t;switch(e){case Oa:t=this.InterpolantFactoryMethodDiscrete;break;case Fu:t=this.InterpolantFactoryMethodLinear;break;case Tu:t=this.InterpolantFactoryMethodSmooth;break;case vm:t=this.InterpolantFactoryMethodBezier;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return Ie("KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return Oa;case this.InterpolantFactoryMethodLinear:return Fu;case this.InterpolantFactoryMethodSmooth:return Tu;case this.InterpolantFactoryMethodBezier:return vm}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e;Su(this.settings)&&(lx(this.settings.inTangents,e),lx(this.settings.outTangents,e))}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e;Su(this.settings)&&(ux(this.settings.inTangents,e),ux(this.settings.outTangents,e))}return this}trim(e,t){let i=this.times,r=i.length,s=0,o=r-1;for(;s!==r&&i[s]<e;)++s;for(;o!==-1&&i[o]>t;)--o;if(++o,s!==0||o!==r){s>=o&&(o=Math.max(o,1),s=o-1);let a=this.getValueSize();this.times=i.slice(s,o),this.values=this.values.slice(s*a,o*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(Ne("KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,s=i.length;s===0&&(Ne("KeyframeTrack: Track is empty.",this),e=!1);let o=null;for(let a=0;a!==s;a++){let c=i[a];if(typeof c=="number"&&isNaN(c)){Ne("KeyframeTrack: Time is not a valid number.",this,a,c),e=!1;break}if(o!==null&&o>c){Ne("KeyframeTrack: Out of order keys.",this,a,c,o),e=!1;break}o=c}if(r!==void 0&&wI(r))for(let a=0,c=r.length;a!==c;++a){let l=r[a];if(isNaN(l)){Ne("KeyframeTrack: Value is not a valid number.",this,a,l),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===Tu,s=e.length-1,o=1;for(let a=1;a<s;++a){let c=!1,l=e[a],u=e[a+1];if(l!==u&&(a!==1||l!==e[0]))if(r)c=!0;else{let d=a*i,f=d-i,h=d+i;for(let g=0;g!==i;++g){let M=t[d+g];if(M!==t[f+g]||M!==t[h+g]){c=!0;break}}}if(c){if(a!==o){e[o]=e[a];let d=a*i,f=o*i;for(let h=0;h!==i;++h)t[f+h]=t[d+h]}++o}}if(s>0){e[o]=e[s];for(let a=s*i,c=o*i,l=0;l!==i;++l)t[c+l]=t[a+l];++o}return o!==e.length?(this.times=e.slice(0,o),this.values=t.slice(0,o*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,Su(this.settings)&&(r.settings={inTangents:this.settings.inTangents.slice(),outTangents:this.settings.outTangents.slice()}),r}};function lx(n,e){for(let t=0,i=n.length;t!==i;t+=2)n[t]+=e}function ux(n,e){for(let t=0,i=n.length;t!==i;t+=2)n[t]*=e}Fn.prototype.ValueTypeName="";Fn.prototype.TimeBufferType=Float32Array;Fn.prototype.ValueBufferType=Float32Array;Fn.prototype.DefaultInterpolation=Fu;var Rr=class extends Fn{constructor(e,t,i){super(e,t,i)}};Rr.prototype.ValueTypeName="bool";Rr.prototype.ValueBufferType=Array;Rr.prototype.DefaultInterpolation=Oa;Rr.prototype.InterpolantFactoryMethodLinear=void 0;Rr.prototype.InterpolantFactoryMethodSmooth=void 0;var Ju=class extends Fn{constructor(e,t,i,r){super(e,t,i,r)}};Ju.prototype.ValueTypeName="color";var Qu=class extends Fn{constructor(e,t,i,r){super(e,t,i,r)}};Qu.prototype.ValueTypeName="number";var ed=class extends Dr{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=(i-t)/(r-t),l=e*a;for(let u=l+a;l!==u;l+=4)Ln.slerpFlat(s,0,o,l-a,o,l,c);return s}},ec=class extends Fn{constructor(e,t,i,r){super(e,t,i,r)}InterpolantFactoryMethodLinear(e){return new ed(this.times,this.values,this.getValueSize(),e)}};ec.prototype.ValueTypeName="quaternion";ec.prototype.InterpolantFactoryMethodSmooth=void 0;var Nr=class extends Fn{constructor(e,t,i){super(e,t,i)}};Nr.prototype.ValueTypeName="string";Nr.prototype.ValueBufferType=Array;Nr.prototype.DefaultInterpolation=Oa;Nr.prototype.InterpolantFactoryMethodLinear=void 0;Nr.prototype.InterpolantFactoryMethodSmooth=void 0;var td=class extends Fn{constructor(e,t,i,r){super(e,t,i,r)}};td.prototype.ValueTypeName="vector";var nd=class{constructor(e,t,i){let r=this,s=!1,o=0,a=0,c,l=[];this.onStart=void 0,this.onLoad=e,this.onProgress=t,this.onError=i,this._abortController=null,this.itemStart=function(u){a++,s===!1&&r.onStart!==void 0&&r.onStart(u,o,a),s=!0},this.itemEnd=function(u){o++,r.onProgress!==void 0&&r.onProgress(u,o,a),o===a&&(s=!1,r.onLoad!==void 0&&r.onLoad())},this.itemError=function(u){r.onError!==void 0&&r.onError(u)},this.resolveURL=function(u){return u=u.normalize("NFC"),c?c(u):u},this.setURLModifier=function(u){return c=u,this},this.addHandler=function(u,d){return l.push(u,d),this},this.removeHandler=function(u){let d=l.indexOf(u);return d!==-1&&l.splice(d,2),this},this.getHandler=function(u){for(let d=0,f=l.length;d<f;d+=2){let h=l[d],g=l[d+1];if(h.global&&(h.lastIndex=0),h.test(u))return g}return null},this.abort=function(){return this.abortController.abort(),this._abortController=null,this}}get abortController(){return this._abortController||(this._abortController=new AbortController),this._abortController}},rM=new nd,id=class{constructor(e){this.manager=e!==void 0?e:rM,this.crossOrigin="anonymous",this.withCredentials=!1,this.path="",this.resourcePath="",this.requestHeader={},this.priority="auto",this.cache="default",typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}load(){}loadAsync(e,t){let i=this;return new Promise(function(r,s){i.load(e,r,t,s)})}parse(){}setCrossOrigin(e){return this.crossOrigin=e,this}setWithCredentials(e){return this.withCredentials=e,this}setPath(e){return this.path=e,this}setResourcePath(e){return this.resourcePath=e,this}setRequestHeader(e){return this.requestHeader=e,this}setPriority(e){return this.priority=e,this}setCache(e){return this.cache=e,this}abort(){return this}};id.DEFAULT_MATERIAL_NAME="__DEFAULT";var tc=class extends Qt{static registerNode(e){this.prototype._lightNode=e}constructor(e,t=1){super(),this.isLight=!0,this.type="Light",this.color=new He(e),this.intensity=t}copy(e,t){return super.copy(e,t),this.color.copy(e.color),this.intensity=e.intensity,this}toJSON(e){let t=super.toJSON(e);return t.object.color=this.color.getHex(),t.object.intensity=this.intensity,t}},nc=class extends tc{constructor(e,t,i){super(e,i),this.isHemisphereLight=!0,this.type="HemisphereLight",this.position.copy(Qt.DEFAULT_UP),this.updateMatrix(),this.groundColor=new He(t)}copy(e,t){return super.copy(e,t),this.groundColor.copy(e.groundColor),this}toJSON(e){let t=super.toJSON(e);return t.object.groundColor=this.groundColor.getHex(),t}},ym=new _t,dx=new O,fx=new O,rd=class{constructor(e){this.camera=e,this.intensity=1,this.bias=0,this.biasNode=null,this.normalBias=0,this.radius=1,this.blurSamples=8,this.mapSize=new Te(512,512),this.mapType=En,this.map=null,this.mapPass=null,this.matrix=new _t,this.autoUpdate=!0,this.needsUpdate=!1,this._frustum=new Cr,this._frameExtents=new Te(1,1),this._viewportCount=1,this._viewports=[new Et(0,0,1,1)]}getViewportCount(){return this._viewportCount}getCamera(){return this.camera}getFrustum(){return this._frustum}updateMatrices(e){let t=this.camera;dx.setFromMatrixPosition(e.matrixWorld),t.position.copy(dx),fx.setFromMatrixPosition(e.target.matrixWorld),t.lookAt(fx),t.updateMatrixWorld(),this._updateMatrix(t,this.matrix,this._frustum)}_updateMatrix(e,t,i,r){ym.multiplyMatrices(e.projectionMatrix,e.matrixWorldInverse),i.setFromProjectionMatrix(ym,e.coordinateSystem,e.reversedDepth);let s=this._frameExtents,o=r?r.z/s.x:1,a=r?r.w/s.y:1,c=r?r.x/s.x:0,l=r?r.y/s.y:0;e.coordinateSystem===Mo||e.reversedDepth?t.set(.5*o,0,0,.5*o+c,0,.5*a,0,.5*a+l,0,0,1,0,0,0,0,1):t.set(.5*o,0,0,.5*o+c,0,.5*a,0,.5*a+l,0,0,.5,.5,0,0,0,1),t.multiply(ym)}getViewport(e){return this._viewports[e]}getFrameExtents(){return this._frameExtents}dispose(){this.map&&this.map.dispose(),this.mapPass&&this.mapPass.dispose()}copy(e){return this.camera=e.camera.clone(),this.intensity=e.intensity,this.bias=e.bias,this.radius=e.radius,this.autoUpdate=e.autoUpdate,this.needsUpdate=e.needsUpdate,this.normalBias=e.normalBias,this.blurSamples=e.blurSamples,this.mapSize.copy(e.mapSize),this.mapType=e.mapType,this.biasNode=e.biasNode,this}clone(){return new this.constructor().copy(this)}toJSON(){let e={};return e.intensity=this.intensity,e.bias=this.bias,e.normalBias=this.normalBias,e.radius=this.radius,e.blurSamples=this.blurSamples,e.mapSize=this.mapSize.toArray(),e.mapType=this.mapType,e.camera=this.camera.toJSON(!1).object,delete e.camera.matrix,e}},Eu=new O,bu=new Ln,Ti=new O,ic=class extends Qt{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new _t,this.projectionMatrix=new _t,this.projectionMatrixInverse=new _t,this.coordinateSystem=Xn,this._reversedDepth=!1}get reversedDepth(){return this._reversedDepth}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorld.decompose(Eu,bu,Ti),Ti.x===1&&Ti.y===1&&Ti.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(Eu,bu,Ti.set(1,1,1)).invert()}updateWorldMatrix(e,t,i=!1){super.updateWorldMatrix(e,t,i),this.matrixWorld.decompose(Eu,bu,Ti),Ti.x===1&&Ti.y===1&&Ti.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(Eu,bu,Ti.set(1,1,1)).invert()}clone(){return new this.constructor().copy(this)}},br=new O,hx=new Te,px=new Te,hn=class extends ic{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=Eo*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(Pa*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return Eo*2*Math.atan(Math.tan(Pa*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){br.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(br.x,br.y).multiplyScalar(-e/br.z),br.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(br.x,br.y).multiplyScalar(-e/br.z)}getViewSize(e,t){return this.getViewBounds(e,hx,px),t.subVectors(px,hx)}setViewOffset(e,t,i,r,s,o){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(Pa*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,s=-.5*r,o=this.view;if(this.view!==null&&this.view.enabled){let c=o.fullWidth,l=o.fullHeight;s+=o.offsetX*r/c,t-=o.offsetY*i/l,r*=o.width/c,i*=o.height/l}let a=this.filmOffset;a!==0&&(s+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(s,s+r,t,t-i,e,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}};var rc=class extends ic{constructor(e=-1,t=1,i=1,r=-1,s=.1,o=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=s,this.far=o,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,s,o){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,s=i-e,o=i+e,a=r+t,c=r-t;if(this.view!==null&&this.view.enabled){let l=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;s+=l*this.view.offsetX,o=s+l*this.view.width,a-=u*this.view.offsetY,c=a-u*this.view.height}this.projectionMatrix.makeOrthographic(s,o,a,c,this.near,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}},Sm=class extends rd{constructor(){super(new rc(-5,5,5,-5,.5,500)),this.isDirectionalLightShadow=!0}},sc=class extends tc{constructor(e,t){super(e,t),this.isDirectionalLight=!0,this.type="DirectionalLight",this.position.copy(Qt.DEFAULT_UP),this.updateMatrix(),this.target=new Qt,this.shadow=new Sm}dispose(){super.dispose(),this.shadow.dispose()}copy(e){return super.copy(e),this.target=e.target.clone(),this.shadow=e.shadow.clone(),this}toJSON(e){let t=super.toJSON(e);return t.object.shadow=this.shadow.toJSON(),t.object.target=this.target.uuid,t}};var yo=-90,vo=1,Co=class extends Qt{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new hn(yo,vo,e,t);r.layers=this.layers,this.add(r);let s=new hn(yo,vo,e,t);s.layers=this.layers,this.add(s);let o=new hn(yo,vo,e,t);o.layers=this.layers,this.add(o);let a=new hn(yo,vo,e,t);a.layers=this.layers,this.add(a);let c=new hn(yo,vo,e,t);c.layers=this.layers,this.add(c);let l=new hn(yo,vo,e,t);l.layers=this.layers,this.add(l)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,s,o,a,c]=t;for(let l of t)this.remove(l);if(e===Xn)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),s.up.set(0,0,-1),s.lookAt(0,1,0),o.up.set(0,0,1),o.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),c.up.set(0,1,0),c.lookAt(0,0,-1);else if(e===Mo)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),s.up.set(0,0,1),s.lookAt(0,1,0),o.up.set(0,0,-1),o.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),c.up.set(0,-1,0),c.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let l of t)this.add(l),l.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[s,o,a,c,l,u]=this.children,d=e.getRenderTarget(),f=e.getActiveCubeFace(),h=e.getActiveMipmapLevel(),g=e.xr.enabled;e.xr.enabled=!1;let M=i.texture.mipmapsAutoUpdate;i.texture.mipmapsAutoUpdate=!1;let m=!1;e.isWebGLRenderer===!0?m=e.state.buffers.depth.getReversed():m=e.reversedDepthBuffer,e.setRenderTarget(i,0,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,s),e.setRenderTarget(i,1,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,o),e.setRenderTarget(i,2,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,a),e.setRenderTarget(i,3,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,c),e.setRenderTarget(i,4,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,l),i.texture.mipmapsAutoUpdate=M,e.setRenderTarget(i,5,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,u),e.setRenderTarget(d,f,h),e.xr.enabled=g,i.texture.needsPMREMUpdate=!0}},sd=class extends hn{constructor(e=[]){super(),this.isArrayCamera=!0,this.isMultiViewCamera=!1,this.cameras=e,this.perCameraFrustumCulled=!1}};var rg="\\[\\]\\.:\\/",dC=new RegExp("["+rg+"]","g"),sg="[^"+rg+"]",fC="[^"+rg.replace("\\.","")+"]",hC=/((?:WC+[\/:])*)/.source.replace("WC",sg),pC=/(WCOD+)?/.source.replace("WCOD",fC),mC=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",sg),gC=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",sg),yC=new RegExp("^"+hC+pC+mC+gC+"$"),vC=["material","materials","bones","map"],Em=class{constructor(e,t,i){let r=i||xt.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,s=i.length;r!==s;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},xt=class n{constructor(e,t,i){this.path=t,this.parsedPath=i||n.parseTrackName(t),this.node=n.findNode(e,this.parsedPath.nodeName),this.rootNode=e,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(e,t,i){return e&&e.isAnimationObjectGroup?new n.Composite(e,t,i):new n(e,t,i)}static sanitizeNodeName(e){return e.replace(/\s/g,"_").replace(dC,"")}static parseTrackName(e){let t=yC.exec(e);if(t===null)throw new Error("THREE.PropertyBinding: Cannot parse trackName: "+e);let i={nodeName:t[2],objectName:t[3],objectIndex:t[4],propertyName:t[5],propertyIndex:t[6]},r=i.nodeName&&i.nodeName.lastIndexOf(".");if(r!==void 0&&r!==-1){let s=i.nodeName.substring(r+1);vC.indexOf(s)!==-1&&(i.nodeName=i.nodeName.substring(0,r),i.objectName=s)}if(i.propertyName===null||i.propertyName.length===0)throw new Error("THREE.PropertyBinding: can not parse propertyName from trackName: "+e);return i}static findNode(e,t){if(t===void 0||t===""||t==="."||t===-1||t===e.name||t===e.uuid)return e;if(e.skeleton){let i=e.skeleton.getBoneByName(t);if(i!==void 0)return i}if(e.children){let i=function(s){for(let o=0;o<s.length;o++){let a=s[o];if(a.name===t||a.uuid===t)return a;let c=i(a.children);if(c)return c}return null},r=i(e.children);if(r)return r}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(e,t){e[t]=this.targetObject[this.propertyName]}_getValue_array(e,t){let i=this.resolvedProperty;for(let r=0,s=i.length;r!==s;++r)e[t++]=i[r]}_getValue_arrayElement(e,t){e[t]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(e,t){this.resolvedProperty.toArray(e,t)}_setValue_direct(e,t){this.targetObject[this.propertyName]=e[t]}_setValue_direct_setNeedsUpdate(e,t){this.targetObject[this.propertyName]=e[t],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(e,t){this.targetObject[this.propertyName]=e[t],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(e,t){let i=this.resolvedProperty;for(let r=0,s=i.length;r!==s;++r)i[r]=e[t++]}_setValue_array_setNeedsUpdate(e,t){let i=this.resolvedProperty;for(let r=0,s=i.length;r!==s;++r)i[r]=e[t++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(e,t){let i=this.resolvedProperty;for(let r=0,s=i.length;r!==s;++r)i[r]=e[t++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(e,t){this.resolvedProperty[this.propertyIndex]=e[t]}_setValue_arrayElement_setNeedsUpdate(e,t){this.resolvedProperty[this.propertyIndex]=e[t],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(e,t){this.resolvedProperty[this.propertyIndex]=e[t],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(e,t){this.resolvedProperty.fromArray(e,t)}_setValue_fromArray_setNeedsUpdate(e,t){this.resolvedProperty.fromArray(e,t),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(e,t){this.resolvedProperty.fromArray(e,t),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(e,t){this.bind(),this.getValue(e,t)}_setValue_unbound(e,t){this.bind(),this.setValue(e,t)}bind(){let e=this.node,t=this.parsedPath,i=t.objectName,r=t.propertyName,s=t.propertyIndex;if(e||(e=n.findNode(this.rootNode,t.nodeName),this.node=e),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!e){Ie("PropertyBinding: No target node found for track: "+this.path+".");return}if(i){let l=t.objectIndex;switch(i){case"materials":if(!e.material){Ne("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!e.material.materials){Ne("PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}e=e.material.materials;break;case"bones":if(!e.skeleton){Ne("PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}e=e.skeleton.bones;for(let u=0;u<e.length;u++)if(e[u].name===l){l=u;break}break;case"map":if("map"in e){e=e.map;break}if(!e.material){Ne("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!e.material.map){Ne("PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}e=e.material.map;break;default:if(e[i]===void 0){Ne("PropertyBinding: Can not bind to objectName of node undefined.",this);return}e=e[i]}if(l!==void 0){if(e[l]===void 0){Ne("PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,e);return}e=e[l]}}let o=e[r];if(o===void 0){let l=t.nodeName;Ne("PropertyBinding: Trying to update property for track: "+l+"."+r+" but it wasn't found.",e);return}let a=this.Versioning.None;this.targetObject=e,e.isMaterial===!0?a=this.Versioning.NeedsUpdate:e.isObject3D===!0&&(a=this.Versioning.MatrixWorldNeedsUpdate);let c=this.BindingType.Direct;if(s!==void 0){if(r==="morphTargetInfluences"){if(!e.geometry){Ne("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!e.geometry.morphAttributes){Ne("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}e.morphTargetDictionary[s]!==void 0&&(s=e.morphTargetDictionary[s])}c=this.BindingType.ArrayElement,this.resolvedProperty=o,this.propertyIndex=s}else o.fromArray!==void 0&&o.toArray!==void 0?(c=this.BindingType.HasFromToArray,this.resolvedProperty=o):Array.isArray(o)?(c=this.BindingType.EntireArray,this.resolvedProperty=o):this.propertyName=r;this.getValue=this.GetterByBindingType[c],this.setValue=this.SetterByBindingTypeAndVersioning[c][a]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}};xt.Composite=Em;xt.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};xt.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};xt.prototype.GetterByBindingType=[xt.prototype._getValue_direct,xt.prototype._getValue_array,xt.prototype._getValue_arrayElement,xt.prototype._getValue_toArray];xt.prototype.SetterByBindingTypeAndVersioning=[[xt.prototype._setValue_direct,xt.prototype._setValue_direct_setNeedsUpdate,xt.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[xt.prototype._setValue_array,xt.prototype._setValue_array_setNeedsUpdate,xt.prototype._setValue_array_setMatrixWorldNeedsUpdate],[xt.prototype._setValue_arrayElement,xt.prototype._setValue_arrayElement_setNeedsUpdate,xt.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[xt.prototype._setValue_fromArray,xt.prototype._setValue_fromArray_setNeedsUpdate,xt.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var xz=new Float32Array(1);var mx=new _t,oc=class{constructor(e,t,i=0,r=1/0){this.ray=new Ir(e,t),this.near=i,this.far=r,this.camera=null,this.layers=new To,this.params={Mesh:{},Line:{threshold:1},LOD:{},Points:{threshold:1},Sprite:{}}}set(e,t){this.ray.set(e,t)}setFromCamera(e,t){t.isPerspectiveCamera?(this.ray.origin.setFromMatrixPosition(t.matrixWorld),this.ray.direction.set(e.x,e.y,.5).unproject(t).sub(this.ray.origin).normalize(),this.camera=t):t.isOrthographicCamera?(this.ray.origin.set(e.x,e.y,t.projectionMatrix.elements[14]).unproject(t),this.ray.direction.set(0,0,-1).transformDirection(t.matrixWorld),this.camera=t):Ne("Raycaster: Unsupported camera type: "+t.type)}setFromXRController(e){return mx.identity().extractRotation(e.matrixWorld),this.ray.origin.setFromMatrixPosition(e.matrixWorld),this.ray.direction.set(0,0,-1).applyMatrix4(mx),this}intersectObject(e,t=!0,i=[]){return bm(e,this,i,t),i.sort(gx),i}intersectObjects(e,t=!0,i=[]){for(let r=0,s=e.length;r<s;r++)bm(e[r],this,i,t);return i.sort(gx),i}};function gx(n,e){return n.distance-e.distance}function bm(n,e,t,i){let r=!0;if(n.layers.test(e.layers)&&n.raycast(e,t)===!1&&(r=!1),r===!0&&i===!0){let s=n.children;for(let o=0,a=s.length;o<a;o++)bm(s[o],e,t,!0)}}var Ao=class{constructor(e=1,t=0,i=0){this.radius=e,this.phi=t,this.theta=i}set(e,t,i){return this.radius=e,this.phi=t,this.theta=i,this}copy(e){return this.radius=e.radius,this.phi=e.phi,this.theta=e.theta,this}makeSafe(){return this.phi=je(this.phi,1e-6,Math.PI-1e-6),this}setFromVector3(e){return this.setFromCartesianCoords(e.x,e.y,e.z)}setFromCartesianCoords(e,t,i){return this.radius=Math.sqrt(e*e+t*t+i*i),this.radius===0?(this.theta=0,this.phi=0):(this.theta=Math.atan2(e,i),this.phi=Math.acos(je(t/this.radius,-1,1))),this}clone(){return new this.constructor().copy(this)}};var Tm=class{static{this.prototype.isMatrix2=!0}constructor(e,t,i,r){this.elements=[1,0,0,1],e!==void 0&&this.set(e,t,i,r)}identity(){return this.set(1,0,0,1),this}fromArray(e,t=0){for(let i=0;i<4;i++)this.elements[i]=e[i+t];return this}set(e,t,i,r){let s=this.elements;return s[0]=e,s[2]=t,s[1]=i,s[3]=r,this}};var ac=class extends ci{constructor(e,t=null){super(),this.object=e,this.domElement=t,this.enabled=!0,this.state=-1,this.keys={},this.mouseButtons={LEFT:null,MIDDLE:null,RIGHT:null},this.touches={ONE:null,TWO:null}}connect(e){this.domElement!==null&&this.disconnect(),this.domElement=e}disconnect(){}dispose(){}update(){}};function og(n,e,t,i){let r=_C(i);switch(t){case jm:return n*e;case qm:return n*e/r.components*r.byteLength;case hd:return n*e/r.components*r.byteLength;case Lr:return n*e*2/r.components*r.byteLength;case pd:return n*e*2/r.components*r.byteLength;case $m:return n*e*3/r.components*r.byteLength;case hi:return n*e*4/r.components*r.byteLength;case md:return n*e*4/r.components*r.byteLength;case uc:case dc:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case fc:case hc:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case yd:case _d:return Math.max(n,16)*Math.max(e,8)/4;case gd:case vd:return Math.max(n,8)*Math.max(e,8)/2;case xd:case Md:case Ed:case bd:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case Sd:case pc:case Td:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case wd:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Id:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case Cd:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case Ad:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case Dd:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case Rd:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case Nd:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case Pd:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case Ld:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case Od:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case Fd:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case kd:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case Ud:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case Bd:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case Vd:case Hd:case zd:return Math.ceil(n/4)*Math.ceil(e/4)*16;case Gd:case Wd:return Math.ceil(n/4)*Math.ceil(e/4)*8;case mc:case jd:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function _C(n){switch(n){case En:case Hm:return{byteLength:1,components:1};case Po:case zm:case fi:return{byteLength:2,components:1};case dd:case fd:return{byteLength:2,components:4};case ui:case ud:case di:return{byteLength:4,components:1};case Gm:case Wm:return{byteLength:4,components:3}}throw new Error(`THREE.TextureUtils: Unknown texture type ${n}.`)}typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:"187"}}));typeof window<"u"&&(window.__THREE__?Ie("WARNING: Multiple instances of Three.js being imported."):window.__THREE__="187");function wM(){let n=null,e=!1,t=null,i=null;function r(s,o){i=n.requestAnimationFrame(r),t(s,o)}return{start:function(){e!==!0&&t!==null&&n!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n!==null&&n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(s){t=s},setContext:function(s){n=s}}}function MC(n){let e=new WeakMap;function t(a,c){let l=a.array,u=a.usage,d=l.byteLength,f=n.createBuffer();n.bindBuffer(c,f),n.bufferData(c,l,u),a.onUploadCallback();let h;if(l instanceof Float32Array)h=n.FLOAT;else if(typeof Float16Array<"u"&&l instanceof Float16Array)h=n.HALF_FLOAT;else if(l instanceof Uint16Array)a.isFloat16BufferAttribute?h=n.HALF_FLOAT:h=n.UNSIGNED_SHORT;else if(l instanceof Int16Array)h=n.SHORT;else if(l instanceof Uint32Array)h=n.UNSIGNED_INT;else if(l instanceof Int32Array)h=n.INT;else if(l instanceof Int8Array)h=n.BYTE;else if(l instanceof Uint8Array)h=n.UNSIGNED_BYTE;else if(l instanceof Uint8ClampedArray)h=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+l);return{buffer:f,type:h,bytesPerElement:l.BYTES_PER_ELEMENT,version:a.version,size:d}}function i(a,c,l){let u=c.array,d=c.updateRanges;if(n.bindBuffer(l,a),d.length===0)n.bufferSubData(l,0,u);else{d.sort((h,g)=>h.start-g.start);let f=0;for(let h=1;h<d.length;h++){let g=d[f],M=d[h];M.start<=g.start+g.count+1?g.count=Math.max(g.count,M.start+M.count-g.start):(++f,d[f]=M)}d.length=f+1;for(let h=0,g=d.length;h<g;h++){let M=d[h];n.bufferSubData(l,M.start*u.BYTES_PER_ELEMENT,u,M.start,M.count)}c.clearUpdateRanges()}c.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function s(a){a.isInterleavedBufferAttribute&&(a=a.data);let c=e.get(a);c&&(n.deleteBuffer(c.buffer),e.delete(a))}function o(a,c){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let l=e.get(a);if(l===void 0)e.set(a,t(a,c));else if(l.version<a.version){if(l.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(l.buffer,a,c),l.version=a.version}}return{get:r,remove:s,update:o}}var SC=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,EC=`#ifdef USE_ALPHAHASH
	const float ALPHA_HASH_SCALE = 0.05;
	float hash2D( vec2 value ) {
		return fract( 1.0e4 * sin( 17.0 * value.x + 0.1 * value.y ) * ( 0.1 + abs( sin( 13.0 * value.y + value.x ) ) ) );
	}
	float hash3D( vec3 value ) {
		return hash2D( vec2( hash2D( value.xy ), value.z ) );
	}
	float getAlphaHashThreshold( vec3 position ) {
		float maxDeriv = max(
			length( dFdx( position.xyz ) ),
			length( dFdy( position.xyz ) )
		);
		float pixScale = 1.0 / ( ALPHA_HASH_SCALE * maxDeriv );
		vec2 pixScales = vec2(
			exp2( floor( log2( pixScale ) ) ),
			exp2( ceil( log2( pixScale ) ) )
		);
		vec2 alpha = vec2(
			hash3D( floor( pixScales.x * position.xyz ) ),
			hash3D( floor( pixScales.y * position.xyz ) )
		);
		float lerpFactor = fract( log2( pixScale ) );
		float x = ( 1.0 - lerpFactor ) * alpha.x + lerpFactor * alpha.y;
		float a = min( lerpFactor, 1.0 - lerpFactor );
		vec3 cases = vec3(
			x * x / ( 2.0 * a * ( 1.0 - a ) ),
			( x - 0.5 * a ) / ( 1.0 - a ),
			1.0 - ( ( 1.0 - x ) * ( 1.0 - x ) / ( 2.0 * a * ( 1.0 - a ) ) )
		);
		float threshold = ( x < ( 1.0 - a ) )
			? ( ( x < a ) ? cases.x : cases.y )
			: cases.z;
		return clamp( threshold , 1.0e-6, 1.0 );
	}
#endif`,bC=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,TC=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,wC=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,IC=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,CC=`#ifdef USE_AOMAP
	float ambientOcclusion = ( texture2D( aoMap, vAoMapUv ).r - 1.0 ) * aoMapIntensity + 1.0;
	reflectedLight.indirectDiffuse *= ambientOcclusion;
	#if defined( USE_CLEARCOAT ) 
		clearcoatSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_SHEEN ) 
		sheenSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_ENVMAP ) && defined( STANDARD )
		float dotNV = saturate( dot( geometryNormal, geometryViewDir ) );
		reflectedLight.indirectSpecular *= computeSpecularOcclusion( dotNV, ambientOcclusion, material.roughness );
	#endif
#endif`,AC=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,DC=`#ifdef USE_BATCHING
	#if ! defined( GL_ANGLE_multi_draw )
	#define gl_DrawID _gl_DrawID
	uniform int _gl_DrawID;
	#endif
	uniform highp sampler2D batchingTexture;
	uniform highp usampler2D batchingIdTexture;
	mat4 getBatchingMatrix( const in float i ) {
		int size = textureSize( batchingTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( batchingTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( batchingTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( batchingTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( batchingTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
	float getIndirectIndex( const in int i ) {
		int size = textureSize( batchingIdTexture, 0 ).x;
		int x = i % size;
		int y = i / size;
		return float( texelFetch( batchingIdTexture, ivec2( x, y ), 0 ).r );
	}
#endif
#ifdef USE_BATCHING_COLOR
	uniform sampler2D batchingColorTexture;
	vec4 getBatchingColor( const in float i ) {
		int size = textureSize( batchingColorTexture, 0 ).x;
		int j = int( i );
		int x = j % size;
		int y = j / size;
		return texelFetch( batchingColorTexture, ivec2( x, y ), 0 );
	}
#endif`,RC=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,NC=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,PC=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,LC=`float G_BlinnPhong_Implicit( ) {
	return 0.25;
}
float D_BlinnPhong( const in float shininess, const in float dotNH ) {
	return RECIPROCAL_PI * ( shininess * 0.5 + 1.0 ) * pow( dotNH, shininess );
}
vec3 BRDF_BlinnPhong( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in vec3 specularColor, const in float shininess ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( specularColor, 1.0, dotVH );
	float G = G_BlinnPhong_Implicit( );
	float D = D_BlinnPhong( shininess, dotNH );
	return F * ( G * D );
} // validated`,OC=`#ifdef USE_IRIDESCENCE
	const mat3 XYZ_TO_REC709 = mat3(
		 3.2404542, -0.9692660,  0.0556434,
		-1.5371385,  1.8760108, -0.2040259,
		-0.4985314,  0.0415560,  1.0572252
	);
	vec3 Fresnel0ToIor( vec3 fresnel0 ) {
		vec3 sqrtF0 = sqrt( fresnel0 );
		return ( vec3( 1.0 ) + sqrtF0 ) / ( vec3( 1.0 ) - sqrtF0 );
	}
	vec3 IorToFresnel0( vec3 transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - vec3( incidentIor ) ) / ( transmittedIor + vec3( incidentIor ) ) );
	}
	float IorToFresnel0( float transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - incidentIor ) / ( transmittedIor + incidentIor ));
	}
	vec3 evalSensitivity( float OPD, vec3 shift ) {
		float phase = 2.0 * PI * OPD * 1.0e-9;
		vec3 val = vec3( 5.4856e-13, 4.4201e-13, 5.2481e-13 );
		vec3 pos = vec3( 1.6810e+06, 1.7953e+06, 2.2084e+06 );
		vec3 var = vec3( 4.3278e+09, 9.3046e+09, 6.6121e+09 );
		vec3 xyz = val * sqrt( 2.0 * PI * var ) * cos( pos * phase + shift ) * exp( - pow2( phase ) * var );
		xyz.x += 9.7470e-14 * sqrt( 2.0 * PI * 4.5282e+09 ) * cos( 2.2399e+06 * phase + shift[ 0 ] ) * exp( - 4.5282e+09 * pow2( phase ) );
		xyz /= 1.0685e-7;
		vec3 rgb = XYZ_TO_REC709 * xyz;
		return rgb;
	}
	vec3 evalIridescence( float outsideIOR, float eta2, float cosTheta1, float thinFilmThickness, vec3 baseF0 ) {
		vec3 I;
		float iridescenceIOR = mix( outsideIOR, eta2, smoothstep( 0.0, 0.03, thinFilmThickness ) );
		float sinTheta2Sq = pow2( outsideIOR / iridescenceIOR ) * ( 1.0 - pow2( cosTheta1 ) );
		float cosTheta2Sq = 1.0 - sinTheta2Sq;
		if ( cosTheta2Sq < 0.0 ) {
			return vec3( 1.0 );
		}
		float cosTheta2 = sqrt( cosTheta2Sq );
		float R0 = IorToFresnel0( iridescenceIOR, outsideIOR );
		float R12 = F_Schlick( R0, 1.0, cosTheta1 );
		float T121 = 1.0 - R12;
		float phi12 = 0.0;
		if ( iridescenceIOR < outsideIOR ) phi12 = PI;
		float phi21 = PI - phi12;
		vec3 baseIOR = Fresnel0ToIor( clamp( baseF0, 0.0, 0.9999 ) );		vec3 R1 = IorToFresnel0( baseIOR, iridescenceIOR );
		vec3 R23 = F_Schlick( R1, 1.0, cosTheta2 );
		vec3 phi23 = vec3( 0.0 );
		if ( baseIOR[ 0 ] < iridescenceIOR ) phi23[ 0 ] = PI;
		if ( baseIOR[ 1 ] < iridescenceIOR ) phi23[ 1 ] = PI;
		if ( baseIOR[ 2 ] < iridescenceIOR ) phi23[ 2 ] = PI;
		float OPD = 2.0 * iridescenceIOR * thinFilmThickness * cosTheta2;
		vec3 phi = vec3( phi21 ) + phi23;
		vec3 R123 = clamp( R12 * R23, 1e-5, 0.9999 );
		vec3 r123 = sqrt( R123 );
		vec3 Rs = pow2( T121 ) * R23 / ( vec3( 1.0 ) - R123 );
		vec3 C0 = R12 + Rs;
		I = C0;
		vec3 Cm = Rs - T121;
		for ( int m = 1; m <= 2; ++ m ) {
			Cm *= r123;
			vec3 Sm = 2.0 * evalSensitivity( float( m ) * OPD, float( m ) * phi );
			I += Cm * Sm;
		}
		return max( I, vec3( 0.0 ) );
	}
#endif`,FC=`#ifdef USE_BUMPMAP
	uniform sampler2D bumpMap;
	uniform float bumpScale;
	vec2 dHdxy_fwd() {
		vec2 dSTdx = dFdx( vBumpMapUv );
		vec2 dSTdy = dFdy( vBumpMapUv );
		float Hll = bumpScale * texture2D( bumpMap, vBumpMapUv ).x;
		float dBx = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdx ).x - Hll;
		float dBy = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdy ).x - Hll;
		return vec2( dBx, dBy );
	}
	vec3 perturbNormalArb( vec3 surf_pos, vec3 surf_norm, vec2 dHdxy, float faceDirection ) {
		vec3 vSigmaX = normalize( dFdx( surf_pos.xyz ) );
		vec3 vSigmaY = normalize( dFdy( surf_pos.xyz ) );
		vec3 vN = surf_norm;
		vec3 R1 = cross( vSigmaY, vN );
		vec3 R2 = cross( vN, vSigmaX );
		float fDet = dot( vSigmaX, R1 ) * faceDirection;
		vec3 vGrad = sign( fDet ) * ( dHdxy.x * R1 + dHdxy.y * R2 );
		return normalize( abs( fDet ) * surf_norm - vGrad );
	}
#endif`,kC=`#if NUM_CLIPPING_PLANES > 0
	vec4 plane;
	#ifdef ALPHA_TO_COVERAGE
		float distanceToPlane, distanceGradient;
		float clipOpacity = 1.0;
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
			distanceGradient = fwidth( distanceToPlane ) / 2.0;
			clipOpacity *= smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			if ( clipOpacity == 0.0 ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			float unionClipOpacity = 1.0;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
				distanceGradient = fwidth( distanceToPlane ) / 2.0;
				unionClipOpacity *= 1.0 - smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			}
			#pragma unroll_loop_end
			clipOpacity *= 1.0 - unionClipOpacity;
		#endif
		diffuseColor.a *= clipOpacity;
		if ( diffuseColor.a == 0.0 ) discard;
	#else
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			if ( dot( vClipPosition, plane.xyz ) > plane.w ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			bool clipped = true;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				clipped = ( dot( vClipPosition, plane.xyz ) > plane.w ) && clipped;
			}
			#pragma unroll_loop_end
			if ( clipped ) discard;
		#endif
	#endif
#endif`,UC=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,BC=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,VC=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,HC=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#endif`,zC=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#endif`,GC=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec4 vColor;
#endif`,WC=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	vColor = vec4( 1.0 );
#endif
#ifdef USE_COLOR_ALPHA
	vColor *= color;
#elif defined( USE_COLOR )
	vColor.rgb *= color;
#endif
#ifdef USE_INSTANCING_COLOR
	vColor.rgb *= instanceColor.rgb;
#endif
#ifdef USE_BATCHING_COLOR
	vColor *= getBatchingColor( getIndirectIndex( gl_DrawID ) );
#endif`,jC=`#define PI 3.141592653589793
#define PI2 6.283185307179586
#define PI_HALF 1.5707963267948966
#define RECIPROCAL_PI 0.3183098861837907
#define RECIPROCAL_PI2 0.15915494309189535
#define EPSILON 1e-6
#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
#define whiteComplement( a ) ( 1.0 - saturate( a ) )
float pow2( const in float x ) { return x*x; }
vec3 pow2( const in vec3 x ) { return x*x; }
float pow3( const in float x ) { return x*x*x; }
float pow4( const in float x ) { float x2 = x*x; return x2*x2; }
float max3( const in vec3 v ) { return max( max( v.x, v.y ), v.z ); }
float average( const in vec3 v ) { return dot( v, vec3( 0.3333333 ) ); }
highp float rand( const in vec2 uv ) {
	const highp float a = 12.9898, b = 78.233, c = 43758.5453;
	highp float dt = dot( uv.xy, vec2( a,b ) ), sn = mod( dt, PI );
	return fract( sin( sn ) * c );
}
#ifdef HIGH_PRECISION
	float precisionSafeLength( vec3 v ) { return length( v ); }
#else
	float precisionSafeLength( vec3 v ) {
		float maxComponent = max3( abs( v ) );
		return length( v / maxComponent ) * maxComponent;
	}
#endif
struct IncidentLight {
	vec3 color;
	vec3 direction;
	bool visible;
};
struct ReflectedLight {
	vec3 directDiffuse;
	vec3 directSpecular;
	vec3 indirectDiffuse;
	vec3 indirectSpecular;
};
#ifdef USE_ALPHAHASH
	varying vec3 vPosition;
#endif
vec3 transformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );
}
#define inverseTransformDirection transformDirectionByInverseViewMatrix
vec3 transformNormalByInverseViewMatrix( in vec3 normal, in mat4 viewMatrix ) {
	return normalize( ( vec4( normal, 0.0 ) * viewMatrix ).xyz );
}
vec3 transformDirectionByInverseViewMatrix( in vec3 dir, in mat4 viewMatrix ) {
	return normalize( ( vec4( dir, 0.0 ) * viewMatrix ).xyz );
}
bool isPerspectiveMatrix( mat4 m ) {
	return m[ 2 ][ 3 ] == - 1.0;
}
vec2 equirectUv( in vec3 dir ) {
	float u = atan( dir.z, dir.x ) * RECIPROCAL_PI2 + 0.5;
	float v = asin( clamp( dir.y, - 1.0, 1.0 ) ) * RECIPROCAL_PI + 0.5;
	return vec2( u, v );
}
vec3 BRDF_Lambert( const in vec3 diffuseColor ) {
	return RECIPROCAL_PI * diffuseColor;
}
vec3 F_Schlick( const in vec3 f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
}
float F_Schlick( const in float f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
} // validated`,$C=`vec3 transformedNormal = objectNormal;
#ifdef USE_TANGENT
	vec3 transformedTangent = objectTangent;
#endif
#ifdef USE_BATCHING
	mat3 bm = mat3( batchingMatrix );
	transformedNormal /= vec3( dot( bm[ 0 ], bm[ 0 ] ), dot( bm[ 1 ], bm[ 1 ] ), dot( bm[ 2 ], bm[ 2 ] ) );
	transformedNormal = bm * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = bm * transformedTangent;
	#endif
#endif
#ifdef USE_INSTANCING
	mat3 im = mat3( instanceMatrix );
	transformedNormal /= vec3( dot( im[ 0 ], im[ 0 ] ), dot( im[ 1 ], im[ 1 ] ), dot( im[ 2 ], im[ 2 ] ) );
	transformedNormal = im * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = im * transformedTangent;
	#endif
#endif
transformedNormal = normalMatrix * transformedNormal;
#ifdef FLIP_SIDED
	transformedNormal = - transformedNormal;
#endif
#ifdef USE_TANGENT
	transformedTangent = ( modelViewMatrix * vec4( transformedTangent, 0.0 ) ).xyz;
#endif`,qC=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,XC=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,YC=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	#ifdef DECODE_VIDEO_TEXTURE_EMISSIVE
		emissiveColor = sRGBTransferEOTF( emissiveColor );
	#endif
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,ZC=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,KC="gl_FragColor = linearToOutputTexel( gl_FragColor );",JC=`vec4 LinearTransferOETF( in vec4 value ) {
	return value;
}
vec4 sRGBTransferEOTF( in vec4 value ) {
	return vec4( mix( pow( value.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), value.rgb * 0.0773993808, vec3( lessThanEqual( value.rgb, vec3( 0.04045 ) ) ) ), value.a );
}
vec4 sRGBTransferOETF( in vec4 value ) {
	return vec4( mix( pow( value.rgb, vec3( 0.41666 ) ) * 1.055 - vec3( 0.055 ), value.rgb * 12.92, vec3( lessThanEqual( value.rgb, vec3( 0.0031308 ) ) ) ), value.a );
}`,QC=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vec3 cameraToFrag;
		if ( isOrthographic ) {
			cameraToFrag = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToFrag = normalize( vWorldPosition - cameraPosition );
		}
		vec3 worldNormal = transformNormalByInverseViewMatrix( normal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vec3 reflectVec = reflect( cameraToFrag, worldNormal );
		#else
			vec3 reflectVec = refract( cameraToFrag, worldNormal, refractionRatio );
		#endif
	#else
		vec3 reflectVec = vReflect;
	#endif
	#ifdef ENVMAP_TYPE_CUBE
		vec4 envColor = textureCube( envMap, envMapRotation * reflectVec );
		#ifdef ENVMAP_BLENDING_MULTIPLY
			outgoingLight = mix( outgoingLight, outgoingLight * envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_MIX )
			outgoingLight = mix( outgoingLight, envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_ADD )
			outgoingLight += envColor.xyz * specularStrength * reflectivity;
		#endif
	#endif
#endif`,eA=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform mat3 envMapRotation;
	uniform samplerCube envMap;
	#ifdef ENVMAP_TYPE_PMREM
		float roughnessToMip( const in float roughness ) {
			float r = clamp( roughness, 0.0, 1.0 );
			return ENVMAP_MAX_LOD * r * ( 2.0 - r );
		}
	#endif
#endif`,tA=`#ifdef USE_ENVMAP
	uniform float reflectivity;
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		varying vec3 vWorldPosition;
		uniform float refractionRatio;
	#else
		varying vec3 vReflect;
	#endif
#endif`,nA=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,iA=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vWorldPosition = worldPosition.xyz;
	#else
		vec3 cameraToVertex;
		if ( isOrthographic ) {
			cameraToVertex = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToVertex = normalize( worldPosition.xyz - cameraPosition );
		}
		vec3 worldNormal = transformNormalByInverseViewMatrix( transformedNormal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vReflect = reflect( cameraToVertex, worldNormal );
		#else
			vReflect = refract( cameraToVertex, worldNormal, refractionRatio );
		#endif
	#endif
#endif`,rA=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,sA=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,oA=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,aA=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,cA=`#ifdef USE_GRADIENTMAP
	uniform sampler2D gradientMap;
#endif
vec3 getGradientIrradiance( vec3 normal, vec3 lightDirection ) {
	float dotNL = dot( normal, lightDirection );
	vec2 coord = vec2( dotNL * 0.5 + 0.5, 0.0 );
	#ifdef USE_GRADIENTMAP
		return vec3( texture2D( gradientMap, coord ).r );
	#else
		vec2 fw = fwidth( coord ) * 0.5;
		return mix( vec3( 0.7 ), vec3( 1.0 ), smoothstep( 0.7 - fw.x, 0.7 + fw.x, coord.x ) );
	#endif
}`,lA=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,uA=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,dA=`varying vec3 vViewPosition;
struct LambertMaterial {
	vec3 diffuseColor;
	float specularStrength;
};
void RE_Direct_Lambert( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Lambert( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Lambert
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,fA=`uniform bool receiveShadow;
uniform vec3 ambientLightColor;
#if defined( USE_LIGHT_PROBES )
	uniform vec3 lightProbe[ 9 ];
#endif
vec3 shGetIrradianceAt( in vec3 normal, in vec3 shCoefficients[ 9 ] ) {
	float x = normal.x, y = normal.y, z = normal.z;
	vec3 result = shCoefficients[ 0 ] * 0.886227;
	result += shCoefficients[ 1 ] * 2.0 * 0.511664 * y;
	result += shCoefficients[ 2 ] * 2.0 * 0.511664 * z;
	result += shCoefficients[ 3 ] * 2.0 * 0.511664 * x;
	result += shCoefficients[ 4 ] * 2.0 * 0.429043 * x * y;
	result += shCoefficients[ 5 ] * 2.0 * 0.429043 * y * z;
	result += shCoefficients[ 6 ] * ( 0.743125 * z * z - 0.247708 );
	result += shCoefficients[ 7 ] * 2.0 * 0.429043 * x * z;
	result += shCoefficients[ 8 ] * 0.429043 * ( x * x - y * y );
	return result;
}
vec3 getLightProbeIrradiance( const in vec3 lightProbe[ 9 ], const in vec3 normal ) {
	vec3 worldNormal = transformNormalByInverseViewMatrix( normal, viewMatrix );
	vec3 irradiance = shGetIrradianceAt( worldNormal, lightProbe );
	return irradiance;
}
vec3 getAmbientLightIrradiance( const in vec3 ambientLightColor ) {
	vec3 irradiance = ambientLightColor;
	return irradiance;
}
float getDistanceAttenuation( const in float lightDistance, const in float cutoffDistance, const in float decayExponent ) {
	float distanceFalloff = 1.0 / max( pow( lightDistance, decayExponent ), 0.01 );
	if ( cutoffDistance > 0.0 ) {
		distanceFalloff *= pow2( saturate( 1.0 - pow4( lightDistance / cutoffDistance ) ) );
	}
	return distanceFalloff;
}
float getSpotAttenuation( const in float coneCosine, const in float penumbraCosine, const in float angleCosine ) {
	return smoothstep( coneCosine, penumbraCosine, angleCosine );
}
#if NUM_SUN_LIGHTS > 0
	struct SunLight {
		vec3 direction;
		vec3 color;
	};
	uniform SunLight sunLights[ NUM_SUN_LIGHTS ];
	void getSunLightInfo( const in SunLight sunLight, out IncidentLight light ) {
		light.color = sunLight.color;
		light.direction = sunLight.direction;
		light.visible = true;
	}
#endif
#if NUM_DIR_LIGHTS > 0
	struct DirectionalLight {
		vec3 direction;
		vec3 color;
	};
	uniform DirectionalLight directionalLights[ NUM_DIR_LIGHTS ];
	void getDirectionalLightInfo( const in DirectionalLight directionalLight, out IncidentLight light ) {
		light.color = directionalLight.color;
		light.direction = directionalLight.direction;
		light.visible = true;
	}
#endif
#if NUM_POINT_LIGHTS > 0
	struct PointLight {
		vec3 position;
		vec3 color;
		float distance;
		float decay;
	};
	uniform PointLight pointLights[ NUM_POINT_LIGHTS ];
	void getPointLightInfo( const in PointLight pointLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = pointLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float lightDistance = length( lVector );
		light.color = pointLight.color;
		light.color *= getDistanceAttenuation( lightDistance, pointLight.distance, pointLight.decay );
		light.visible = ( light.color != vec3( 0.0 ) );
	}
#endif
#if NUM_SPOT_LIGHTS > 0
	struct SpotLight {
		vec3 position;
		vec3 direction;
		vec3 color;
		float distance;
		float decay;
		float coneCos;
		float penumbraCos;
	};
	uniform SpotLight spotLights[ NUM_SPOT_LIGHTS ];
	void getSpotLightInfo( const in SpotLight spotLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = spotLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float angleCos = dot( light.direction, spotLight.direction );
		float spotAttenuation = getSpotAttenuation( spotLight.coneCos, spotLight.penumbraCos, angleCos );
		if ( spotAttenuation > 0.0 ) {
			float lightDistance = length( lVector );
			light.color = spotLight.color * spotAttenuation;
			light.color *= getDistanceAttenuation( lightDistance, spotLight.distance, spotLight.decay );
			light.visible = ( light.color != vec3( 0.0 ) );
		} else {
			light.color = vec3( 0.0 );
			light.visible = false;
		}
	}
#endif
#if NUM_RECT_AREA_LIGHTS > 0
	struct RectAreaLight {
		vec3 color;
		vec3 position;
		vec3 halfWidth;
		vec3 halfHeight;
	};
	uniform sampler2D ltc_1;	uniform sampler2D ltc_2;
	uniform RectAreaLight rectAreaLights[ NUM_RECT_AREA_LIGHTS ];
#endif
#if NUM_HEMI_LIGHTS > 0
	struct HemisphereLight {
		vec3 direction;
		vec3 skyColor;
		vec3 groundColor;
	};
	uniform HemisphereLight hemisphereLights[ NUM_HEMI_LIGHTS ];
	vec3 getHemisphereLightIrradiance( const in HemisphereLight hemiLight, const in vec3 normal ) {
		float dotNL = dot( normal, hemiLight.direction );
		float hemiDiffuseWeight = 0.5 * dotNL + 0.5;
		vec3 irradiance = mix( hemiLight.groundColor, hemiLight.skyColor, hemiDiffuseWeight );
		return irradiance;
	}
#endif
#include <lightprobes_pars_fragment>`,hA=`#ifdef USE_ENVMAP
	vec3 getIBLIrradiance( const in vec3 normal ) {
		#ifdef ENVMAP_TYPE_PMREM
			vec3 worldNormal = transformNormalByInverseViewMatrix( normal, viewMatrix );
			vec4 envMapColor = textureLod( envMap, envMapRotation * worldNormal, ENVMAP_MAX_LOD );
			return PI * envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	vec3 getIBLRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness ) {
		#ifdef ENVMAP_TYPE_PMREM
			vec3 reflectVec = reflect( - viewDir, normal );
			reflectVec = normalize( mix( reflectVec, normal, pow4( roughness ) ) );
			reflectVec = transformDirectionByInverseViewMatrix( reflectVec, viewMatrix );
			vec4 envMapColor = textureLod( envMap, envMapRotation * reflectVec, roughnessToMip( roughness ) );
			return envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	#ifdef USE_RETROREFLECTION
		vec3 getIBLRetroRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness ) {
			#ifdef ENVMAP_TYPE_PMREM
				vec3 retroVec = normalize( mix( viewDir, normal, pow4( roughness ) ) );
				retroVec = transformDirectionByInverseViewMatrix( retroVec, viewMatrix );
				vec4 envMapColor = textureLod( envMap, envMapRotation * retroVec, roughnessToMip( roughness ) );
				return envMapColor.rgb * envMapIntensity;
			#else
				return vec3( 0.0 );
			#endif
		}
	#endif
	#ifdef USE_ANISOTROPY
		vec3 getIBLAnisotropyRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness, const in vec3 bitangent, const in float anisotropy ) {
			#ifdef ENVMAP_TYPE_PMREM
				vec3 bentNormal = cross( bitangent, viewDir );
				bentNormal = normalize( cross( bentNormal, bitangent ) );
				bentNormal = normalize( mix( bentNormal, normal, pow2( pow2( 1.0 - anisotropy * ( 1.0 - roughness ) ) ) ) );
				return getIBLRadiance( viewDir, bentNormal, roughness );
			#else
				return vec3( 0.0 );
			#endif
		}
		#ifdef USE_RETROREFLECTION
			vec3 getIBLAnisotropyRetroRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness, const in vec3 bitangent, const in float anisotropy ) {
				#ifdef ENVMAP_TYPE_PMREM
					vec3 bentNormal = cross( bitangent, viewDir );
					bentNormal = normalize( cross( bentNormal, bitangent ) );
					bentNormal = normalize( mix( bentNormal, normal, pow2( pow2( 1.0 - anisotropy * ( 1.0 - roughness ) ) ) ) );
					return getIBLRetroRadiance( viewDir, bentNormal, roughness );
				#else
					return vec3( 0.0 );
				#endif
			}
		#endif
	#endif
#endif`,pA=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,mA=`varying vec3 vViewPosition;
struct ToonMaterial {
	vec3 diffuseColor;
};
void RE_Direct_Toon( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 irradiance = getGradientIrradiance( geometryNormal, directLight.direction ) * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Toon( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Toon
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,gA=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,yA=`varying vec3 vViewPosition;
struct BlinnPhongMaterial {
	vec3 diffuseColor;
	vec3 specularColor;
	float specularShininess;
	float specularStrength;
};
void RE_Direct_BlinnPhong( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
	reflectedLight.directSpecular += irradiance * BRDF_BlinnPhong( directLight.direction, geometryViewDir, geometryNormal, material.specularColor, material.specularShininess ) * material.specularStrength;
}
void RE_IndirectDiffuse_BlinnPhong( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_BlinnPhong
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,vA=`PhysicalMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.diffuseContribution = diffuseColor.rgb * ( 1.0 - metalnessFactor );
material.metalness = metalnessFactor;
vec3 dxy = max( abs( dFdx( nonPerturbedNormal ) ), abs( dFdy( nonPerturbedNormal ) ) );
float geometryRoughness = max( max( dxy.x, dxy.y ), dxy.z );
material.roughness = max( roughnessFactor, 0.045 );
material.roughness += geometryRoughness;
material.roughness = min( material.roughness, 1.0 );
#ifdef USE_DIFFUSE_ROUGHNESS
	float diffuseRoughnessFactor = diffuseRoughness;
	#ifdef USE_DIFFUSE_ROUGHNESSMAP
		diffuseRoughnessFactor *= texture2D( diffuseRoughnessMap, vDiffuseRoughnessMapUv ).r;
	#endif
	material.diffuseRoughness = saturate( diffuseRoughnessFactor );
#endif
#ifdef IOR
	material.ior = ior;
	#ifdef USE_SPECULAR
		float specularIntensityFactor = specularIntensity;
		vec3 specularColorFactor = specularColor;
		#ifdef USE_SPECULAR_COLORMAP
			specularColorFactor *= texture2D( specularColorMap, vSpecularColorMapUv ).rgb;
		#endif
		#ifdef USE_SPECULAR_INTENSITYMAP
			specularIntensityFactor *= texture2D( specularIntensityMap, vSpecularIntensityMapUv ).a;
		#endif
		material.specularF90 = mix( specularIntensityFactor, 1.0, metalnessFactor );
	#else
		float specularIntensityFactor = 1.0;
		vec3 specularColorFactor = vec3( 1.0 );
		material.specularF90 = 1.0;
	#endif
	material.specularColor = min( pow2( ( material.ior - 1.0 ) / ( material.ior + 1.0 ) ) * specularColorFactor, vec3( 1.0 ) ) * specularIntensityFactor;
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
#else
	material.specularColor = vec3( 0.04 );
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
	material.specularF90 = 1.0;
#endif
#ifdef USE_CLEARCOAT
	material.clearcoat = clearcoat;
	material.clearcoatRoughness = clearcoatRoughness;
	material.clearcoatF0 = vec3( 0.04 );
	material.clearcoatF90 = 1.0;
	#ifdef USE_CLEARCOATMAP
		material.clearcoat *= texture2D( clearcoatMap, vClearcoatMapUv ).x;
	#endif
	#ifdef USE_CLEARCOAT_ROUGHNESSMAP
		material.clearcoatRoughness *= texture2D( clearcoatRoughnessMap, vClearcoatRoughnessMapUv ).y;
	#endif
	material.clearcoat = saturate( material.clearcoat );	material.clearcoatRoughness = max( material.clearcoatRoughness, 0.045 );
	material.clearcoatRoughness += geometryRoughness;
	material.clearcoatRoughness = min( material.clearcoatRoughness, 1.0 );
#endif
#ifdef USE_DISPERSION
	material.dispersion = dispersion;
#endif
#ifdef USE_RETROREFLECTION
	material.retroreflectivity = retroreflectivity;
#endif
#ifdef USE_IRIDESCENCE
	material.iridescence = iridescence;
	material.iridescenceIOR = iridescenceIOR;
	#ifdef USE_IRIDESCENCEMAP
		material.iridescence *= texture2D( iridescenceMap, vIridescenceMapUv ).r;
	#endif
	#ifdef USE_IRIDESCENCE_THICKNESSMAP
		material.iridescenceThickness = (iridescenceThicknessMaximum - iridescenceThicknessMinimum) * texture2D( iridescenceThicknessMap, vIridescenceThicknessMapUv ).g + iridescenceThicknessMinimum;
	#else
		material.iridescenceThickness = iridescenceThicknessMaximum;
	#endif
#endif
#ifdef USE_SHEEN
	material.sheenColor = sheenColor;
	#ifdef USE_SHEEN_COLORMAP
		material.sheenColor *= texture2D( sheenColorMap, vSheenColorMapUv ).rgb;
	#endif
	material.sheenRoughness = clamp( sheenRoughness, 0.0001, 1.0 );
	#ifdef USE_SHEEN_ROUGHNESSMAP
		material.sheenRoughness *= texture2D( sheenRoughnessMap, vSheenRoughnessMapUv ).a;
	#endif
#endif
#ifdef USE_ANISOTROPY
	#ifdef USE_ANISOTROPYMAP
		mat2 anisotropyMat = mat2( anisotropyVector.x, anisotropyVector.y, - anisotropyVector.y, anisotropyVector.x );
		vec3 anisotropyPolar = texture2D( anisotropyMap, vAnisotropyMapUv ).rgb;
		vec2 anisotropyV = anisotropyMat * normalize( 2.0 * anisotropyPolar.rg - vec2( 1.0 ) ) * anisotropyPolar.b;
	#else
		vec2 anisotropyV = anisotropyVector;
	#endif
	material.anisotropy = length( anisotropyV );
	if( material.anisotropy == 0.0 ) {
		anisotropyV = vec2( 1.0, 0.0 );
	} else {
		anisotropyV /= material.anisotropy;
		material.anisotropy = saturate( material.anisotropy );
	}
	material.alphaT = mix( pow2( material.roughness ), 1.0, pow2( material.anisotropy ) );
	material.anisotropyT = tbn[ 0 ] * anisotropyV.x + tbn[ 1 ] * anisotropyV.y;
	material.anisotropyB = tbn[ 1 ] * anisotropyV.x - tbn[ 0 ] * anisotropyV.y;
#endif`,_A=`uniform sampler2D dfgLUT;
struct PhysicalMaterial {
	vec3 diffuseColor;
	vec3 diffuseContribution;
	vec3 specularColor;
	vec3 specularColorBlended;
	float roughness;
	float metalness;
	float specularF90;
	float dispersion;
	vec2 dfg;
	vec3 multiScatteringCompensation;
	#ifdef USE_DIFFUSE_ROUGHNESS
		float diffuseRoughness;
	#endif
	#ifdef USE_RETROREFLECTION
		float retroreflectivity;
	#endif
	#ifdef USE_CLEARCOAT
		float clearcoat;
		float clearcoatRoughness;
		vec3 clearcoatF0;
		float clearcoatF90;
	#endif
	#ifdef USE_IRIDESCENCE
		float iridescence;
		float iridescenceIOR;
		float iridescenceThickness;
		vec3 iridescenceFresnel;
		vec3 iridescenceF0Dielectric;
		vec3 iridescenceF0Metallic;
	#endif
	#ifdef USE_SHEEN
		vec3 sheenColor;
		float sheenRoughness;
	#endif
	#ifdef IOR
		float ior;
	#endif
	#ifdef USE_TRANSMISSION
		float transmission;
		float transmissionAlpha;
		float thickness;
		float attenuationDistance;
		vec3 attenuationColor;
	#endif
	#ifdef USE_ANISOTROPY
		float anisotropy;
		float alphaT;
		vec3 anisotropyT;
		vec3 anisotropyB;
	#endif
};
vec3 clearcoatSpecularDirect = vec3( 0.0 );
vec3 clearcoatSpecularIndirect = vec3( 0.0 );
vec3 sheenSpecularDirect = vec3( 0.0 );
vec3 sheenSpecularIndirect = vec3(0.0 );
vec3 Schlick_to_F0( const in vec3 f, const in float f90, const in float dotVH ) {
    float x = clamp( 1.0 - dotVH, 0.0, 1.0 );
    float x2 = x * x;
    float x5 = clamp( x * x2 * x2, 0.0, 0.9999 );
    return ( f - vec3( f90 ) * x5 ) / ( 1.0 - x5 );
}
#ifdef USE_DIFFUSE_ROUGHNESS
	const float EON_EPSILON = 1e-7;
	float FON_DirectionalAlbedo( const in float mu, const in float roughness, const in float A ) {
		float muComp = 1.0 - mu;
		float gOverPi = muComp * ( 0.0571085289 + muComp * ( 0.491881867 + muComp * ( - 0.332181442 + muComp * 0.0714429953 ) ) );
		return A * ( 1.0 + roughness * gOverPi );
	}
	vec3 BRDF_EON( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in vec3 diffuseColor, const in float roughness ) {
		vec3 rho = saturate( diffuseColor );
		if ( roughness <= EON_EPSILON ) return BRDF_Lambert( rho );
		float dotNL = saturate( dot( normal, lightDir ) );
		float dotNV = saturate( dot( normal, viewDir ) );
		float s = dot( lightDir, viewDir ) - dotNL * dotNV;
		float sOverT = ( s > 0.0 ) ? s / max( max( dotNL, dotNV ), EON_EPSILON ) : s;
		float A = 1.0 / ( 1.0 + ( 0.5 - 2.0 / ( 3.0 * PI ) ) * roughness );
		vec3 singleScatter = rho * RECIPROCAL_PI * A * ( 1.0 + roughness * sOverT );
		float averageAlbedo = A * ( 1.0 + ( 2.0 / 3.0 - 28.0 / ( 15.0 * PI ) ) * roughness );
		float albedoV = FON_DirectionalAlbedo( dotNV, roughness, A );
		float albedoL = FON_DirectionalAlbedo( dotNL, roughness, A );
		vec3 rhoMultiScatter = rho * rho * averageAlbedo / max( vec3( EON_EPSILON ), vec3( 1.0 ) - rho * ( 1.0 - averageAlbedo ) );
		vec3 multiScatter = rhoMultiScatter * RECIPROCAL_PI
			* max( EON_EPSILON, 1.0 - albedoV )
			* max( EON_EPSILON, 1.0 - albedoL )
			/ max( EON_EPSILON, 1.0 - averageAlbedo );
		return singleScatter + multiScatter;
	}
	vec3 EON_DirectionalAlbedo( const in vec3 diffuseColor, const in float roughness, const in float dotNV ) {
		vec3 rho = saturate( diffuseColor );
		if ( roughness <= EON_EPSILON ) return rho;
		float A = 1.0 / ( 1.0 + ( 0.5 - 2.0 / ( 3.0 * PI ) ) * roughness );
		float directionalAlbedo = FON_DirectionalAlbedo( dotNV, roughness, A );
		float averageAlbedo = A * ( 1.0 + ( 2.0 / 3.0 - 28.0 / ( 15.0 * PI ) ) * roughness );
		vec3 rhoMultiScatter = rho * rho * averageAlbedo / max( vec3( EON_EPSILON ), vec3( 1.0 ) - rho * ( 1.0 - averageAlbedo ) );
		return rho * directionalAlbedo + rhoMultiScatter * ( 1.0 - directionalAlbedo );
	}
#endif
float V_GGX_SmithCorrelated( const in float alpha, const in float dotNL, const in float dotNV ) {
	float a2 = pow2( alpha );
	float gv = dotNL * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNV ) );
	float gl = dotNV * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNL ) );
	return 0.5 / max( gv + gl, EPSILON );
}
float D_GGX( const in float alpha, const in float dotNH ) {
	float a2 = pow2( alpha );
	float denom = pow2( dotNH ) * ( a2 - 1.0 ) + 1.0;
	return RECIPROCAL_PI * a2 / pow2( denom );
}
#ifdef USE_ANISOTROPY
	float V_GGX_SmithCorrelated_Anisotropic( const in float alphaT, const in float alphaB, const in float dotTV, const in float dotBV, const in float dotTL, const in float dotBL, const in float dotNV, const in float dotNL ) {
		float gv = dotNL * length( vec3( alphaT * dotTV, alphaB * dotBV, dotNV ) );
		float gl = dotNV * length( vec3( alphaT * dotTL, alphaB * dotBL, dotNL ) );
		return 0.5 / max( gv + gl, EPSILON );
	}
	float D_GGX_Anisotropic( const in float alphaT, const in float alphaB, const in float dotNH, const in float dotTH, const in float dotBH ) {
		float a2 = alphaT * alphaB;
		highp vec3 v = vec3( alphaB * dotTH, alphaT * dotBH, a2 * dotNH );
		highp float v2 = dot( v, v );
		float w2 = a2 / v2;
		return RECIPROCAL_PI * a2 * pow2 ( w2 );
	}
#endif
#ifdef USE_CLEARCOAT
	vec3 BRDF_GGX_Clearcoat( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material) {
		vec3 f0 = material.clearcoatF0;
		float f90 = material.clearcoatF90;
		float roughness = max( material.clearcoatRoughness, 0.045 );
		float alpha = pow2( roughness );
		vec3 halfDir = normalize( lightDir + viewDir );
		float dotNL = saturate( dot( normal, lightDir ) );
		float dotNV = saturate( dot( normal, viewDir ) );
		float dotNH = saturate( dot( normal, halfDir ) );
		float dotVH = saturate( dot( viewDir, halfDir ) );
		vec3 F = F_Schlick( f0, f90, dotVH );
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
		return F * ( V * D );
	}
#endif
vec3 BRDF_GGX( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 f0 = material.specularColorBlended;
	float f90 = material.specularF90;
	float roughness = max( material.roughness, 0.045 );
	float alpha = pow2( roughness );
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( f0, f90, dotVH );
	#ifdef USE_IRIDESCENCE
		F = mix( F, material.iridescenceFresnel, material.iridescence );
	#endif
	#ifdef USE_ANISOTROPY
		float dotTL = dot( material.anisotropyT, lightDir );
		float dotTV = dot( material.anisotropyT, viewDir );
		float dotTH = dot( material.anisotropyT, halfDir );
		float dotBL = dot( material.anisotropyB, lightDir );
		float dotBV = dot( material.anisotropyB, viewDir );
		float dotBH = dot( material.anisotropyB, halfDir );
		float alphaT = max( material.alphaT, alpha );
		float V = V_GGX_SmithCorrelated_Anisotropic( alphaT, alpha, dotTV, dotBV, dotTL, dotBL, dotNV, dotNL );
		float D = D_GGX_Anisotropic( alphaT, alpha, dotNH, dotTH, dotBH );
	#else
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
	#endif
	return F * ( V * D );
}
vec2 LTC_Uv( const in vec3 N, const in vec3 V, const in float roughness ) {
	const float LUT_SIZE = 64.0;
	const float LUT_SCALE = ( LUT_SIZE - 1.0 ) / LUT_SIZE;
	const float LUT_BIAS = 0.5 / LUT_SIZE;
	float dotNV = saturate( dot( N, V ) );
	vec2 uv = vec2( roughness, sqrt( 1.0 - dotNV ) );
	uv = uv * LUT_SCALE + LUT_BIAS;
	return uv;
}
float LTC_ClippedSphereFormFactor( const in vec3 f ) {
	float l = length( f );
	return max( ( l * l + f.z ) / ( l + 1.0 ), 0.0 );
}
vec3 LTC_EdgeVectorFormFactor( const in vec3 v1, const in vec3 v2 ) {
	float x = dot( v1, v2 );
	float y = abs( x );
	float a = 0.8543985 + ( 0.4965155 + 0.0145206 * y ) * y;
	float b = 3.4175940 + ( 4.1616724 + y ) * y;
	float v = a / b;
	float theta_sintheta = ( x > 0.0 ) ? v : 0.5 * inversesqrt( max( 1.0 - x * x, 1e-7 ) ) - v;
	return cross( v1, v2 ) * theta_sintheta;
}
vec3 LTC_Evaluate( const in vec3 N, const in vec3 V, const in vec3 P, const in mat3 mInv, const in vec3 rectCoords[ 4 ] ) {
	vec3 v1 = rectCoords[ 1 ] - rectCoords[ 0 ];
	vec3 v2 = rectCoords[ 3 ] - rectCoords[ 0 ];
	vec3 lightNormal = cross( v1, v2 );
	if( dot( lightNormal, P - rectCoords[ 0 ] ) < 0.0 ) return vec3( 0.0 );
	vec3 T1, T2;
	T1 = normalize( V - N * dot( V, N ) );
	T2 = - cross( N, T1 );
	mat3 mat = mInv * transpose( mat3( T1, T2, N ) );
	vec3 coords[ 4 ];
	coords[ 0 ] = mat * ( rectCoords[ 0 ] - P );
	coords[ 1 ] = mat * ( rectCoords[ 1 ] - P );
	coords[ 2 ] = mat * ( rectCoords[ 2 ] - P );
	coords[ 3 ] = mat * ( rectCoords[ 3 ] - P );
	coords[ 0 ] = normalize( coords[ 0 ] );
	coords[ 1 ] = normalize( coords[ 1 ] );
	coords[ 2 ] = normalize( coords[ 2 ] );
	coords[ 3 ] = normalize( coords[ 3 ] );
	vec3 vectorFormFactor = vec3( 0.0 );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 0 ], coords[ 1 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 1 ], coords[ 2 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 2 ], coords[ 3 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 3 ], coords[ 0 ] );
	float result = LTC_ClippedSphereFormFactor( vectorFormFactor );
	return vec3( result );
}
#if defined( USE_SHEEN )
float D_Charlie( float roughness, float dotNH ) {
	float alpha = pow2( roughness );
	float invAlpha = 1.0 / alpha;
	float cos2h = dotNH * dotNH;
	float sin2h = max( 1.0 - cos2h, 0.0078125 );
	return ( 2.0 + invAlpha ) * pow( sin2h, invAlpha * 0.5 ) / ( 2.0 * PI );
}
float V_Neubelt( float dotNV, float dotNL ) {
	return saturate( 1.0 / ( 4.0 * ( dotNL + dotNV - dotNL * dotNV ) ) );
}
vec3 BRDF_Sheen( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, vec3 sheenColor, const in float sheenRoughness ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float D = D_Charlie( sheenRoughness, dotNH );
	float V = V_Neubelt( dotNV, dotNL );
	return sheenColor * ( D * V );
}
#endif
float IBLSheenBRDF( const in vec3 normal, const in vec3 viewDir, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	float r2 = roughness * roughness;
	float rInv = 1.0 / ( roughness + 0.1 );
	float a = -1.9362 + 1.0678 * roughness + 0.4573 * r2 - 0.8469 * rInv;
	float b = -0.6014 + 0.5538 * roughness - 0.4670 * r2 - 0.1255 * rInv;
	float DG = exp( a * dotNV + b );
	return saturate( DG );
}
vec3 EnvironmentBRDF( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	return specularColor * fab.x + specularF90 * fab.y;
}
#ifdef USE_IRIDESCENCE
void computeMultiscatteringIridescence( const in vec2 fab, const in vec3 specularColor, const in float specularF90, const in float iridescence, const in vec3 iridescenceF0, inout vec3 singleScatter, inout vec3 multiScatter ) {
#else
void computeMultiscattering( const in vec2 fab, const in vec3 specularColor, const in float specularF90, inout vec3 singleScatter, inout vec3 multiScatter ) {
#endif
	#ifdef USE_IRIDESCENCE
		vec3 Fr = mix( specularColor, iridescenceF0, iridescence );
	#else
		vec3 Fr = specularColor;
	#endif
	vec3 FssEss = Fr * fab.x + specularF90 * fab.y;
	float Ess = fab.x + fab.y;
	float Ems = 1.0 - Ess;
	vec3 Favg = Fr + ( 1.0 - Fr ) * 0.047619;	vec3 Fms = FssEss * Favg / ( 1.0 - Ems * Favg );
	singleScatter += FssEss;
	multiScatter += Fms * Ems;
}
#if NUM_RECT_AREA_LIGHTS > 0
	void RE_Direct_RectArea_Physical( const in RectAreaLight rectAreaLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
		vec3 normal = geometryNormal;
		vec3 viewDir = geometryViewDir;
		vec3 position = geometryPosition;
		vec3 lightPos = rectAreaLight.position;
		vec3 halfWidth = rectAreaLight.halfWidth;
		vec3 halfHeight = rectAreaLight.halfHeight;
		vec3 lightColor = rectAreaLight.color;
		float roughness = material.roughness;
		vec3 rectCoords[ 4 ];
		rectCoords[ 0 ] = lightPos + halfWidth - halfHeight;		rectCoords[ 1 ] = lightPos - halfWidth - halfHeight;
		rectCoords[ 2 ] = lightPos - halfWidth + halfHeight;
		rectCoords[ 3 ] = lightPos + halfWidth + halfHeight;
		vec2 uv = LTC_Uv( normal, viewDir, roughness );
		vec4 t1 = texture2D( ltc_1, uv );
		vec4 t2 = texture2D( ltc_2, uv );
		mat3 mInv = mat3(
			vec3( t1.x, 0, t1.y ),
			vec3(    0, 1,    0 ),
			vec3( t1.z, 0, t1.w )
		);
		vec3 fresnel = ( material.specularColorBlended * t2.x + ( material.specularF90 - material.specularColorBlended ) * t2.y );
		reflectedLight.directSpecular += lightColor * fresnel * LTC_Evaluate( normal, viewDir, position, mInv, rectCoords );
		reflectedLight.directDiffuse += lightColor * material.diffuseContribution * LTC_Evaluate( normal, viewDir, position, mat3( 1.0 ), rectCoords );
		#ifdef USE_CLEARCOAT
			vec3 Ncc = geometryClearcoatNormal;
			vec2 uvClearcoat = LTC_Uv( Ncc, viewDir, material.clearcoatRoughness );
			vec4 t1Clearcoat = texture2D( ltc_1, uvClearcoat );
			vec4 t2Clearcoat = texture2D( ltc_2, uvClearcoat );
			mat3 mInvClearcoat = mat3(
				vec3( t1Clearcoat.x, 0, t1Clearcoat.y ),
				vec3(             0, 1,             0 ),
				vec3( t1Clearcoat.z, 0, t1Clearcoat.w )
			);
			vec3 fresnelClearcoat = material.clearcoatF0 * t2Clearcoat.x + ( material.clearcoatF90 - material.clearcoatF0 ) * t2Clearcoat.y;
			clearcoatSpecularDirect += lightColor * fresnelClearcoat * LTC_Evaluate( Ncc, viewDir, position, mInvClearcoat, rectCoords );
		#endif
	}
#endif
void RE_Direct_Physical( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	#ifdef USE_CLEARCOAT
		float dotNLcc = saturate( dot( geometryClearcoatNormal, directLight.direction ) );
		vec3 ccIrradiance = dotNLcc * directLight.color;
		clearcoatSpecularDirect += ccIrradiance * BRDF_GGX_Clearcoat( directLight.direction, geometryViewDir, geometryClearcoatNormal, material );
	#endif
	#ifdef USE_SHEEN
 
 		sheenSpecularDirect += irradiance * BRDF_Sheen( directLight.direction, geometryViewDir, geometryNormal, material.sheenColor, material.sheenRoughness );
 
 		float sheenAlbedoV = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
 		float sheenAlbedoL = IBLSheenBRDF( geometryNormal, directLight.direction, material.sheenRoughness );
 
 		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * max( sheenAlbedoV, sheenAlbedoL );
 
 		irradiance *= sheenEnergyComp;
 
 	#endif
	vec3 specularBRDF = BRDF_GGX( directLight.direction, geometryViewDir, geometryNormal, material );
	#ifdef USE_RETROREFLECTION
		vec3 retroViewDir = reflect( - geometryViewDir, geometryNormal );
		vec3 retroSpecularBRDF = BRDF_GGX( directLight.direction, retroViewDir, geometryNormal, material );
		specularBRDF = mix( specularBRDF, retroSpecularBRDF, saturate( material.retroreflectivity ) );
	#endif
	reflectedLight.directSpecular += irradiance * specularBRDF * material.multiScatteringCompensation;
	vec3 halfDir = normalize( directLight.direction + geometryViewDir );
	float dotVH = saturate( dot( geometryViewDir, halfDir ) );
	vec3 F = F_Schlick( material.specularColor, material.specularF90, dotVH );
	#ifdef USE_RETROREFLECTION
		vec3 retroHalfDir = normalize( directLight.direction + retroViewDir );
		float dotRetroVH = saturate( dot( retroViewDir, retroHalfDir ) );
		vec3 retroF = F_Schlick( material.specularColor, material.specularF90, dotRetroVH );
		F = mix( F, retroF, saturate( material.retroreflectivity ) );
	#endif
	#ifdef USE_DIFFUSE_ROUGHNESS
		vec3 diffuseBRDF = BRDF_EON( directLight.direction, geometryViewDir, geometryNormal, material.diffuseColor, material.diffuseRoughness ) * ( 1.0 - material.metalness );
	#else
		vec3 diffuseBRDF = BRDF_Lambert( material.diffuseContribution );
	#endif
	reflectedLight.directDiffuse += irradiance * diffuseBRDF * ( 1.0 - F );
}
void RE_IndirectDiffuse_Physical( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 singleScattering = vec3( 0.0 );
	vec3 multiScattering = vec3( 0.0 );
	#ifdef USE_IRIDESCENCE
		computeMultiscatteringIridescence( material.dfg, material.specularColor, material.specularF90, material.iridescence, material.iridescenceF0Dielectric, singleScattering, multiScattering );
	#else
		computeMultiscattering( material.dfg, material.specularColor, material.specularF90, singleScattering, multiScattering );
	#endif
	#ifdef USE_DIFFUSE_ROUGHNESS
		float dotNV = saturate( dot( geometryNormal, geometryViewDir ) );
		vec3 diffuseAlbedo = EON_DirectionalAlbedo( material.diffuseColor, material.diffuseRoughness, dotNV ) * ( 1.0 - material.metalness );
		vec3 diffuse = irradiance * RECIPROCAL_PI * diffuseAlbedo * ( 1.0 - singleScattering - multiScattering );
	#else
		vec3 diffuse = irradiance * BRDF_Lambert( material.diffuseContribution ) * ( 1.0 - singleScattering - multiScattering );
	#endif
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		sheenSpecularIndirect += irradiance * material.sheenColor * sheenAlbedo * RECIPROCAL_PI;
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		diffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectDiffuse += diffuse;
}
void RE_IndirectSpecular_Physical( const in vec3 radiance, const in vec3 irradiance, const in vec3 clearcoatRadiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight) {
	#ifdef USE_CLEARCOAT
		clearcoatSpecularIndirect += clearcoatRadiance * EnvironmentBRDF( geometryClearcoatNormal, geometryViewDir, material.clearcoatF0, material.clearcoatF90, material.clearcoatRoughness );
	#endif
	#ifdef USE_SHEEN
		sheenSpecularIndirect += irradiance * material.sheenColor * IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness ) * RECIPROCAL_PI;
 	#endif
	vec3 singleScatteringDielectric = vec3( 0.0 );
	vec3 multiScatteringDielectric = vec3( 0.0 );
	vec3 singleScatteringMetallic = vec3( 0.0 );
	vec3 multiScatteringMetallic = vec3( 0.0 );
	#ifdef USE_IRIDESCENCE
		computeMultiscatteringIridescence( material.dfg, material.specularColor, material.specularF90, material.iridescence, material.iridescenceF0Dielectric, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscatteringIridescence( material.dfg, material.diffuseColor, material.specularF90, material.iridescence, material.iridescenceF0Metallic, singleScatteringMetallic, multiScatteringMetallic );
	#else
		computeMultiscattering( material.dfg, material.specularColor, material.specularF90, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscattering( material.dfg, material.diffuseColor, material.specularF90, singleScatteringMetallic, multiScatteringMetallic );
	#endif
	vec3 singleScattering = mix( singleScatteringDielectric, singleScatteringMetallic, material.metalness );
	vec3 multiScattering = mix( multiScatteringDielectric, multiScatteringMetallic, material.metalness );
	vec3 totalScatteringDielectric = singleScatteringDielectric + multiScatteringDielectric;
	#ifdef USE_DIFFUSE_ROUGHNESS
		float dotNV = saturate( dot( geometryNormal, geometryViewDir ) );
		vec3 diffuse = EON_DirectionalAlbedo( material.diffuseColor, material.diffuseRoughness, dotNV ) * ( 1.0 - material.metalness ) * ( 1.0 - totalScatteringDielectric );
	#else
		vec3 diffuse = material.diffuseContribution * ( 1.0 - totalScatteringDielectric );
	#endif
	vec3 cosineWeightedIrradiance = irradiance * RECIPROCAL_PI;
	vec3 indirectSpecular = radiance * singleScattering;
	indirectSpecular += multiScattering * cosineWeightedIrradiance;
	vec3 indirectDiffuse = diffuse * cosineWeightedIrradiance;
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		indirectSpecular *= sheenEnergyComp;
		indirectDiffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectSpecular += indirectSpecular;
	reflectedLight.indirectDiffuse += indirectDiffuse;
}
#define RE_Direct				RE_Direct_Physical
#define RE_Direct_RectArea		RE_Direct_RectArea_Physical
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Physical
#define RE_IndirectSpecular		RE_IndirectSpecular_Physical
float computeSpecularOcclusion( const in float dotNV, const in float ambientOcclusion, const in float roughness ) {
	return saturate( pow( dotNV + ambientOcclusion, exp2( - 16.0 * roughness - 1.0 ) ) - 1.0 + ambientOcclusion );
}`,xA=`
vec3 geometryPosition = - vViewPosition;
vec3 geometryNormal = normal;
vec3 geometryViewDir = ( isOrthographic ) ? vec3( 0, 0, 1 ) : normalize( vViewPosition );
vec3 geometryClearcoatNormal = vec3( 0.0 );
#ifdef USE_CLEARCOAT
	geometryClearcoatNormal = clearcoatNormal;
#endif
#ifdef USE_IRIDESCENCE
	float dotNVi = saturate( dot( normal, geometryViewDir ) );
	if ( material.iridescenceThickness == 0.0 ) {
		material.iridescence = 0.0;
	} else {
		material.iridescence = saturate( material.iridescence );
	}
	if ( material.iridescence > 0.0 ) {
		vec3 iridescenceFresnelDielectric = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.specularColor );
		vec3 iridescenceFresnelMetallic = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.diffuseColor );
		material.iridescenceFresnel = mix( iridescenceFresnelDielectric, iridescenceFresnelMetallic, material.metalness );
		material.iridescenceF0Dielectric = Schlick_to_F0( iridescenceFresnelDielectric, 1.0, dotNVi );
		material.iridescenceF0Metallic = Schlick_to_F0( iridescenceFresnelMetallic, 1.0, dotNVi );
	}
#endif
#ifdef STANDARD
	float dotNVms = saturate( dot( geometryNormal, geometryViewDir ) );
	material.dfg = texture2D( dfgLUT, vec2( material.roughness, dotNVms ) ).rg;
	#if ( NUM_SUN_LIGHTS > 0 || NUM_DIR_LIGHTS > 0 || NUM_POINT_LIGHTS > 0 || NUM_SPOT_LIGHTS > 0 )
		float EssMs = material.dfg.x + material.dfg.y;
		material.multiScatteringCompensation = 1.0 + material.specularColorBlended * ( 1.0 / EssMs - 1.0 );
	#endif
#endif
IncidentLight directLight;
#if ( NUM_POINT_LIGHTS > 0 ) && defined( RE_Direct )
	PointLight pointLight;
	#if defined( USE_SHADOWMAP ) && NUM_POINT_LIGHT_SHADOWS > 0
	PointLightShadow pointLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHTS; i ++ ) {
		pointLight = pointLights[ i ];
		getPointLightInfo( pointLight, geometryPosition, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_POINT_LIGHT_SHADOWS ) && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
		pointLightShadow = pointLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getPointShadow( pointShadowMap[ i ], pointLightShadow.shadowMapSize, pointLightShadow.shadowIntensity, pointLightShadow.shadowBias, pointLightShadow.shadowRadius, vPointShadowCoord[ i ], pointLightShadow.shadowCameraNear, pointLightShadow.shadowCameraFar ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_SPOT_LIGHTS > 0 ) && defined( RE_Direct )
	SpotLight spotLight;
	vec4 spotColor;
	vec3 spotLightCoord;
	bool inSpotLightMap;
	#if defined( USE_SHADOWMAP ) && NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHTS; i ++ ) {
		spotLight = spotLights[ i ];
		getSpotLightInfo( spotLight, geometryPosition, directLight );
		#if ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#define SPOT_LIGHT_MAP_INDEX UNROLLED_LOOP_INDEX
		#elif ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		#define SPOT_LIGHT_MAP_INDEX NUM_SPOT_LIGHT_MAPS
		#else
		#define SPOT_LIGHT_MAP_INDEX ( UNROLLED_LOOP_INDEX - NUM_SPOT_LIGHT_SHADOWS + NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#endif
		#if ( SPOT_LIGHT_MAP_INDEX < NUM_SPOT_LIGHT_MAPS )
			spotLightCoord = vSpotLightCoord[ i ].xyz / vSpotLightCoord[ i ].w;
			inSpotLightMap = all( lessThan( abs( spotLightCoord * 2. - 1. ), vec3( 1.0 ) ) );
			spotColor = texture2D( spotLightMap[ SPOT_LIGHT_MAP_INDEX ], spotLightCoord.xy );
			directLight.color = inSpotLightMap ? directLight.color * spotColor.rgb : directLight.color;
		#endif
		#undef SPOT_LIGHT_MAP_INDEX
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		spotLightShadow = spotLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( spotShadowMap[ i ], spotLightShadow.shadowMapSize, spotLightShadow.shadowIntensity, spotLightShadow.shadowBias, spotLightShadow.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_SUN_LIGHTS > 0 ) && defined( RE_Direct )
	SunLight sunLight;
	#if defined( USE_SHADOWMAP ) && NUM_SUN_LIGHT_SHADOWS > 0
	SunLightShadow sunLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SUN_LIGHTS; i ++ ) {
		sunLight = sunLights[ i ];
		getSunLightInfo( sunLight, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_SUN_LIGHT_SHADOWS )
		sunLightShadow = sunLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getSunShadow( sunShadowMap[ i ], sunLightShadow, UNROLLED_LOOP_INDEX ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_DIR_LIGHTS > 0 ) && defined( RE_Direct )
	DirectionalLight directionalLight;
	#if defined( USE_SHADOWMAP ) && NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHTS; i ++ ) {
		directionalLight = directionalLights[ i ];
		getDirectionalLightInfo( directionalLight, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_DIR_LIGHT_SHADOWS )
		directionalLightShadow = directionalLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( directionalShadowMap[ i ], directionalLightShadow.shadowMapSize, directionalLightShadow.shadowIntensity, directionalLightShadow.shadowBias, directionalLightShadow.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_RECT_AREA_LIGHTS > 0 ) && defined( RE_Direct_RectArea )
	RectAreaLight rectAreaLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_RECT_AREA_LIGHTS; i ++ ) {
		rectAreaLight = rectAreaLights[ i ];
		RE_Direct_RectArea( rectAreaLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if defined( RE_IndirectDiffuse )
	vec3 iblIrradiance = vec3( 0.0 );
	vec3 irradiance = getAmbientLightIrradiance( ambientLightColor );
	#if defined( USE_LIGHT_PROBES )
		irradiance += getLightProbeIrradiance( lightProbe, geometryNormal );
	#endif
	#if ( NUM_HEMI_LIGHTS > 0 )
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_HEMI_LIGHTS; i ++ ) {
			irradiance += getHemisphereLightIrradiance( hemisphereLights[ i ], geometryNormal );
		}
		#pragma unroll_loop_end
	#endif
	#if NUM_LIGHT_PROBE_GRIDS > 0
		vec3 probeWorldPos = ( ( vec4( geometryPosition, 1.0 ) - viewMatrix[ 3 ] ) * viewMatrix ).xyz;
		vec3 probeWorldNormal = transformNormalByInverseViewMatrix( geometryNormal, viewMatrix );
		int probeGrid = - 1;
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_LIGHT_PROBE_GRIDS; i ++ ) {
			if ( isInsideLightProbeGrid( probeWorldPos, probeWorldNormal, probesMin[ i ], probesMax[ i ], probesResolution[ i ] ) ) probeGrid = UNROLLED_LOOP_INDEX;
		}
		#pragma unroll_loop_end
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_LIGHT_PROBE_GRIDS; i ++ ) {
			if ( probeGrid == UNROLLED_LOOP_INDEX ) irradiance += getLightProbeGridIrradiance( probesSH[ i ], probesMin[ i ], probesMax[ i ], probesResolution[ i ], probeWorldPos, probeWorldNormal );
		}
		#pragma unroll_loop_end
	#endif
#endif
#if defined( RE_IndirectSpecular )
	vec3 radiance = vec3( 0.0 );
	vec3 clearcoatRadiance = vec3( 0.0 );
#endif`,MA=`#if defined( RE_IndirectDiffuse )
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		vec3 lightMapIrradiance = lightMapTexel.rgb * lightMapIntensity;
		irradiance += lightMapIrradiance;
	#endif
	#if defined( USE_ENVMAP ) && defined( ENVMAP_TYPE_PMREM )
		#if defined( STANDARD ) || defined( LAMBERT ) || defined( PHONG )
			iblIrradiance += getIBLIrradiance( geometryNormal );
		#endif
	#endif
#endif
#if defined( USE_ENVMAP ) && defined( RE_IndirectSpecular )
	#ifdef USE_ANISOTROPY
		vec3 iblRadiance = getIBLAnisotropyRadiance( geometryViewDir, geometryNormal, material.roughness, material.anisotropyB, material.anisotropy );
	#else
		vec3 iblRadiance = getIBLRadiance( geometryViewDir, geometryNormal, material.roughness );
	#endif
	#ifdef USE_RETROREFLECTION
		#ifdef USE_ANISOTROPY
			vec3 retroIBLRadiance = getIBLAnisotropyRetroRadiance( geometryViewDir, geometryNormal, material.roughness, material.anisotropyB, material.anisotropy );
		#else
			vec3 retroIBLRadiance = getIBLRetroRadiance( geometryViewDir, geometryNormal, material.roughness );
		#endif
		iblRadiance = mix( iblRadiance, retroIBLRadiance, saturate( material.retroreflectivity ) );
	#endif
	radiance += iblRadiance;
	#ifdef USE_CLEARCOAT
		clearcoatRadiance += getIBLRadiance( geometryViewDir, geometryClearcoatNormal, material.clearcoatRoughness );
	#endif
#endif`,SA=`#if defined( RE_IndirectDiffuse )
	#if defined( LAMBERT ) || defined( PHONG )
		irradiance += iblIrradiance;
	#endif
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,EA=`#if NUM_LIGHT_PROBE_GRIDS > 0
uniform highp sampler3D probesSH[ NUM_LIGHT_PROBE_GRIDS ];
uniform vec3 probesMin[ NUM_LIGHT_PROBE_GRIDS ];
uniform vec3 probesMax[ NUM_LIGHT_PROBE_GRIDS ];
uniform vec3 probesResolution[ NUM_LIGHT_PROBE_GRIDS ];
bool isInsideLightProbeGrid( vec3 worldPos, vec3 worldNormal, vec3 probesMin, vec3 probesMax, vec3 probesResolution ) {
	vec3 spacing = ( probesMax - probesMin ) / probesResolution;
	vec3 samplePos = worldPos + worldNormal * spacing * 0.5;
	vec3 margin = spacing * 0.01;
	return all( greaterThanEqual( samplePos, probesMin - margin ) ) && all( lessThanEqual( samplePos, probesMax + margin ) );
}
vec3 getLightProbeGridIrradiance( highp sampler3D probesSH, vec3 probesMin, vec3 probesMax, vec3 probesResolution, vec3 worldPos, vec3 worldNormal ) {
	vec3 res = probesResolution;
	vec3 gridRange = probesMax - probesMin;
	vec3 probeSpacing = gridRange / res;
	vec3 samplePos = worldPos + worldNormal * probeSpacing * 0.5;
	vec3 uvw = clamp( ( samplePos - probesMin ) / gridRange, 0.5 / res, 1.0 - 0.5 / res );
	float nz          = res.z;
	float paddedSlices = nz + 2.0;
	float atlasDepth  = 7.0 * paddedSlices;
	float uvZBase     = uvw.z * nz + 1.0;
	vec4 s0 = texture( probesSH, vec3( uvw.xy, ( uvZBase                       ) / atlasDepth ) );
	vec4 s1 = texture( probesSH, vec3( uvw.xy, ( uvZBase +       paddedSlices   ) / atlasDepth ) );
	vec4 s2 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 2.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s3 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 3.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s4 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 4.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s5 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 5.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s6 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 6.0 * paddedSlices   ) / atlasDepth ) );
	vec3 c0 = s0.xyz;
	vec3 c1 = vec3( s0.w, s1.xy );
	vec3 c2 = vec3( s1.zw, s2.x );
	vec3 c3 = s2.yzw;
	vec3 c4 = s3.xyz;
	vec3 c5 = vec3( s3.w, s4.xy );
	vec3 c6 = vec3( s4.zw, s5.x );
	vec3 c7 = s5.yzw;
	vec3 c8 = s6.xyz;
	float x = worldNormal.x, y = worldNormal.y, z = worldNormal.z;
	vec3 result = c0 * 0.886227;
	result += c1 * 2.0 * 0.511664 * y;
	result += c2 * 2.0 * 0.511664 * z;
	result += c3 * 2.0 * 0.511664 * x;
	result += c4 * 2.0 * 0.429043 * x * y;
	result += c5 * 2.0 * 0.429043 * y * z;
	result += c6 * ( 0.743125 * z * z - 0.247708 );
	result += c7 * 2.0 * 0.429043 * x * z;
	result += c8 * 0.429043 * ( x * x - y * y );
	return max( result, vec3( 0.0 ) );
}
#endif`,bA=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,TA=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,wA=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,IA=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,CA=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = sRGBTransferEOTF( sampledDiffuseColor );
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,AA=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,DA=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
	#if defined( USE_POINTS_UV )
		vec2 uv = vUv;
	#else
		vec2 uv = ( uvTransform * vec3( gl_PointCoord.x, 1.0 - gl_PointCoord.y, 1 ) ).xy;
	#endif
#endif
#ifdef USE_MAP
	diffuseColor *= texture2D( map, uv );
#endif
#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, uv ).g;
#endif`,RA=`#if defined( USE_POINTS_UV )
	varying vec2 vUv;
#else
	#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
		uniform mat3 uvTransform;
	#endif
#endif
#ifdef USE_MAP
	uniform sampler2D map;
#endif
#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,NA=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,PA=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,LA=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,OA=`#if defined( USE_MORPHCOLORS )
	#if defined( USE_COLOR_ALPHA )
		vColor *= morphTargetBaseInfluence;
	#elif defined( USE_COLOR )
		vColor.rgb *= morphTargetBaseInfluence;
	#endif
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor.rgb += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,FA=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,kA=`#ifdef USE_MORPHTARGETS
	#ifndef USE_INSTANCING_MORPH
		uniform float morphTargetBaseInfluence;
		uniform float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	#endif
	uniform sampler2DArray morphTargetsTexture;
	uniform ivec2 morphTargetsTextureSize;
	vec4 getMorph( const in int vertexIndex, const in int morphTargetIndex, const in int offset ) {
		int texelIndex = vertexIndex * MORPHTARGETS_TEXTURE_STRIDE + offset;
		int y = texelIndex / morphTargetsTextureSize.x;
		int x = texelIndex - y * morphTargetsTextureSize.x;
		ivec3 morphUV = ivec3( x, y, morphTargetIndex );
		return texelFetch( morphTargetsTexture, morphUV, 0 );
	}
#endif`,UA=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,BA=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
#ifdef FLAT_SHADED
	vec3 fdx = dFdx( vViewPosition );
	vec3 fdy = dFdy( vViewPosition );
	vec3 normal = normalize( cross( fdx, fdy ) );
#else
	vec3 normal = normalize( vNormal );
	#ifdef DOUBLE_SIDED
		normal *= faceDirection;
	#endif
#endif
#if defined( USE_NORMALMAP_TANGENTSPACE ) || defined( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY )
	#ifdef USE_TANGENT
		mat3 tbn = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn = getTangentFrame( - vViewPosition, normal,
		#if defined( USE_NORMALMAP )
			vNormalMapUv
		#elif defined( USE_CLEARCOAT_NORMALMAP )
			vClearcoatNormalMapUv
		#else
			vUv
		#endif
		);
	#endif
	#ifdef DOUBLE_SIDED
		tbn[0] *= faceDirection;
		tbn[1] *= faceDirection;
	#endif
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	#ifdef USE_TANGENT
		mat3 tbn2 = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn2 = getTangentFrame( - vViewPosition, normal, vClearcoatNormalMapUv );
	#endif
	#ifdef DOUBLE_SIDED
		tbn2[0] *= faceDirection;
		tbn2[1] *= faceDirection;
	#endif
#endif
vec3 nonPerturbedNormal = normal;`,VA=`#ifdef USE_NORMALMAP_OBJECTSPACE
	normal = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#ifdef FLIP_SIDED
		normal = - normal;
	#endif
	#ifdef DOUBLE_SIDED
		normal = normal * faceDirection;
	#endif
	normal = normalize( normalMatrix * normal );
#elif defined( USE_NORMALMAP_TANGENTSPACE )
	vec3 mapN = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#if defined( USE_PACKED_NORMALMAP )
		mapN = vec3( mapN.xy, sqrt( saturate( 1.0 - dot( mapN.xy, mapN.xy ) ) ) );
	#endif
	mapN.xy *= normalScale;
	normal = normalize( tbn * mapN );
#elif defined( USE_BUMPMAP )
	normal = perturbNormalArb( - vViewPosition, normal, dHdxy_fwd(), faceDirection );
#endif`,HA=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,zA=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,GA=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
		#ifdef FLIP_SIDED
			vBitangent = - vBitangent;
		#endif
	#endif
#endif`,WA=`#ifdef USE_NORMALMAP
	uniform sampler2D normalMap;
	uniform vec2 normalScale;
#endif
#ifdef USE_NORMALMAP_OBJECTSPACE
	uniform mat3 normalMatrix;
#endif
#if ! defined ( USE_TANGENT ) && ( defined ( USE_NORMALMAP_TANGENTSPACE ) || defined ( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY ) )
	mat3 getTangentFrame( vec3 eye_pos, vec3 surf_norm, vec2 uv ) {
		vec3 q0 = dFdx( eye_pos.xyz );
		vec3 q1 = dFdy( eye_pos.xyz );
		vec2 st0 = dFdx( uv.st );
		vec2 st1 = dFdy( uv.st );
		vec3 N = surf_norm;
		vec3 q1perp = cross( q1, N );
		vec3 q0perp = cross( N, q0 );
		vec3 T = q1perp * st0.x + q0perp * st1.x;
		vec3 B = q1perp * st0.y + q0perp * st1.y;
		float det = max( dot( T, T ), dot( B, B ) );
		float scale = ( det == 0.0 ) ? 0.0 : inversesqrt( det );
		return mat3( T * scale, B * scale, N );
	}
#endif`,jA=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,$A=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,qA=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,XA=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,YA=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,ZA=`vec3 packNormalToRGB( const in vec3 normal ) {
	return normalize( normal ) * 0.5 + 0.5;
}
vec3 unpackRGBToNormal( const in vec3 rgb ) {
	return 2.0 * rgb.xyz - 1.0;
}
const float PackUpscale = 256. / 255.;const float UnpackDownscale = 255. / 256.;const float ShiftRight8 = 1. / 256.;
const float Inv255 = 1. / 255.;
const vec4 PackFactors = vec4( 1.0, 256.0, 256.0 * 256.0, 256.0 * 256.0 * 256.0 );
const vec2 UnpackFactors2 = vec2( UnpackDownscale, 1.0 / PackFactors.g );
const vec3 UnpackFactors3 = vec3( UnpackDownscale / PackFactors.rg, 1.0 / PackFactors.b );
const vec4 UnpackFactors4 = vec4( UnpackDownscale / PackFactors.rgb, 1.0 / PackFactors.a );
vec4 packDepthToRGBA( const in float v ) {
	if( v <= 0.0 )
		return vec4( 0., 0., 0., 0. );
	if( v >= 1.0 )
		return vec4( 1., 1., 1., 1. );
	float vuf;
	float af = modf( v * PackFactors.a, vuf );
	float bf = modf( vuf * ShiftRight8, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec4( vuf * Inv255, gf * PackUpscale, bf * PackUpscale, af );
}
vec3 packDepthToRGB( const in float v ) {
	if( v <= 0.0 )
		return vec3( 0., 0., 0. );
	if( v >= 1.0 )
		return vec3( 1., 1., 1. );
	float vuf;
	float bf = modf( v * PackFactors.b, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec3( vuf * Inv255, gf * PackUpscale, bf );
}
vec2 packDepthToRG( const in float v ) {
	if( v <= 0.0 )
		return vec2( 0., 0. );
	if( v >= 1.0 )
		return vec2( 1., 1. );
	float vuf;
	float gf = modf( v * 256., vuf );
	return vec2( vuf * Inv255, gf );
}
float unpackRGBAToDepth( const in vec4 v ) {
	return dot( v, UnpackFactors4 );
}
float unpackRGBToDepth( const in vec3 v ) {
	return dot( v, UnpackFactors3 );
}
float unpackRGToDepth( const in vec2 v ) {
	return v.r * UnpackFactors2.r + v.g * UnpackFactors2.g;
}
vec4 pack2HalfToRGBA( const in vec2 v ) {
	vec4 r = vec4( v.x, fract( v.x * 255.0 ), v.y, fract( v.y * 255.0 ) );
	return vec4( r.x - r.y / 255.0, r.y, r.z - r.w / 255.0, r.w );
}
vec2 unpackRGBATo2Half( const in vec4 v ) {
	return vec2( v.x + ( v.y / 255.0 ), v.z + ( v.w / 255.0 ) );
}
float viewZToOrthographicDepth( const in float viewZ, const in float near, const in float far ) {
	return ( viewZ + near ) / ( near - far );
}
float orthographicDepthToViewZ( const in float depth, const in float near, const in float far ) {
	#ifdef USE_REVERSED_DEPTH_BUFFER
	
		return depth * ( far - near ) - far;
	#else
		return depth * ( near - far ) - near;
	#endif
}
float viewZToPerspectiveDepth( const in float viewZ, const in float near, const in float far ) {
	return ( ( near + viewZ ) * far ) / ( ( far - near ) * viewZ );
}
float perspectiveDepthToViewZ( const in float depth, const in float near, const in float far ) {
	
	#ifdef USE_REVERSED_DEPTH_BUFFER
		return ( near * far ) / ( ( near - far ) * depth - near );
	#else
		return ( near * far ) / ( ( far - near ) * depth - far );
	#endif
}`,KA=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,JA=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,QA=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,eD=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,tD=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,nD=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,iD=`#if NUM_SPOT_LIGHT_COORDS > 0
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#if NUM_SPOT_LIGHT_MAPS > 0
	uniform sampler2D spotLightMap[ NUM_SPOT_LIGHT_MAPS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_SUN_LIGHT_SHADOWS > 0
		#define SUN_LIGHT_CASCADES 2
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow sunShadowMap[ NUM_SUN_LIGHT_SHADOWS ];
		#else
			uniform sampler2D sunShadowMap[ NUM_SUN_LIGHT_SHADOWS ];
		#endif
		uniform mat4 sunShadowMatrix[ NUM_SUN_LIGHT_SHADOWS * SUN_LIGHT_CASCADES ];
		uniform vec4 sunShadowCascade[ NUM_SUN_LIGHT_SHADOWS * SUN_LIGHT_CASCADES ];
		varying vec4 vSunShadowWorldPosition;
		varying vec3 vSunShadowWorldNormal;
		struct SunLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SunLightShadow sunLightShadows[ NUM_SUN_LIGHT_SHADOWS ];
	#endif
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#else
			uniform sampler2D directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#endif
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#else
			uniform sampler2D spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#endif
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform samplerCubeShadow pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#elif defined( SHADOWMAP_TYPE_BASIC )
			uniform samplerCube pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#endif
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float interleavedGradientNoise( vec2 position ) {
			return fract( 52.9829189 * fract( dot( position, vec2( 0.06711056, 0.00583715 ) ) ) );
		}
		vec2 vogelDiskSample( int sampleIndex, int samplesCount, float phi ) {
			const float goldenAngle = 2.399963229728653;
			float r = sqrt( ( float( sampleIndex ) + 0.5 ) / float( samplesCount ) );
			float theta = float( sampleIndex ) * goldenAngle + phi;
			return vec2( cos( theta ), sin( theta ) ) * r;
		}
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float getShadow( sampler2DShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 texelSize = vec2( 1.0 ) / shadowMapSize;
				vec2 radius = shadowRadius * texelSize;
				float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
				shadow = (
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 0, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 1, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 2, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 3, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 4, 5, phi ) * radius, shadowCoord.z ) )
				) * 0.2;
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#elif defined( SHADOWMAP_TYPE_VSM )
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 distribution = texture2D( shadowMap, shadowCoord.xy ).rg;
				float mean = distribution.x;
				float variance = distribution.y * distribution.y;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					float hard_shadow = step( mean, shadowCoord.z );
				#else
					float hard_shadow = step( shadowCoord.z, mean );
				#endif
				
				if ( hard_shadow == 1.0 ) {
					shadow = 1.0;
				} else {
					variance = max( variance, 0.0000001 );
					float d = shadowCoord.z - mean;
					float p_max = variance / ( variance + d * d );
					p_max = clamp( ( p_max - 0.3 ) / 0.65, 0.0, 1.0 );
					shadow = max( hard_shadow, p_max );
				}
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#else
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				float depth = texture2D( shadowMap, shadowCoord.xy ).r;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					shadow = step( depth, shadowCoord.z );
				#else
					shadow = step( shadowCoord.z, depth );
				#endif
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#endif
	#if NUM_SUN_LIGHT_SHADOWS > 0
		float getSunShadow(
			#if defined( SHADOWMAP_TYPE_PCF )
				sampler2DShadow shadowMap,
			#else
				sampler2D shadowMap,
			#endif
			SunLightShadow sunLightShadow,
			int shadowIndex
		) {
			vec4 shadowWorldPosition = vec4( vSunShadowWorldPosition.xyz + vSunShadowWorldNormal * sunLightShadow.shadowNormalBias, 1.0 );
			float viewDepth = vSunShadowWorldPosition.w;
			int cascadeOffset = shadowIndex * SUN_LIGHT_CASCADES;
			float shadow = 1.0;
			for ( int i = SUN_LIGHT_CASCADES - 1; i >= 0; i -- ) {
				vec4 cascade = sunShadowCascade[ cascadeOffset + i ];
				if ( viewDepth >= cascade.x && viewDepth < cascade.y ) {
					float cascadeShadow = getShadow(
						shadowMap,
						sunLightShadow.shadowMapSize,
						sunLightShadow.shadowIntensity,
						sunLightShadow.shadowBias,
						sunLightShadow.shadowRadius,
						sunShadowMatrix[ cascadeOffset + i ] * shadowWorldPosition
					);
					shadow = mix( cascadeShadow, shadow, smoothstep( cascade.z, cascade.y, viewDepth ) );
				}
			}
			return shadow;
		}
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
	#if defined( SHADOWMAP_TYPE_PCF )
	float getPointShadow( samplerCubeShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 bd3D = normalize( lightToPosition );
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			float lightDistance = length( lightToPosition );
			float depthRange = shadowCameraFar - shadowCameraNear;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				float depthA = - shadowCameraNear / depthRange - shadowBias;
				float depthB = shadowCameraNear * shadowCameraFar / ( depthRange * lightDistance );
			#else
				float depthA = shadowCameraFar / depthRange + shadowBias;
				float depthB = - shadowCameraNear * shadowCameraFar / ( depthRange * lightDistance );
			#endif
			float texelSize = shadowRadius / shadowMapSize.x;
			vec3 absDir = abs( bd3D );
			vec3 tangent = absDir.x > absDir.z ? vec3( 0.0, 1.0, 0.0 ) : vec3( 1.0, 0.0, 0.0 );
			tangent = normalize( cross( bd3D, tangent ) );
			vec3 bitangent = cross( bd3D, tangent );
			float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
			shadow = 0.0;
			for ( int i = 0; i < 5; i ++ ) {
				vec2 offset = vogelDiskSample( i, 5, phi );
				vec3 direction = bd3D + ( tangent * offset.x + bitangent * offset.y ) * texelSize;
				vec3 absDirection = abs( direction );
				float dp = depthA + depthB / max( max( absDirection.x, absDirection.y ), absDirection.z );
				shadow += texture( shadowMap, vec4( direction, dp ) );
			}
			shadow *= 0.2;
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#elif defined( SHADOWMAP_TYPE_BASIC )
	float getPointShadow( samplerCube shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
			dp += shadowBias;
			vec3 bd3D = normalize( lightToPosition );
			float depth = textureCube( shadowMap, bd3D ).r;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				depth = 1.0 - depth;
			#endif
			shadow = step( dp, depth );
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#endif
	#endif
#endif`,rD=`#if NUM_SPOT_LIGHT_COORDS > 0
	uniform mat4 spotLightMatrix[ NUM_SPOT_LIGHT_COORDS ];
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_SUN_LIGHT_SHADOWS > 0
		varying vec4 vSunShadowWorldPosition;
		varying vec3 vSunShadowWorldNormal;
	#endif
	#if NUM_DIR_LIGHT_SHADOWS > 0
		uniform mat4 directionalShadowMatrix[ NUM_DIR_LIGHT_SHADOWS ];
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		uniform mat4 pointShadowMatrix[ NUM_POINT_LIGHT_SHADOWS ];
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
#endif`,sD=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_SUN_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
	#ifdef HAS_NORMAL
		vec3 shadowWorldNormal = transformNormalByInverseViewMatrix( transformedNormal, viewMatrix );
	#else
		vec3 shadowWorldNormal = vec3( 0.0 );
	#endif
	vec4 shadowWorldPosition;
#endif
#if defined( USE_SHADOWMAP )
	#if NUM_SUN_LIGHT_SHADOWS > 0
		vSunShadowWorldPosition = vec4( worldPosition.xyz, - mvPosition.z );
		vSunShadowWorldNormal = shadowWorldNormal;
	#endif
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * directionalLightShadows[ i ].shadowNormalBias, 0 );
			vDirectionalShadowCoord[ i ] = directionalShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * pointLightShadows[ i ].shadowNormalBias, 0 );
			vPointShadowCoord[ i ] = pointShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
#endif
#if NUM_SPOT_LIGHT_COORDS > 0
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_COORDS; i ++ ) {
		shadowWorldPosition = worldPosition;
		#if ( defined( USE_SHADOWMAP ) && UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
			shadowWorldPosition.xyz += shadowWorldNormal * spotLightShadows[ i ].shadowNormalBias;
		#endif
		vSpotLightCoord[ i ] = spotLightMatrix[ i ] * shadowWorldPosition;
	}
	#pragma unroll_loop_end
#endif`,oD=`float getShadowMask() {
	float shadow = 1.0;
	#ifdef USE_SHADOWMAP
	#if NUM_SUN_LIGHT_SHADOWS > 0
	SunLightShadow sunLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SUN_LIGHT_SHADOWS; i ++ ) {
		sunLight = sunLightShadows[ i ];
		shadow *= receiveShadow ? getSunShadow( sunShadowMap[ i ], sunLight, UNROLLED_LOOP_INDEX ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
		directionalLight = directionalLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( directionalShadowMap[ i ], directionalLight.shadowMapSize, directionalLight.shadowIntensity, directionalLight.shadowBias, directionalLight.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_SHADOWS; i ++ ) {
		spotLight = spotLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( spotShadowMap[ i ], spotLight.shadowMapSize, spotLight.shadowIntensity, spotLight.shadowBias, spotLight.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0 && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
	PointLightShadow pointLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
		pointLight = pointLightShadows[ i ];
		shadow *= receiveShadow ? getPointShadow( pointShadowMap[ i ], pointLight.shadowMapSize, pointLight.shadowIntensity, pointLight.shadowBias, pointLight.shadowRadius, vPointShadowCoord[ i ], pointLight.shadowCameraNear, pointLight.shadowCameraFar ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#endif
	return shadow;
}`,aD=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,cD=`#ifdef USE_SKINNING
	uniform mat4 bindMatrix;
	uniform mat4 bindMatrixInverse;
	uniform highp sampler2D boneTexture;
	mat4 getBoneMatrix( const in float i ) {
		int size = textureSize( boneTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( boneTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( boneTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( boneTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( boneTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
#endif`,lD=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,uD=`#ifdef USE_SKINNING
	mat4 skinMatrix = mat4( 0.0 );
	skinMatrix += skinWeight.x * boneMatX;
	skinMatrix += skinWeight.y * boneMatY;
	skinMatrix += skinWeight.z * boneMatZ;
	skinMatrix += skinWeight.w * boneMatW;
	skinMatrix = bindMatrixInverse * skinMatrix * bindMatrix;
	objectNormal = vec4( skinMatrix * vec4( objectNormal, 0.0 ) ).xyz;
	#ifdef USE_TANGENT
		objectTangent = vec4( skinMatrix * vec4( objectTangent, 0.0 ) ).xyz;
	#endif
#endif`,dD=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,fD=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,hD=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,pD=`#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
uniform float toneMappingExposure;
vec3 LinearToneMapping( vec3 color ) {
	return saturate( toneMappingExposure * color );
}
vec3 ReinhardToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	return saturate( color / ( vec3( 1.0 ) + color ) );
}
vec3 CineonToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	color = max( vec3( 0.0 ), color - 0.004 );
	return pow( ( color * ( 6.2 * color + 0.5 ) ) / ( color * ( 6.2 * color + 1.7 ) + 0.06 ), vec3( 2.2 ) );
}
vec3 RRTAndODTFit( vec3 v ) {
	vec3 a = v * ( v + 0.0245786 ) - 0.000090537;
	vec3 b = v * ( 0.983729 * v + 0.4329510 ) + 0.238081;
	return a / b;
}
vec3 ACESFilmicToneMapping( vec3 color ) {
	const mat3 ACESInputMat = mat3(
		vec3( 0.59719, 0.07600, 0.02840 ),		vec3( 0.35458, 0.90834, 0.13383 ),
		vec3( 0.04823, 0.01566, 0.83777 )
	);
	const mat3 ACESOutputMat = mat3(
		vec3(  1.60475, -0.10208, -0.00327 ),		vec3( -0.53108,  1.10813, -0.07276 ),
		vec3( -0.07367, -0.00605,  1.07602 )
	);
	color *= toneMappingExposure / 0.6;
	color = ACESInputMat * color;
	color = RRTAndODTFit( color );
	color = ACESOutputMat * color;
	return saturate( color );
}
const mat3 LINEAR_REC2020_TO_LINEAR_SRGB = mat3(
	vec3( 1.6605, - 0.1246, - 0.0182 ),
	vec3( - 0.5876, 1.1329, - 0.1006 ),
	vec3( - 0.0728, - 0.0083, 1.1187 )
);
const mat3 LINEAR_SRGB_TO_LINEAR_REC2020 = mat3(
	vec3( 0.6274, 0.0691, 0.0164 ),
	vec3( 0.3293, 0.9195, 0.0880 ),
	vec3( 0.0433, 0.0113, 0.8956 )
);
vec3 agxDefaultContrastApprox( vec3 x ) {
	vec3 x2 = x * x;
	vec3 x4 = x2 * x2;
	return + 15.5 * x4 * x2
		- 40.14 * x4 * x
		+ 31.96 * x4
		- 6.868 * x2 * x
		+ 0.4298 * x2
		+ 0.1191 * x
		- 0.00232;
}
vec3 AgXToneMapping( vec3 color ) {
	const mat3 AgXInsetMatrix = mat3(
		vec3( 0.856627153315983, 0.137318972929847, 0.11189821299995 ),
		vec3( 0.0951212405381588, 0.761241990602591, 0.0767994186031903 ),
		vec3( 0.0482516061458583, 0.101439036467562, 0.811302368396859 )
	);
	const mat3 AgXOutsetMatrix = mat3(
		vec3( 1.1271005818144368, - 0.1413297634984383, - 0.14132976349843826 ),
		vec3( - 0.11060664309660323, 1.157823702216272, - 0.11060664309660294 ),
		vec3( - 0.016493938717834573, - 0.016493938717834257, 1.2519364065950405 )
	);
	const float AgxMinEv = - 12.47393;	const float AgxMaxEv = 4.026069;
	color *= toneMappingExposure;
	color = LINEAR_SRGB_TO_LINEAR_REC2020 * color;
	color = AgXInsetMatrix * color;
	color = max( color, 1e-10 );	color = log2( color );
	color = ( color - AgxMinEv ) / ( AgxMaxEv - AgxMinEv );
	color = clamp( color, 0.0, 1.0 );
	color = agxDefaultContrastApprox( color );
	color = AgXOutsetMatrix * color;
	color = pow( max( vec3( 0.0 ), color ), vec3( 2.2 ) );
	color = LINEAR_REC2020_TO_LINEAR_SRGB * color;
	color = clamp( color, 0.0, 1.0 );
	return color;
}
vec3 NeutralToneMapping( vec3 color ) {
	const float StartCompression = 0.8 - 0.04;
	const float Desaturation = 0.15;
	color *= toneMappingExposure;
	float x = min( color.r, min( color.g, color.b ) );
	float offset = x < 0.08 ? x - 6.25 * x * x : 0.04;
	color -= offset;
	float peak = max( color.r, max( color.g, color.b ) );
	if ( peak < StartCompression ) return color;
	float d = 1. - StartCompression;
	float newPeak = 1. - d * d / ( peak + d - StartCompression );
	color *= newPeak / peak;
	float g = 1. - 1. / ( Desaturation * ( peak - newPeak ) + 1. );
	return mix( color, vec3( newPeak ), g );
}
vec3 CustomToneMapping( vec3 color ) { return color; }`,mD=`#ifdef USE_TRANSMISSION
	material.transmission = transmission;
	material.transmissionAlpha = 1.0;
	material.thickness = thickness;
	material.attenuationDistance = attenuationDistance;
	material.attenuationColor = attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		material.transmission *= texture2D( transmissionMap, vTransmissionMapUv ).r;
	#endif
	#ifdef USE_THICKNESSMAP
		material.thickness *= texture2D( thicknessMap, vThicknessMapUv ).g;
	#endif
	vec3 pos = vWorldPosition;
	vec3 v = normalize( cameraPosition - pos );
	vec3 n = transformNormalByInverseViewMatrix( normal, viewMatrix );
	vec4 transmitted = getIBLVolumeRefraction(
		n, v, material.roughness, material.diffuseContribution, material.specularColorBlended, material.specularF90,
		pos, modelMatrix, viewMatrix, projectionMatrix, material.dispersion, material.ior, material.thickness,
		material.attenuationColor, material.attenuationDistance );
	material.transmissionAlpha = mix( material.transmissionAlpha, transmitted.a, material.transmission );
	totalDiffuse = mix( totalDiffuse, transmitted.rgb, material.transmission );
#endif`,gD=`#ifdef USE_TRANSMISSION
	uniform float transmission;
	uniform float thickness;
	uniform float attenuationDistance;
	uniform vec3 attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		uniform sampler2D transmissionMap;
	#endif
	#ifdef USE_THICKNESSMAP
		uniform sampler2D thicknessMap;
	#endif
	uniform vec2 transmissionSamplerSize;
	uniform sampler2D transmissionSamplerMap;
	uniform mat4 modelMatrix;
	uniform mat4 projectionMatrix;
	varying vec3 vWorldPosition;
	float w0( float a ) {
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - a + 3.0 ) - 3.0 ) + 1.0 );
	}
	float w1( float a ) {
		return ( 1.0 / 6.0 ) * ( a *  a * ( 3.0 * a - 6.0 ) + 4.0 );
	}
	float w2( float a ){
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - 3.0 * a + 3.0 ) + 3.0 ) + 1.0 );
	}
	float w3( float a ) {
		return ( 1.0 / 6.0 ) * ( a * a * a );
	}
	float g0( float a ) {
		return w0( a ) + w1( a );
	}
	float g1( float a ) {
		return w2( a ) + w3( a );
	}
	float h0( float a ) {
		return - 1.0 + w1( a ) / ( w0( a ) + w1( a ) );
	}
	float h1( float a ) {
		return 1.0 + w3( a ) / ( w2( a ) + w3( a ) );
	}
	vec4 bicubic( sampler2D tex, vec2 uv, vec4 texelSize, float lod ) {
		uv = uv * texelSize.zw + 0.5;
		vec2 iuv = floor( uv );
		vec2 fuv = fract( uv );
		float g0x = g0( fuv.x );
		float g1x = g1( fuv.x );
		float h0x = h0( fuv.x );
		float h1x = h1( fuv.x );
		float h0y = h0( fuv.y );
		float h1y = h1( fuv.y );
		vec2 p0 = ( vec2( iuv.x + h0x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p1 = ( vec2( iuv.x + h1x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p2 = ( vec2( iuv.x + h0x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		vec2 p3 = ( vec2( iuv.x + h1x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		return g0( fuv.y ) * ( g0x * textureLod( tex, p0, lod ) + g1x * textureLod( tex, p1, lod ) ) +
			g1( fuv.y ) * ( g0x * textureLod( tex, p2, lod ) + g1x * textureLod( tex, p3, lod ) );
	}
	vec4 textureBicubic( sampler2D sampler, vec2 uv, float lod ) {
		vec2 fLodSize = vec2( textureSize( sampler, int( lod ) ) );
		vec2 cLodSize = vec2( textureSize( sampler, int( lod + 1.0 ) ) );
		vec2 fLodSizeInv = 1.0 / fLodSize;
		vec2 cLodSizeInv = 1.0 / cLodSize;
		vec4 fSample = bicubic( sampler, uv, vec4( fLodSizeInv, fLodSize ), floor( lod ) );
		vec4 cSample = bicubic( sampler, uv, vec4( cLodSizeInv, cLodSize ), ceil( lod ) );
		return mix( fSample, cSample, fract( lod ) );
	}
	vec3 getVolumeTransmissionRay( const in vec3 n, const in vec3 v, const in float thickness, const in float ior, const in mat4 modelMatrix ) {
		vec3 refractionVector = refract( - v, normalize( n ), 1.0 / ior );
		vec3 modelScale;
		modelScale.x = length( vec3( modelMatrix[ 0 ].xyz ) );
		modelScale.y = length( vec3( modelMatrix[ 1 ].xyz ) );
		modelScale.z = length( vec3( modelMatrix[ 2 ].xyz ) );
		return normalize( refractionVector ) * thickness * modelScale;
	}
	float applyIorToRoughness( const in float roughness, const in float ior ) {
		return roughness * clamp( ior * 2.0 - 2.0, 0.0, 1.0 );
	}
	vec4 getTransmissionSample( const in vec2 fragCoord, const in float roughness, const in float ior ) {
		float lod = log2( transmissionSamplerSize.x ) * applyIorToRoughness( roughness, ior );
		return textureBicubic( transmissionSamplerMap, fragCoord.xy, lod );
	}
	vec3 volumeAttenuation( const in float transmissionDistance, const in vec3 attenuationColor, const in float attenuationDistance ) {
		if ( isinf( attenuationDistance ) ) {
			return vec3( 1.0 );
		} else {
			vec3 attenuationCoefficient = -log( attenuationColor ) / attenuationDistance;
			vec3 transmittance = exp( - attenuationCoefficient * transmissionDistance );			return transmittance;
		}
	}
	vec4 getIBLVolumeRefraction( const in vec3 n, const in vec3 v, const in float roughness, const in vec3 diffuseColor,
		const in vec3 specularColor, const in float specularF90, const in vec3 position, const in mat4 modelMatrix,
		const in mat4 viewMatrix, const in mat4 projMatrix, const in float dispersion, const in float ior, const in float thickness,
		const in vec3 attenuationColor, const in float attenuationDistance ) {
		vec4 transmittedLight;
		vec3 transmittance;
		#ifdef USE_DISPERSION
			float halfSpread = ( ior - 1.0 ) * 0.025 * dispersion;
			vec3 iors = vec3( ior - halfSpread, ior, ior + halfSpread );
			for ( int i = 0; i < 3; i ++ ) {
				vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, iors[ i ], modelMatrix );
				vec3 refractedRayExit = position + transmissionRay;
				vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
				vec2 refractionCoords = ndcPos.xy / ndcPos.w;
				refractionCoords += 1.0;
				refractionCoords /= 2.0;
				vec4 transmissionSample = getTransmissionSample( refractionCoords, roughness, iors[ i ] );
				transmittedLight[ i ] = transmissionSample[ i ];
				transmittedLight.a += transmissionSample.a;
				transmittance[ i ] = diffuseColor[ i ] * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance )[ i ];
			}
			transmittedLight.a /= 3.0;
		#else
			vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, ior, modelMatrix );
			vec3 refractedRayExit = position + transmissionRay;
			vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
			vec2 refractionCoords = ndcPos.xy / ndcPos.w;
			refractionCoords += 1.0;
			refractionCoords /= 2.0;
			transmittedLight = getTransmissionSample( refractionCoords, roughness, ior );
			transmittance = diffuseColor * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance );
		#endif
		vec3 attenuatedColor = transmittance * transmittedLight.rgb;
		vec3 F = EnvironmentBRDF( n, v, specularColor, specularF90, roughness );
		float transmittanceFactor = ( transmittance.r + transmittance.g + transmittance.b ) / 3.0;
		return vec4( ( 1.0 - F ) * attenuatedColor, 1.0 - ( 1.0 - transmittedLight.a ) * transmittanceFactor );
	}
#endif`,yD=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_DIFFUSE_ROUGHNESSMAP
	varying vec2 vDiffuseRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_SPECULARMAP
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,vD=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	uniform mat3 mapTransform;
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	uniform mat3 alphaMapTransform;
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	uniform mat3 lightMapTransform;
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	uniform mat3 aoMapTransform;
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	uniform mat3 bumpMapTransform;
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	uniform mat3 normalMapTransform;
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_DISPLACEMENTMAP
	uniform mat3 displacementMapTransform;
	varying vec2 vDisplacementMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	uniform mat3 emissiveMapTransform;
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	uniform mat3 metalnessMapTransform;
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	uniform mat3 roughnessMapTransform;
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_DIFFUSE_ROUGHNESSMAP
	uniform mat3 diffuseRoughnessMapTransform;
	varying vec2 vDiffuseRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	uniform mat3 anisotropyMapTransform;
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	uniform mat3 clearcoatMapTransform;
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform mat3 clearcoatNormalMapTransform;
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform mat3 clearcoatRoughnessMapTransform;
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	uniform mat3 sheenColorMapTransform;
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	uniform mat3 sheenRoughnessMapTransform;
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	uniform mat3 iridescenceMapTransform;
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform mat3 iridescenceThicknessMapTransform;
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SPECULARMAP
	uniform mat3 specularMapTransform;
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	uniform mat3 specularColorMapTransform;
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	uniform mat3 specularIntensityMapTransform;
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,_D=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	vUv = vec3( uv, 1 ).xy;
#endif
#ifdef USE_MAP
	vMapUv = ( mapTransform * vec3( MAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ALPHAMAP
	vAlphaMapUv = ( alphaMapTransform * vec3( ALPHAMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_LIGHTMAP
	vLightMapUv = ( lightMapTransform * vec3( LIGHTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_AOMAP
	vAoMapUv = ( aoMapTransform * vec3( AOMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_BUMPMAP
	vBumpMapUv = ( bumpMapTransform * vec3( BUMPMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_NORMALMAP
	vNormalMapUv = ( normalMapTransform * vec3( NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_DISPLACEMENTMAP
	vDisplacementMapUv = ( displacementMapTransform * vec3( DISPLACEMENTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_EMISSIVEMAP
	vEmissiveMapUv = ( emissiveMapTransform * vec3( EMISSIVEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_METALNESSMAP
	vMetalnessMapUv = ( metalnessMapTransform * vec3( METALNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ROUGHNESSMAP
	vRoughnessMapUv = ( roughnessMapTransform * vec3( ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_DIFFUSE_ROUGHNESSMAP
	vDiffuseRoughnessMapUv = ( diffuseRoughnessMapTransform * vec3( DIFFUSE_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ANISOTROPYMAP
	vAnisotropyMapUv = ( anisotropyMapTransform * vec3( ANISOTROPYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOATMAP
	vClearcoatMapUv = ( clearcoatMapTransform * vec3( CLEARCOATMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	vClearcoatNormalMapUv = ( clearcoatNormalMapTransform * vec3( CLEARCOAT_NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	vClearcoatRoughnessMapUv = ( clearcoatRoughnessMapTransform * vec3( CLEARCOAT_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCEMAP
	vIridescenceMapUv = ( iridescenceMapTransform * vec3( IRIDESCENCEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	vIridescenceThicknessMapUv = ( iridescenceThicknessMapTransform * vec3( IRIDESCENCE_THICKNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_COLORMAP
	vSheenColorMapUv = ( sheenColorMapTransform * vec3( SHEEN_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	vSheenRoughnessMapUv = ( sheenRoughnessMapTransform * vec3( SHEEN_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULARMAP
	vSpecularMapUv = ( specularMapTransform * vec3( SPECULARMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_COLORMAP
	vSpecularColorMapUv = ( specularColorMapTransform * vec3( SPECULAR_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	vSpecularIntensityMapUv = ( specularIntensityMapTransform * vec3( SPECULAR_INTENSITYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_TRANSMISSIONMAP
	vTransmissionMapUv = ( transmissionMapTransform * vec3( TRANSMISSIONMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_THICKNESSMAP
	vThicknessMapUv = ( thicknessMapTransform * vec3( THICKNESSMAP_UV, 1 ) ).xy;
#endif`,xD=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,MD=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,SD=`uniform sampler2D t2D;
uniform float backgroundIntensity;
varying vec2 vUv;
void main() {
	vec4 texColor = texture2D( t2D, vUv );
	#ifdef DECODE_VIDEO_TEXTURE
		texColor = vec4( mix( pow( texColor.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), texColor.rgb * 0.0773993808, vec3( lessThanEqual( texColor.rgb, vec3( 0.04045 ) ) ) ), texColor.w );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,ED=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,bD=`uniform float backgroundBlurriness;
uniform float backgroundIntensity;
uniform mat3 backgroundRotation;
varying vec3 vWorldDirection;
#include <envmap_common_pars_fragment>
void main() {
	#ifdef ENVMAP_TYPE_PMREM
		vec4 texColor = textureLod( envMap, backgroundRotation * vWorldDirection, roughnessToMip( backgroundBlurriness ) );
	#else
		vec4 texColor = textureCube( envMap, backgroundRotation * vWorldDirection );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,TD=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,wD=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,ID=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
varying vec2 vHighPrecisionZW;
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vHighPrecisionZW = gl_Position.zw;
}`,CD=`#if DEPTH_PACKING == 3200
	uniform float opacity;
#endif
#include <common>
#include <packing>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
varying vec2 vHighPrecisionZW;
void main() {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#if DEPTH_PACKING == 3200
		diffuseColor.a = opacity;
	#endif
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <logdepthbuf_fragment>
	#ifdef USE_REVERSED_DEPTH_BUFFER
		float fragCoordZ = vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ];
	#else
		float fragCoordZ = 0.5 * vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ] + 0.5;
	#endif
	#if DEPTH_PACKING == 3200
		gl_FragColor = vec4( vec3( 1.0 - fragCoordZ ), opacity );
	#elif DEPTH_PACKING == 3201
		gl_FragColor = packDepthToRGBA( fragCoordZ );
	#elif DEPTH_PACKING == 3202
		gl_FragColor = vec4( packDepthToRGB( fragCoordZ ), 1.0 );
	#elif DEPTH_PACKING == 3203
		gl_FragColor = vec4( packDepthToRG( fragCoordZ ), 0.0, 1.0 );
	#endif
}`,AD=`#define DISTANCE
varying vec3 vWorldPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <worldpos_vertex>
	#include <clipping_planes_vertex>
	vWorldPosition = worldPosition.xyz;
}`,DD=`#define DISTANCE
uniform vec3 referencePosition;
uniform float nearDistance;
uniform float farDistance;
varying vec3 vWorldPosition;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	float dist = length( vWorldPosition - referencePosition );
	dist = ( dist - nearDistance ) / ( farDistance - nearDistance );
	dist = saturate( dist );
	gl_FragColor = vec4( dist, 0.0, 0.0, 1.0 );
}`,RD=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,ND=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,PD=`uniform float scale;
attribute float lineDistance;
varying float vLineDistance;
#include <common>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	vLineDistance = scale * lineDistance;
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,LD=`uniform vec3 diffuse;
uniform float opacity;
uniform float dashSize;
uniform float totalSize;
varying float vLineDistance;
#include <common>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	if ( mod( vLineDistance, totalSize ) > dashSize ) {
		discard;
	}
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,OD=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#if defined ( USE_ENVMAP ) || defined ( USE_SKINNING )
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinbase_vertex>
		#include <skinnormal_vertex>
		#include <defaultnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <fog_vertex>
}`,FD=`uniform vec3 diffuse;
uniform float opacity;
#ifndef FLAT_SHADED
	varying vec3 vNormal;
#endif
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		reflectedLight.indirectDiffuse += lightMapTexel.rgb * lightMapIntensity * RECIPROCAL_PI;
	#else
		reflectedLight.indirectDiffuse += vec3( 1.0 );
	#endif
	#include <aomap_fragment>
	reflectedLight.indirectDiffuse *= diffuseColor.rgb;
	vec3 outgoingLight = reflectedLight.indirectDiffuse;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,kD=`#define LAMBERT
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,UD=`#define LAMBERT
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_lambert_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_lambert_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,BD=`#define MATCAP
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <displacementmap_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
	vViewPosition = - mvPosition.xyz;
}`,VD=`#define MATCAP
uniform vec3 diffuse;
uniform float opacity;
uniform sampler2D matcap;
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	vec3 viewDir = normalize( vViewPosition );
	vec3 x = normalize( vec3( viewDir.z, 0.0, - viewDir.x ) );
	vec3 y = cross( viewDir, x );
	vec2 uv = vec2( dot( x, normal ), dot( y, normal ) ) * 0.495 + 0.5;
	#ifdef USE_MATCAP
		vec4 matcapColor = texture2D( matcap, uv );
	#else
		vec4 matcapColor = vec4( vec3( mix( 0.2, 0.8, uv.y ) ), 1.0 );
	#endif
	vec3 outgoingLight = diffuseColor.rgb * matcapColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,HD=`#define NORMAL
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	vViewPosition = - mvPosition.xyz;
#endif
}`,zD=`#define NORMAL
uniform float opacity;
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <uv_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( 0.0, 0.0, 0.0, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	gl_FragColor = vec4( normalize( normal ) * 0.5 + 0.5, diffuseColor.a );
	#ifdef OPAQUE
		gl_FragColor.a = 1.0;
	#endif
}`,GD=`#define PHONG
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,WD=`#define PHONG
uniform vec3 diffuse;
uniform vec3 emissive;
uniform vec3 specular;
uniform float shininess;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_phong_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_phong_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + reflectedLight.directSpecular + reflectedLight.indirectSpecular + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,jD=`#define STANDARD
varying vec3 vViewPosition;
#ifdef USE_TRANSMISSION
	varying vec3 vWorldPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
#ifdef USE_TRANSMISSION
	vWorldPosition = worldPosition.xyz;
#endif
}`,$D=`#define STANDARD
#ifdef PHYSICAL
	#define IOR
	#define USE_SPECULAR
#endif
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float roughness;
uniform float metalness;
uniform float opacity;
#ifdef IOR
	uniform float ior;
#endif
#ifdef USE_SPECULAR
	uniform float specularIntensity;
	uniform vec3 specularColor;
	#ifdef USE_SPECULAR_COLORMAP
		uniform sampler2D specularColorMap;
	#endif
	#ifdef USE_SPECULAR_INTENSITYMAP
		uniform sampler2D specularIntensityMap;
	#endif
#endif
#ifdef USE_CLEARCOAT
	uniform float clearcoat;
	uniform float clearcoatRoughness;
#endif
#ifdef USE_DIFFUSE_ROUGHNESS
	uniform float diffuseRoughness;
	#ifdef USE_DIFFUSE_ROUGHNESSMAP
		uniform sampler2D diffuseRoughnessMap;
	#endif
#endif
#ifdef USE_DISPERSION
	uniform float dispersion;
#endif
#ifdef USE_RETROREFLECTION
	uniform float retroreflectivity;
#endif
#ifdef USE_IRIDESCENCE
	uniform float iridescence;
	uniform float iridescenceIOR;
	uniform float iridescenceThicknessMinimum;
	uniform float iridescenceThicknessMaximum;
#endif
#ifdef USE_SHEEN
	uniform vec3 sheenColor;
	uniform float sheenRoughness;
	#ifdef USE_SHEEN_COLORMAP
		uniform sampler2D sheenColorMap;
	#endif
	#ifdef USE_SHEEN_ROUGHNESSMAP
		uniform sampler2D sheenRoughnessMap;
	#endif
#endif
#ifdef USE_ANISOTROPY
	uniform vec2 anisotropyVector;
	#ifdef USE_ANISOTROPYMAP
		uniform sampler2D anisotropyMap;
	#endif
#endif
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <iridescence_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_physical_pars_fragment>
#include <transmission_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <clearcoat_pars_fragment>
#include <iridescence_pars_fragment>
#include <roughnessmap_pars_fragment>
#include <metalnessmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <roughnessmap_fragment>
	#include <metalnessmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <clearcoat_normal_fragment_begin>
	#include <clearcoat_normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_physical_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 totalDiffuse = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse;
	vec3 totalSpecular = reflectedLight.directSpecular + reflectedLight.indirectSpecular;
	#include <transmission_fragment>
	vec3 outgoingLight = totalDiffuse + totalSpecular + totalEmissiveRadiance;
	#ifdef USE_SHEEN
 
		outgoingLight = outgoingLight + sheenSpecularDirect + sheenSpecularIndirect;
 
 	#endif
	#ifdef USE_CLEARCOAT
		float dotNVcc = saturate( dot( geometryClearcoatNormal, geometryViewDir ) );
		vec3 Fcc = F_Schlick( material.clearcoatF0, material.clearcoatF90, dotNVcc );
		outgoingLight = outgoingLight * ( 1.0 - material.clearcoat * Fcc ) + ( clearcoatSpecularDirect + clearcoatSpecularIndirect ) * material.clearcoat;
	#endif
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,qD=`#define TOON
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,XD=`#define TOON
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <gradientmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_toon_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_toon_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,YD=`uniform float size;
uniform float scale;
#include <common>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
#ifdef USE_POINTS_UV
	varying vec2 vUv;
	uniform mat3 uvTransform;
#endif
void main() {
	#ifdef USE_POINTS_UV
		vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	#endif
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	gl_PointSize = size;
	#ifdef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) gl_PointSize *= ( scale / - mvPosition.z );
	#endif
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <fog_vertex>
}`,ZD=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <color_pars_fragment>
#include <map_particle_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_particle_fragment>
	#include <color_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,KD=`#include <common>
#include <batching_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <shadowmap_pars_vertex>
void main() {
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,JD=`uniform vec3 color;
uniform float opacity;
#include <common>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <logdepthbuf_pars_fragment>
#include <shadowmap_pars_fragment>
#include <shadowmask_pars_fragment>
void main() {
	#include <logdepthbuf_fragment>
	gl_FragColor = vec4( color, opacity * ( 1.0 - getShadowMask() ) );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,QD=`uniform float rotation;
uniform vec2 center;
#include <common>
#include <uv_pars_vertex>
#include <fog_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	vec4 mvPosition = modelViewMatrix[ 3 ];
	vec2 scale = vec2( length( modelMatrix[ 0 ].xyz ), length( modelMatrix[ 1 ].xyz ) );
	#ifndef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) scale *= - mvPosition.z;
	#endif
	vec2 alignedPosition = ( position.xy - ( center - vec2( 0.5 ) ) ) * scale;
	vec2 rotatedPosition;
	rotatedPosition.x = cos( rotation ) * alignedPosition.x - sin( rotation ) * alignedPosition.y;
	rotatedPosition.y = sin( rotation ) * alignedPosition.x + cos( rotation ) * alignedPosition.y;
	mvPosition.xy += rotatedPosition;
	gl_Position = projectionMatrix * mvPosition;
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,eR=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
}`,We={alphahash_fragment:SC,alphahash_pars_fragment:EC,alphamap_fragment:bC,alphamap_pars_fragment:TC,alphatest_fragment:wC,alphatest_pars_fragment:IC,aomap_fragment:CC,aomap_pars_fragment:AC,batching_pars_vertex:DC,batching_vertex:RC,begin_vertex:NC,beginnormal_vertex:PC,bsdfs:LC,iridescence_fragment:OC,bumpmap_pars_fragment:FC,clipping_planes_fragment:kC,clipping_planes_pars_fragment:UC,clipping_planes_pars_vertex:BC,clipping_planes_vertex:VC,color_fragment:HC,color_pars_fragment:zC,color_pars_vertex:GC,color_vertex:WC,common:jC,defaultnormal_vertex:$C,displacementmap_pars_vertex:qC,displacementmap_vertex:XC,emissivemap_fragment:YC,emissivemap_pars_fragment:ZC,colorspace_fragment:KC,colorspace_pars_fragment:JC,envmap_fragment:QC,envmap_common_pars_fragment:eA,envmap_pars_fragment:tA,envmap_pars_vertex:nA,envmap_physical_pars_fragment:hA,envmap_vertex:iA,fog_vertex:rA,fog_pars_vertex:sA,fog_fragment:oA,fog_pars_fragment:aA,gradientmap_pars_fragment:cA,lightmap_pars_fragment:lA,lights_lambert_fragment:uA,lights_lambert_pars_fragment:dA,lights_pars_begin:fA,lights_toon_fragment:pA,lights_toon_pars_fragment:mA,lights_phong_fragment:gA,lights_phong_pars_fragment:yA,lights_physical_fragment:vA,lights_physical_pars_fragment:_A,lights_fragment_begin:xA,lights_fragment_maps:MA,lights_fragment_end:SA,lightprobes_pars_fragment:EA,logdepthbuf_fragment:bA,logdepthbuf_pars_fragment:TA,logdepthbuf_pars_vertex:wA,logdepthbuf_vertex:IA,map_fragment:CA,map_pars_fragment:AA,map_particle_fragment:DA,map_particle_pars_fragment:RA,metalnessmap_fragment:NA,metalnessmap_pars_fragment:PA,morphinstance_vertex:LA,morphcolor_vertex:OA,morphnormal_vertex:FA,morphtarget_pars_vertex:kA,morphtarget_vertex:UA,normal_fragment_begin:BA,normal_fragment_maps:VA,normal_pars_fragment:HA,normal_pars_vertex:zA,normal_vertex:GA,normalmap_pars_fragment:WA,clearcoat_normal_fragment_begin:jA,clearcoat_normal_fragment_maps:$A,clearcoat_pars_fragment:qA,iridescence_pars_fragment:XA,opaque_fragment:YA,packing:ZA,premultiplied_alpha_fragment:KA,project_vertex:JA,dithering_fragment:QA,dithering_pars_fragment:eD,roughnessmap_fragment:tD,roughnessmap_pars_fragment:nD,shadowmap_pars_fragment:iD,shadowmap_pars_vertex:rD,shadowmap_vertex:sD,shadowmask_pars_fragment:oD,skinbase_vertex:aD,skinning_pars_vertex:cD,skinning_vertex:lD,skinnormal_vertex:uD,specularmap_fragment:dD,specularmap_pars_fragment:fD,tonemapping_fragment:hD,tonemapping_pars_fragment:pD,transmission_fragment:mD,transmission_pars_fragment:gD,uv_pars_fragment:yD,uv_pars_vertex:vD,uv_vertex:_D,worldpos_vertex:xD,background_vert:MD,background_frag:SD,backgroundCube_vert:ED,backgroundCube_frag:bD,cube_vert:TD,cube_frag:wD,depth_vert:ID,depth_frag:CD,distance_vert:AD,distance_frag:DD,equirect_vert:RD,equirect_frag:ND,linedashed_vert:PD,linedashed_frag:LD,meshbasic_vert:OD,meshbasic_frag:FD,meshlambert_vert:kD,meshlambert_frag:UD,meshmatcap_vert:BD,meshmatcap_frag:VD,meshnormal_vert:HD,meshnormal_frag:zD,meshphong_vert:GD,meshphong_frag:WD,meshphysical_vert:jD,meshphysical_frag:$D,meshtoon_vert:qD,meshtoon_frag:XD,points_vert:YD,points_frag:ZD,shadow_vert:KD,shadow_frag:JD,sprite_vert:QD,sprite_frag:eR},he={common:{diffuse:{value:new He(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new Le},alphaMap:{value:null},alphaMapTransform:{value:new Le},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new Le}},envmap:{envMap:{value:null},envMapRotation:{value:new Le},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98},dfgLUT:{value:null}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new Le}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new Le}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new Le},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new Le},normalScale:{value:new Te(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new Le},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new Le}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new Le}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new Le}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new He(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},sunLights:{value:[],properties:{direction:{},color:{}}},sunLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},sunShadowMatrix:{value:[]},sunShadowCascade:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null},probesSH:{value:[]},probesMin:{value:[]},probesMax:{value:[]},probesResolution:{value:[]}},points:{diffuse:{value:new He(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new Le},alphaTest:{value:0},uvTransform:{value:new Le}},sprite:{diffuse:{value:new He(16777215)},opacity:{value:1},center:{value:new Te(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new Le},alphaMap:{value:null},alphaMapTransform:{value:new Le},alphaTest:{value:0}}},Li={basic:{uniforms:cn([he.common,he.specularmap,he.envmap,he.aomap,he.lightmap,he.fog]),vertexShader:We.meshbasic_vert,fragmentShader:We.meshbasic_frag},lambert:{uniforms:cn([he.common,he.specularmap,he.envmap,he.aomap,he.lightmap,he.emissivemap,he.bumpmap,he.normalmap,he.displacementmap,he.fog,he.lights,{emissive:{value:new He(0)},envMapIntensity:{value:1}}]),vertexShader:We.meshlambert_vert,fragmentShader:We.meshlambert_frag},phong:{uniforms:cn([he.common,he.specularmap,he.envmap,he.aomap,he.lightmap,he.emissivemap,he.bumpmap,he.normalmap,he.displacementmap,he.fog,he.lights,{emissive:{value:new He(0)},specular:{value:new He(1118481)},shininess:{value:30},envMapIntensity:{value:1}}]),vertexShader:We.meshphong_vert,fragmentShader:We.meshphong_frag},standard:{uniforms:cn([he.common,he.envmap,he.aomap,he.lightmap,he.emissivemap,he.bumpmap,he.normalmap,he.displacementmap,he.roughnessmap,he.metalnessmap,he.fog,he.lights,{emissive:{value:new He(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:We.meshphysical_vert,fragmentShader:We.meshphysical_frag},toon:{uniforms:cn([he.common,he.aomap,he.lightmap,he.emissivemap,he.bumpmap,he.normalmap,he.displacementmap,he.gradientmap,he.fog,he.lights,{emissive:{value:new He(0)}}]),vertexShader:We.meshtoon_vert,fragmentShader:We.meshtoon_frag},matcap:{uniforms:cn([he.common,he.bumpmap,he.normalmap,he.displacementmap,he.fog,{matcap:{value:null}}]),vertexShader:We.meshmatcap_vert,fragmentShader:We.meshmatcap_frag},points:{uniforms:cn([he.points,he.fog]),vertexShader:We.points_vert,fragmentShader:We.points_frag},dashed:{uniforms:cn([he.common,he.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:We.linedashed_vert,fragmentShader:We.linedashed_frag},depth:{uniforms:cn([he.common,he.displacementmap]),vertexShader:We.depth_vert,fragmentShader:We.depth_frag},normal:{uniforms:cn([he.common,he.bumpmap,he.normalmap,he.displacementmap,{opacity:{value:1}}]),vertexShader:We.meshnormal_vert,fragmentShader:We.meshnormal_frag},sprite:{uniforms:cn([he.sprite,he.fog]),vertexShader:We.sprite_vert,fragmentShader:We.sprite_frag},background:{uniforms:{uvTransform:{value:new Le},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:We.background_vert,fragmentShader:We.background_frag},backgroundCube:{uniforms:{envMap:{value:null},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new Le}},vertexShader:We.backgroundCube_vert,fragmentShader:We.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:We.cube_vert,fragmentShader:We.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:We.equirect_vert,fragmentShader:We.equirect_frag},distance:{uniforms:cn([he.common,he.displacementmap,{referencePosition:{value:new O},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:We.distance_vert,fragmentShader:We.distance_frag},shadow:{uniforms:cn([he.lights,he.fog,{color:{value:new He(0)},opacity:{value:1}}]),vertexShader:We.shadow_vert,fragmentShader:We.shadow_frag}};Li.physical={uniforms:cn([Li.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new Le},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new Le},clearcoatNormalScale:{value:new Te(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new Le},diffuseRoughness:{value:0},diffuseRoughnessMap:{value:null},diffuseRoughnessMapTransform:{value:new Le},dispersion:{value:0},retroreflectivity:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new Le},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new Le},sheen:{value:0},sheenColor:{value:new He(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new Le},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new Le},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new Le},transmissionSamplerSize:{value:new Te},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new Le},attenuationDistance:{value:0},attenuationColor:{value:new He(0)},specularColor:{value:new He(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new Le},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new Le},anisotropyVector:{value:new Te},anisotropyMap:{value:null},anisotropyMapTransform:{value:new Le}}]),vertexShader:We.meshphysical_vert,fragmentShader:We.meshphysical_frag};var qd={r:0,b:0,g:0},tR=new _t,IM=new Le;IM.set(-1,0,0,0,1,0,0,0,1);function nR(n,e,t,i,r,s){let o=new He(0),a=r===!0?0:1,c,l,u=null,d=0,f=null;function h(x){let w=x.isScene===!0?x.background:null;if(w&&w.isTexture){let E=x.backgroundBlurriness>0;w=e.get(w,E)}return w}function g(x){let w=!1,E=h(x);E===null?m(o,a):E&&E.isColor&&(m(E,1),w=!0);let I=n.xr.getEnvironmentBlendMode();I==="additive"?t.buffers.color.setClear(0,0,0,1,s):I==="alpha-blend"&&t.buffers.color.setClear(0,0,0,0,s),(n.autoClear||w)&&(t.buffers.depth.setTest(!0),t.buffers.depth.setMask(!0),t.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function M(x,w){let E=h(w);E&&E.isCubeTexture?(l===void 0&&(l=new en(new Ar(1,1,1),new Ai({name:"BackgroundCubeMaterial",uniforms:Ss(Li.backgroundCube.uniforms),vertexShader:Li.backgroundCube.vertexShader,fragmentShader:Li.backgroundCube.fragmentShader,side:gn,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),l.geometry.deleteAttribute("normal"),l.geometry.deleteAttribute("uv"),l.onBeforeRender=function(I,T,y){this.matrixWorld.copyPosition(y.matrixWorld)},Object.defineProperty(l.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),i.update(l)),l.material.uniforms.envMap.value=E,l.material.uniforms.backgroundBlurriness.value=w.backgroundBlurriness,l.material.uniforms.backgroundIntensity.value=w.backgroundIntensity,l.material.uniforms.backgroundRotation.value.setFromMatrix4(tR.makeRotationFromEuler(w.backgroundRotation)).transpose(),E.isRenderTargetTexture===!1&&l.material.uniforms.backgroundRotation.value.premultiply(IM),l.material.toneMapped=Ke.getTransfer(E.colorSpace)!==ot,(u!==E||d!==E.version||f!==n.toneMapping)&&(l.material.needsUpdate=!0,u=E,d=E.version,f=n.toneMapping),l.layers.enableAll(),x.unshift(l,l.geometry,l.material,0,0,null)):E&&E.isTexture&&(c===void 0&&(c=new en(new Ka(2,2),new Ai({name:"BackgroundMaterial",uniforms:Ss(Li.background.uniforms),vertexShader:Li.background.vertexShader,fragmentShader:Li.background.fragmentShader,side:Pr,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),c.geometry.deleteAttribute("normal"),Object.defineProperty(c.material,"map",{get:function(){return this.uniforms.t2D.value}}),i.update(c)),c.material.uniforms.t2D.value=E,c.material.uniforms.backgroundIntensity.value=w.backgroundIntensity,c.material.toneMapped=Ke.getTransfer(E.colorSpace)!==ot,E.matrixAutoUpdate===!0&&E.updateMatrix(),c.material.uniforms.uvTransform.value.copy(E.matrix),(u!==E||d!==E.version||f!==n.toneMapping)&&(c.material.needsUpdate=!0,u=E,d=E.version,f=n.toneMapping),c.layers.enableAll(),x.unshift(c,c.geometry,c.material,0,0,null))}function m(x,w){x.getRGB(qd,ig(n)),t.buffers.color.setClear(qd.r,qd.g,qd.b,w,s)}function p(){l!==void 0&&(l.geometry.dispose(),l.material.dispose(),l=void 0),c!==void 0&&(c.geometry.dispose(),c.material.dispose(),c=void 0)}return{getClearColor:function(){return o},setClearColor:function(x,w=1){o.set(x),a=w,m(o,a)},getClearAlpha:function(){return a},setClearAlpha:function(x){a=x,m(o,a)},render:g,addToRenderList:M,dispose:p}}function iR(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=f(null),s=r,o=!1;function a(L,F,G,D,z){let $=!1,H=d(L,D,G,F);s!==H&&(s=H,l(s.object)),$=h(L,D,G,z),$&&g(L,D,G,z),z!==null&&e.update(z,n.ELEMENT_ARRAY_BUFFER),($||o)&&(o=!1,E(L,F,G,D),z!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get(z).buffer))}function c(){return n.createVertexArray()}function l(L){return n.bindVertexArray(L)}function u(L){return n.deleteVertexArray(L)}function d(L,F,G,D){let z=D.wireframe===!0,$=i[F.id];$===void 0&&($={},i[F.id]=$);let H=L.isInstancedMesh===!0?L.id:0,te=$[H];te===void 0&&(te={},$[H]=te);let j=te[G.id];j===void 0&&(j={},te[G.id]=j);let J=j[z];return J===void 0&&(J=f(c()),j[z]=J),J}function f(L){let F=[],G=[],D=[];for(let z=0;z<t;z++)F[z]=0,G[z]=0,D[z]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:F,enabledAttributes:G,attributeDivisors:D,object:L,attributes:{},index:null}}function h(L,F,G,D){let z=s.attributes,$=F.attributes,H=0,te=G.getAttributes();for(let j in te)if(te[j].location>=0){let Q=z[j],Me=$[j];if(Me===void 0&&(j==="instanceMatrix"&&L.instanceMatrix&&(Me=L.instanceMatrix),j==="instanceColor"&&L.instanceColor&&(Me=L.instanceColor)),Q===void 0||Q.attribute!==Me||Me&&Q.data!==Me.data)return!0;H++}return s.attributesNum!==H||s.index!==D}function g(L,F,G,D){let z={},$=F.attributes,H=0,te=G.getAttributes();for(let j in te)if(te[j].location>=0){let Q=$[j];Q===void 0&&(j==="instanceMatrix"&&L.instanceMatrix&&(Q=L.instanceMatrix),j==="instanceColor"&&L.instanceColor&&(Q=L.instanceColor));let Me={};Me.attribute=Q,Q&&Q.data&&(Me.data=Q.data),z[j]=Me,H++}s.attributes=z,s.attributesNum=H,s.index=D}function M(){let L=s.newAttributes;for(let F=0,G=L.length;F<G;F++)L[F]=0}function m(L){p(L,0)}function p(L,F){let G=s.newAttributes,D=s.enabledAttributes,z=s.attributeDivisors;G[L]=1,D[L]===0&&(n.enableVertexAttribArray(L),D[L]=1),z[L]!==F&&(n.vertexAttribDivisor(L,F),z[L]=F)}function x(){let L=s.newAttributes,F=s.enabledAttributes;for(let G=0,D=F.length;G<D;G++)F[G]!==L[G]&&(n.disableVertexAttribArray(G),F[G]=0)}function w(L,F,G,D,z,$,H){H===!0?n.vertexAttribIPointer(L,F,G,z,$):n.vertexAttribPointer(L,F,G,D,z,$)}function E(L,F,G,D){M();let z=D.attributes,$=G.getAttributes(),H=F.defaultAttributeValues;for(let te in $){let j=$[te];if(j.location>=0){let J=z[te];if(J===void 0&&(te==="instanceMatrix"&&L.instanceMatrix&&(J=L.instanceMatrix),te==="instanceColor"&&L.instanceColor&&(J=L.instanceColor)),J!==void 0){let Q=J.normalized,Me=J.itemSize,Fe=e.get(J);if(Fe===void 0)continue;let Y=Fe.buffer,pe=Fe.type,ve=Fe.bytesPerElement,Ce=pe===n.INT||pe===n.UNSIGNED_INT||J.gpuType===ud;if(J.isInterleavedBufferAttribute){let me=J.data,Ue=me.isInterleavedBuffer===!0?me.stride:me.itemSize,Rt=J.offset;if(me.isInstancedInterleavedBuffer||me.isInstancedBufferAttribute){for(let Re=0;Re<j.locationSize;Re++)p(j.location+Re,me.meshPerAttribute);L.isInstancedMesh!==!0&&D._maxInstanceCount===void 0&&(D._maxInstanceCount=me.meshPerAttribute*me.count)}else for(let Re=0;Re<j.locationSize;Re++)m(j.location+Re);n.bindBuffer(n.ARRAY_BUFFER,Y);for(let Re=0;Re<j.locationSize;Re++)w(j.location+Re,Me/j.locationSize,pe,Q,Ue*ve,(Rt+Me/j.locationSize*Re)*ve,Ce)}else{if(J.isInstancedBufferAttribute){for(let me=0;me<j.locationSize;me++)p(j.location+me,J.meshPerAttribute);L.isInstancedMesh!==!0&&D._maxInstanceCount===void 0&&(D._maxInstanceCount=J.meshPerAttribute*J.count)}else for(let me=0;me<j.locationSize;me++)m(j.location+me);n.bindBuffer(n.ARRAY_BUFFER,Y);for(let me=0;me<j.locationSize;me++)w(j.location+me,Me/j.locationSize,pe,Q,Me*ve,Me/j.locationSize*me*ve,Ce)}}else if(H!==void 0){let Q=H[te];if(Q!==void 0)switch(Q.length){case 2:n.vertexAttrib2fv(j.location,Q);break;case 3:n.vertexAttrib3fv(j.location,Q);break;case 4:n.vertexAttrib4fv(j.location,Q);break;default:n.vertexAttrib1fv(j.location,Q)}}}}x()}function I(){N();for(let L in i){let F=i[L];for(let G in F){let D=F[G];for(let z in D){let $=D[z];for(let H in $)u($[H].object),delete $[H];delete D[z]}}delete i[L]}}function T(L){if(i[L.id]===void 0)return;let F=i[L.id];for(let G in F){let D=F[G];for(let z in D){let $=D[z];for(let H in $)u($[H].object),delete $[H];delete D[z]}}delete i[L.id]}function y(L){for(let F in i){let G=i[F];for(let D in G){let z=G[D];if(z[L.id]===void 0)continue;let $=z[L.id];for(let H in $)u($[H].object),delete $[H];delete z[L.id]}}}function S(L){for(let F in i){let G=i[F],D=L.isInstancedMesh===!0?L.id:0,z=G[D];if(z!==void 0){for(let $ in z){let H=z[$];for(let te in H)u(H[te].object),delete H[te];delete z[$]}delete G[D],Object.keys(G).length===0&&delete i[F]}}}function N(){A(),o=!0,s!==r&&(s=r,l(s.object))}function A(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:N,resetDefaultState:A,dispose:I,releaseStatesOfGeometry:T,releaseStatesOfObject:S,releaseStatesOfProgram:y,initAttributes:M,enableAttribute:m,disableUnusedAttributes:x}}function rR(n,e,t){let i;function r(c){i=c}function s(c,l){n.drawArrays(i,c,l),t.update(l,i,1)}function o(c,l,u){u!==0&&(n.drawArraysInstanced(i,c,l,u),t.update(l,i,u))}function a(c,l,u){if(u===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,c,0,l,0,u);let f=0;for(let h=0;h<u;h++)f+=l[h];t.update(f,i,1)}this.setMode=r,this.render=s,this.renderInstances=o,this.renderMultiDraw=a}function sR(n,e,t,i){let r;function s(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let y=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(y.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function o(y){return!(y!==hi&&i.convert(y)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(y){let S=y===fi&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(y!==En&&y!==di&&!S&&i.convert(y)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE))}function c(y){if(y==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";y="mediump"}return y==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let l=t.precision!==void 0?t.precision:"highp",u=c(l);u!==l&&(Ie("WebGLRenderer:",l,"not supported, using",u,"instead."),l=u);let d=t.reversedDepthBuffer===!0&&e.has("EXT_clip_control"),f=t.logarithmicDepthBuffer===!0&&d===!1;t.reversedDepthBuffer===!0&&d===!1&&Ie("WebGLRenderer: Unable to use reversed depth buffer due to missing EXT_clip_control extension. Fallback to default depth buffer."),t.logarithmicDepthBuffer===!0&&d===!0&&Ie("WebGLRenderer: Logarithmic depth buffer is ignored when reversed depth buffer is enabled.");let h=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),g=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),M=n.getParameter(n.MAX_TEXTURE_SIZE),m=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),p=n.getParameter(n.MAX_VERTEX_ATTRIBS),x=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),w=n.getParameter(n.MAX_VARYING_VECTORS),E=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),I=n.getParameter(n.MAX_SAMPLES),T=n.getParameter(n.SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:s,getMaxPrecision:c,textureFormatReadable:o,textureTypeReadable:a,precision:l,logarithmicDepthBuffer:f,reversedDepthBuffer:d,maxTextures:h,maxVertexTextures:g,maxTextureSize:M,maxCubemapSize:m,maxAttributes:p,maxVertexUniforms:x,maxVaryings:w,maxFragmentUniforms:E,maxSamples:I,samples:T}}function oR(n){let e=this,t=null,i=0,r=!1,s=!1,o=new fn,a=new Le,c={value:null,needsUpdate:!1};this.uniform=c,this.numPlanes=0,this.numIntersection=0,this.init=function(d,f){let h=d.length!==0||f||i!==0||r;return r=f,i=d.length,h},this.beginShadows=function(){s=!0,u(null)},this.endShadows=function(){s=!1},this.setGlobalState=function(d,f){t=u(d,f,0)},this.setState=function(d,f,h){let g=d.clippingPlanes,M=d.clipIntersection,m=d.clipShadows,p=n.get(d);if(!r||g===null||g.length===0||s&&!m)s?u(null):l();else{let x=s?0:i,w=x*4,E=p.clippingState||null;c.value=E,E=u(g,f,w,h);for(let I=0;I!==w;++I)E[I]=t[I];p.clippingState=E,this.numIntersection=M?this.numPlanes:0,this.numPlanes+=x}};function l(){c.value!==t&&(c.value=t,c.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(d,f,h,g){let M=d!==null?d.length:0,m=null;if(M!==0){if(m=c.value,g!==!0||m===null){let p=h+M*4,x=f.matrixWorldInverse;a.getNormalMatrix(x),(m===null||m.length<p)&&(m=new Float32Array(p));for(let w=0,E=h;w!==M;++w,E+=4)o.copy(d[w]).applyMatrix4(x,a),o.normal.toArray(m,E),m[E+3]=o.constant}c.value=m,c.needsUpdate=!0}return e.numPlanes=M,e.numIntersection=0,m}}var vc=class extends On{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new ja(r),this._setTextureOptions(t),this.texture.isRenderTargetTexture=!0}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

				varying vec3 vWorldDirection;

				vec3 transformDirection( in vec3 dir, in mat4 matrix ) {

					return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );

				}

				void main() {

					vWorldDirection = transformDirection( position, modelMatrix );

					#include <begin_vertex>
					#include <project_vertex>

				}
			`,fragmentShader:`

				uniform sampler2D tEquirect;

				varying vec3 vWorldDirection;

				#include <common>

				void main() {

					vec3 direction = normalize( vWorldDirection );

					vec2 sampleUV = equirectUv( direction );

					gl_FragColor = texture2D( tEquirect, sampleUV );

				}
			`},r=new Ar(5,5,5),s=new Ai({name:"CubemapFromEquirect",uniforms:Ss(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:gn,blending:vs});s.uniforms.tEquirect.value=t;let o=new en(r,s),a=t.minFilter;return t.minFilter===ir&&(t.minFilter=mn),new Co(1,10,this).update(e,o),t.minFilter=a,o.geometry.dispose(),o.material.dispose(),this}clear(e,t=!0,i=!0,r=!0){let s=e.getRenderTarget();for(let o=0;o<6;o++)e.setRenderTarget(this,o),e.clear(t,i,r);e.setRenderTarget(s)}},sM=256,aR=3,cR=20,CM=256,AM=16,lR=3,uR=new O,dR=new He,Yd=class n{constructor(e){this._renderer=e,this._cubeSize=0,this._sourceTarget=null,this._cubeCamera=new Co(1,10,null),this._boxMesh=new en(new Ar(5,5,5),null),this._cubemapMaterial=null,this._equirectMaterial=null,this._blurMaterial=null,this._ggxMaterial=null,this._integrationMaterial=null}fromScene(e,t=0,i=.1,r=100,s={}){let{size:o=256,position:a=uR}=s,c=this._renderer;this._setSize(o);let l=this._allocateTarget(),u=this._getSourceTarget(!0);t>0&&(u.texture.mipmapsAutoUpdate=!1);let d=c.autoClear,f=c.autoClearColor,h=c.autoClearDepth,g=c.autoClearStencil,M=e.background;c.autoClear=!0,c.autoClearColor=!0,c.autoClearDepth=!0,c.autoClearStencil=!0,M===null&&(e.background=c.getClearColor(dR));let m=new Co(i,r,u);return m.position.copy(a),m.update(c,e),c.autoClear=d,c.autoClearColor=f,c.autoClearDepth=h,c.autoClearStencil=g,e.background=M,t>0&&(u.texture.mipmapsAutoUpdate=!0,this._blur(l,t)),this._applyPMREM(l),l}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=cM(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=aM(),this._compileMaterial(this._equirectMaterial))}dispose(){this._sourceTarget!==null&&this._sourceTarget.dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose(),this._blurMaterial!==null&&this._blurMaterial.dispose(),this._ggxMaterial!==null&&this._ggxMaterial.dispose(),this._integrationMaterial!==null&&this._integrationMaterial.dispose(),this._boxMesh.geometry.dispose()}static lodToRoughness(e,t){return t>0?1-Math.sqrt(1-e/t):0}_setSize(e){this._cubeSize=Math.max(sM,eg(e))}_fromTexture(e,t){e.mapping===xs||e.mapping===No?this._setSize(e.image.length===0?sM:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4);let i=t||this._allocateTarget();return this._textureToCubemap(e),this._applyPMREM(i),i}_allocateTarget(){let e=this._cubeSize,t=oM(e,!1,!1),i=Math.log2(e)-aR;for(let r=0;r<=i;r++)t.texture.mipmaps.push({width:e>>r,height:e>>r});return t.texture.name="PMREM",t.texture.isPMREMTexture=!0,t}_getSourceTarget(e=!1){let t=this._cubeSize,i=this._sourceTarget;return(i===null||i.width!==t||e&&i.depthBuffer===!1)&&(i!==null&&i.dispose(),this._sourceTarget=oM(t,!0,e)),this._sourceTarget}_compileMaterial(e){this._boxMesh.material=e,this._renderer.compile(this._boxMesh,this._cubeCamera.children[0])}_renderCube(e,t,i){let r=this._boxMesh,s=this._cubeCamera;r.material=i,s.renderTarget=e,s.activeMipmapLevel=t;let o=e.width>>t;e.viewport.set(0,0,o,o),s.update(this._renderer,r),e.viewport.set(0,0,e.width,e.height)}_textureToCubemap(e){let t;e.mapping===xs||e.mapping===No?(this._cubemapMaterial===null&&(this._cubemapMaterial=cM()),t=this._cubemapMaterial,t.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):(this._equirectMaterial===null&&(this._equirectMaterial=aM()),t=this._equirectMaterial),t.uniforms.envMap.value=e,this._renderCube(this._getSourceTarget(),0,t)}_applyPMREM(e){this._ggxMaterial===null&&(this._ggxMaterial=fR(),this._integrationMaterial=hR());let t=this._cubeSize,i=e.texture.mipmaps.length-1,r=this._ggxMaterial.uniforms;r.envMap.value=this._sourceTarget.texture;let s=this._integrationMaterial.uniforms;s.envMap.value=this._sourceTarget.texture,s.sourceLod.value=Math.log2(t/AM);for(let o=0;o<=i;o++){let a=n.lodToRoughness(o,i);if(o>i-lR)s.roughness.value=a,this._renderCube(e,o,this._integrationMaterial);else{let c=a>0?Math.log2(t)+.5*Math.log2(6/(CM*Math.pow(a,4)))+.5:0;r.roughness.value=a,r.lodBias.value=c,this._renderCube(e,o,this._ggxMaterial)}}}_blur(e,t){this._blurMaterial===null&&(this._blurMaterial=pR());let i=this._blurMaterial,r=i.uniforms,s=this._sourceTarget;r.sigma.value=Math.min(t,Math.PI)/Math.SQRT2,r.envMap.value=s.texture,this._renderCube(e,0,i),r.envMap.value=e.texture,this._renderCube(s,0,i)}};function oM(n,e,t){return new vc(n,{minFilter:ir,generateMipmaps:e,type:fi,colorSpace:Fa,depthBuffer:t})}function _c(n,e,t){return new Ai({name:n,uniforms:e,vertexShader:We.cube_vert,fragmentShader:t,side:gn,blending:vs,depthTest:!1,depthWrite:!1})}function fR(){return _c("PMREMGGXConvolution",{envMap:{value:null},roughness:{value:0},lodBias:{value:0}},`

		#define GGX_SAMPLES ${CM}u

		varying vec3 vWorldDirection;

		uniform samplerCube envMap;
		uniform float roughness;
		uniform float lodBias;

		#include <common>

		// Van der Corput radical inverse
		float radicalInverse_VdC( uint bits ) {

			bits = ( bits << 16u ) | ( bits >> 16u );
			bits = ( ( bits & 0x55555555u ) << 1u ) | ( ( bits & 0xAAAAAAAAu ) >> 1u );
			bits = ( ( bits & 0x33333333u ) << 2u ) | ( ( bits & 0xCCCCCCCCu ) >> 2u );
			bits = ( ( bits & 0x0F0F0F0Fu ) << 4u ) | ( ( bits & 0xF0F0F0F0u ) >> 4u );
			bits = ( ( bits & 0x00FF00FFu ) << 8u ) | ( ( bits & 0xFF00FF00u ) >> 8u );
			return float( bits ) * 2.3283064365386963e-10; // / 0x100000000

		}

		// Hammersley sequence
		vec2 hammersley( uint i, uint N ) {

			return vec2( float( i ) / float( N ), radicalInverse_VdC( i ) );

		}

		void main() {

			vec3 N = normalize( vWorldDirection );

			// For very low roughness, just sample the environment directly
			if ( roughness < 0.001 ) {

				gl_FragColor = vec4( textureLod( envMap, N, 0.0 ).rgb, 1.0 );
				return;

			}

			float alpha = roughness * roughness;
			float alpha2 = alpha * alpha;

			// Tangent space basis for VNDF sampling
			vec3 up = abs( N.z ) < 0.999 ? vec3( 0.0, 0.0, 1.0 ) : vec3( 1.0, 0.0, 0.0 );
			vec3 tangent = normalize( cross( up, N ) );
			vec3 bitangent = cross( N, tangent );

			vec3 prefilteredColor = vec3( 0.0 );
			float totalWeight = 0.0;

			for ( uint i = 0u; i < GGX_SAMPLES; i ++ ) {

				vec2 Xi = hammersley( i, GGX_SAMPLES );

				// With V = N, sample the reflected direction directly.
				float invQ = 1.0 / ( 1.0 - Xi.x + alpha2 * Xi.x );
				float NdotL = ( 1.0 - Xi.x - alpha2 * Xi.x ) * invQ;

				if ( NdotL > 0.0 ) {

					float phi = 2.0 * PI * Xi.y;
					float sinTheta = 2.0 * alpha * sqrt( Xi.x * ( 1.0 - Xi.x ) ) * invQ;
					vec3 L = N * NdotL + ( tangent * cos( phi ) + bitangent * sin( phi ) ) * sinTheta;

					// Match the source mip to the sample's solid angle. The terms
					// independent of the sample are precomputed in lodBias.
					float d = alpha2 * invQ;
					float lod = max( log2( d ) + lodBias, 0.0 );

					// Weight by NdotL for the split-sum approximation
					prefilteredColor += textureLod( envMap, L, lod ).rgb * NdotL;
					totalWeight += NdotL;

				}

			}

			gl_FragColor = vec4( prefilteredColor / totalWeight, 1.0 );

		}
	`)}function hR(){return _c("PMREMGGXIntegration",{envMap:{value:null},roughness:{value:0},sourceLod:{value:0},sourceSize:{value:AM}},`

		varying vec3 vWorldDirection;

		uniform samplerCube envMap;
		uniform float roughness;
		uniform float sourceLod;
		uniform int sourceSize;

		void main() {

			vec3 N = normalize( vWorldDirection );

			float alpha = roughness * roughness;
			float alpha2 = alpha * alpha;

			float texelSize = 2.0 / float( sourceSize );

			vec3 prefilteredColor = vec3( 0.0 );
			float totalWeight = 0.0;

			// Pair opposite texels: only the one in N's hemisphere contributes.
			for ( int face = 0; face < 3; face ++ ) {

				for ( int y = 0; y < sourceSize; y ++ ) {

					for ( int x = 0; x < sourceSize; x ++ ) {

						vec2 uv = ( vec2( x, y ) + 0.5 ) * texelSize - 1.0;
						vec3 texelDirection = face == 0 ? vec3( 1.0, uv ) : ( face == 1 ? vec3( uv.x, 1.0, uv.y ) : vec3( uv, 1.0 ) );

						float invDistance = inversesqrt( 1.0 + dot( uv, uv ) );
						float NdotL = dot( N, texelDirection );
						texelDirection *= NdotL < 0.0 ? - 1.0 : 1.0;
						NdotL = abs( NdotL ) * invDistance;

						// With V = N, NdotH squared is ( 1 + NdotL ) / 2. Common factors
						// in the GGX distribution and texel solid angle cancel when normalized.
						float d = 1.0 + alpha2 + ( alpha2 - 1.0 ) * NdotL;
						float weight = NdotL * invDistance * invDistance * invDistance / ( d * d );

						prefilteredColor += textureLod( envMap, texelDirection, sourceLod ).rgb * weight;
						totalWeight += weight;

					}

				}

			}

			gl_FragColor = vec4( prefilteredColor / totalWeight, 1.0 );

		}
	`)}function pR(){return _c("PMREMSphericalGaussianBlur",{envMap:{value:null},sigma:{value:0}},`

		#define SAMPLES ${cR}
		#define GOLDEN_ANGLE 2.39996322973

		varying vec3 vWorldDirection;

		uniform samplerCube envMap;
		uniform float sigma;

		#include <common>

		void main() {

			vec3 outputDirection = normalize( vWorldDirection );

			vec3 up = abs( outputDirection.z ) < 0.999 ? vec3( 0.0, 0.0, 1.0 ) : vec3( 1.0, 0.0, 0.0 );
			vec3 tangent = normalize( cross( up, outputDirection ) );
			vec3 bitangent = cross( outputDirection, tangent );

			// Truncate the kernel at three standard deviations or at the antipode.
			float thetaMax = min( 3.0 * sigma, PI );
			float truncation = 1.0 - exp( - 0.5 * thetaMax * thetaMax / ( sigma * sigma ) );

			vec3 accumColor = vec3( 0.0 );
			float accumWeight = 0.0;

			for ( int i = 0; i < SAMPLES; i ++ ) {

				// Stratified inverse-CDF sampling of the Gaussian, placed on a golden-angle spiral.
				float stratum = ( float( i ) + 0.5 ) / float( SAMPLES );
				float theta = sigma * sqrt( - 2.0 * log( 1.0 - stratum * truncation ) );
				float phi = float( i ) * GOLDEN_ANGLE;

				vec3 offset = cos( phi ) * tangent + sin( phi ) * bitangent;
				vec3 sampleDirection = cos( theta ) * outputDirection + sin( theta ) * offset;

				// Correct the planar sample density to solid angle.
				float weight = sin( theta ) / theta;

				accumColor += weight * textureLod( envMap, sampleDirection, 0.0 ).rgb;
				accumWeight += weight;

			}

			gl_FragColor = vec4( accumColor / accumWeight, 1.0 );

		}
	`)}function aM(){return _c("PMREMEquirectangularToCubemap",{envMap:{value:null}},`

		varying vec3 vWorldDirection;

		uniform sampler2D envMap;

		#include <common>

		void main() {

			// Average four subpixel samples to preserve small, bright features.
			vec3 direction = normalize( vWorldDirection );
			vec3 dx = dFdx( direction ) * 0.25;
			vec3 dy = dFdy( direction ) * 0.25;

			vec3 color = textureLod( envMap, equirectUv( normalize( direction - dx - dy ) ), 0.0 ).rgb;
			color += textureLod( envMap, equirectUv( normalize( direction + dx - dy ) ), 0.0 ).rgb;
			color += textureLod( envMap, equirectUv( normalize( direction - dx + dy ) ), 0.0 ).rgb;
			color += textureLod( envMap, equirectUv( normalize( direction + dx + dy ) ), 0.0 ).rgb;
			gl_FragColor = vec4( color * 0.25, 1.0 );

		}
	`)}function cM(){return _c("PMREMCubemapToCubemap",{envMap:{value:null},flipEnvMap:{value:-1}},`

		varying vec3 vWorldDirection;

		uniform samplerCube envMap;
		uniform float flipEnvMap;

		void main() {

			gl_FragColor = vec4( textureCube( envMap, vec3( flipEnvMap * vWorldDirection.x, vWorldDirection.yz ) ).rgb, 1.0 );

		}
	`)}function mR(n){let e=new WeakMap,t=new WeakMap,i=null;function r(f,h=!1){return f==null?null:h?o(f):s(f)}function s(f){if(f&&f.isTexture){let h=f.mapping;if(h===ad||h===cd)if(e.has(f)){let g=e.get(f).texture;return a(g,f.mapping)}else{let g=f.image;if(g&&g.height>0){let M=new vc(g.height);return M.fromEquirectangularTexture(n,f),e.set(f,M),f.addEventListener("dispose",l),a(M.texture,f.mapping)}else return null}}return f}function o(f){if(f&&f.isTexture&&f.isPMREMTexture!==!0){let h=f.mapping,g=h===ad||h===cd,M=h===xs||h===No;if(g||M){let m=t.get(f),p=m!==void 0?m.texture.pmremVersion:0;if(f.isRenderTargetTexture&&f.pmremVersion!==p)return i===null&&(i=new Yd(n)),m=g?i.fromEquirectangular(f,m):i.fromCubemap(f,m),m.texture.pmremVersion=f.pmremVersion,t.set(f,m),m.texture;if(m!==void 0)return m.texture;{let x=f.image;return g&&x&&x.height>0||M&&x&&c(x)?(i===null&&(i=new Yd(n)),m=g?i.fromEquirectangular(f):i.fromCubemap(f),m.texture.pmremVersion=f.pmremVersion,t.set(f,m),f.addEventListener("dispose",u),m.texture):null}}}return f}function a(f,h){return h===ad?f.mapping=xs:h===cd&&(f.mapping=No),f}function c(f){let h=0,g=6;for(let M=0;M<g;M++)f[M]!==void 0&&h++;return h===g}function l(f){let h=f.target;h.removeEventListener("dispose",l);let g=e.get(h);g!==void 0&&(e.delete(h),g.dispose())}function u(f){let h=f.target;h.removeEventListener("dispose",u);let g=t.get(h);g!==void 0&&(t.delete(h),g.dispose())}function d(){e=new WeakMap,t=new WeakMap,i!==null&&(i.dispose(),i=null)}return{get:r,dispose:d}}function gR(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r=n.getExtension(i);return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&Na("WebGLRenderer: "+i+" extension not supported."),r}}}function yR(n,e,t,i){let r={},s=new WeakMap,o=new FinalizationRegistry(g=>delete r[g]);function a(g){c(g.target)}function c(g){g.index!==null&&e.remove(g.index);for(let m in g.attributes)e.remove(g.attributes[m]);g.removeEventListener("dispose",a),delete r[g.id],o.unregister(g);let M=s.get(g);M&&(e.remove(M),s.delete(g)),i.releaseStatesOfGeometry(g),g.isInstancedBufferGeometry===!0&&delete g._maxInstanceCount,t.memory.geometries--}function l(g,M){return r[M.id]!==void 0||(M.addEventListener("dispose",a),r[M.id]=new WeakRef(M),o.register(M,M.id,M),t.memory.geometries++),M}function u(g){let M=g.attributes;for(let m in M)e.update(M[m],n.ARRAY_BUFFER)}function d(g){let M=g.index,m=g.attributes.position,p=0;if(m===void 0)return;let x=M!==null?M.array.length:m.array.length/3-1,w=m.count>=65535?Ga:za,E=new w(Math.ceil(x/3)*6,1),I=E.array;if(M!==null){let y=M.array;p=M.version;for(let S=0,N=0;S<x;S+=3){let A=y[S+0],L=y[S+1],F=y[S+2];I[N++]=A,I[N++]=L,I[N++]=L,I[N++]=F,I[N++]=F,I[N++]=A}}else{p=m.version;for(let y=0,S=0;y<x;y+=3){let N=y+0,A=y+1,L=y+2;I[S++]=N,I[S++]=A,I[S++]=A,I[S++]=L,I[S++]=L,I[S++]=N}}E.version=p;let T=s.get(g);T&&e.remove(T),s.set(g,E)}function f(g){let M=s.get(g);if(M){let m=g.index;m!==null&&M.version<m.version&&d(g)}else d(g);return s.get(g)}function h(){for(let g in r){let M=r[g].deref();M!==void 0&&c(M)}}return{get:l,update:u,getWireframeAttribute:f,dispose:h}}function vR(n,e,t){let i;function r(d){i=d}let s,o;function a(d){s=d.type,o=d.bytesPerElement}function c(d,f){n.drawElements(i,f,s,d*o),t.update(f,i,1)}function l(d,f,h){h!==0&&(n.drawElementsInstanced(i,f,s,d*o,h),t.update(f,i,h))}function u(d,f,h){if(h===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,f,0,s,d,0,h);let M=0;for(let m=0;m<h;m++)M+=f[m];t.update(M,i,1)}this.setMode=r,this.setIndex=a,this.render=c,this.renderInstances=l,this.renderMultiDraw=u}function _R(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(s,o,a){switch(t.calls++,o){case n.TRIANGLES:t.triangles+=a*(s/3);break;case n.LINES:t.lines+=a*(s/2);break;case n.LINE_STRIP:t.lines+=a*(s-1);break;case n.LINE_LOOP:t.lines+=a*s;break;case n.POINTS:t.points+=a*s;break;default:Ne("WebGLInfo: Unknown draw mode:",o);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function xR(n,e,t){let i=new WeakMap,r=new Et;function s(o,a,c){let l=o.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,d=u!==void 0?u.length:0,f=i.get(a);if(f===void 0||f.count!==d){let N=function(){y.dispose(),i.delete(a),a.removeEventListener("dispose",N)};f!==void 0&&f.texture.dispose();let h=a.morphAttributes.position!==void 0,g=a.morphAttributes.normal!==void 0,M=a.morphAttributes.color!==void 0,m=a.morphAttributes.position||[],p=a.morphAttributes.normal||[],x=a.morphAttributes.color||[],w=0;h===!0&&(w=1),g===!0&&(w=2),M===!0&&(w=3);let E=a.attributes.position.count*w,I=1;E>e.maxTextureSize&&(I=Math.ceil(E/e.maxTextureSize),E=e.maxTextureSize);let T=new Float32Array(E*I*4*d),y=new Ba(T,E,I,d);y.type=di,y.needsUpdate=!0;let S=w*4;for(let A=0;A<d;A++){let L=m[A],F=p[A],G=x[A],D=E*I*4*A;for(let z=0;z<L.count;z++){let $=z*S;h===!0&&(r.fromBufferAttribute(L,z),T[D+$+0]=r.x,T[D+$+1]=r.y,T[D+$+2]=r.z,T[D+$+3]=0),g===!0&&(r.fromBufferAttribute(F,z),T[D+$+4]=r.x,T[D+$+5]=r.y,T[D+$+6]=r.z,T[D+$+7]=0),M===!0&&(r.fromBufferAttribute(G,z),T[D+$+8]=r.x,T[D+$+9]=r.y,T[D+$+10]=r.z,T[D+$+11]=G.itemSize===4?r.w:1)}}f={count:d,texture:y,size:new Te(E,I)},i.set(a,f),a.addEventListener("dispose",N)}if(o.isInstancedMesh===!0&&o.morphTexture!==null)c.getUniforms().setValue(n,"morphTexture",o.morphTexture,t);else{let h=0;for(let M=0;M<l.length;M++)h+=l[M];let g=a.morphTargetsRelative?1:1-h;c.getUniforms().setValue(n,"morphTargetBaseInfluence",g),c.getUniforms().setValue(n,"morphTargetInfluences",l)}c.getUniforms().setValue(n,"morphTargetsTexture",f.texture,t),c.getUniforms().setValue(n,"morphTargetsTextureSize",f.size)}return{update:s}}function MR(n,e,t,i,r){let s=new WeakMap;function o(l){let u=r.render.frame,d=l.geometry,f=e.get(l,d);if(s.get(f)!==u&&(e.update(f),s.set(f,u)),l.isInstancedMesh&&(l.hasEventListener("dispose",c)===!1&&l.addEventListener("dispose",c),s.get(l)!==u&&(t.update(l.instanceMatrix,n.ARRAY_BUFFER),l.instanceColor!==null&&t.update(l.instanceColor,n.ARRAY_BUFFER),s.set(l,u))),l.isSkinnedMesh){let h=l.skeleton;s.get(h)!==u&&(h.update(),s.set(h,u))}return f}function a(){s=new WeakMap}function c(l){let u=l.target;u.removeEventListener("dispose",c),i.releaseStatesOfObject(u),t.remove(u.instanceMatrix),u.instanceColor!==null&&t.remove(u.instanceColor)}return{update:o,dispose:a}}var SR={[Pm]:"LINEAR_TONE_MAPPING",[Lm]:"REINHARD_TONE_MAPPING",[Om]:"CINEON_TONE_MAPPING",[Fm]:"ACES_FILMIC_TONE_MAPPING",[Um]:"AGX_TONE_MAPPING",[Bm]:"NEUTRAL_TONE_MAPPING",[km]:"CUSTOM_TONE_MAPPING"};function ER(n,e,t,i,r,s){let o=new On(e,t,{type:n,depthBuffer:r,stencilBuffer:s,samples:i?4:0,storeMultisampledDepthBuffer:!1,storeMultisampledStencilBuffer:!1,resolveDepthBuffer:!1,resolveStencilBuffer:!1}),a=null,c=null,l=null,u=new an;u.setAttribute("position",new rt([-1,3,0,-1,-1,0,3,-1,0],3)),u.setAttribute("uv",new rt([0,2,0,0,2,0],2));let d=new ju({uniforms:{tDiffuse:{value:null}},vertexShader:`
			precision highp float;

			uniform mat4 modelViewMatrix;
			uniform mat4 projectionMatrix;

			attribute vec3 position;
			attribute vec2 uv;

			varying vec2 vUv;

			void main() {
				vUv = uv;
				gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
			}`,fragmentShader:`
			precision highp float;

			uniform sampler2D tDiffuse;

			varying vec2 vUv;

			#include <tonemapping_pars_fragment>
			#include <colorspace_pars_fragment>

			void main() {
				gl_FragColor = texture2D( tDiffuse, vUv );

				#ifdef LINEAR_TONE_MAPPING
					gl_FragColor.rgb = LinearToneMapping( gl_FragColor.rgb );
				#elif defined( REINHARD_TONE_MAPPING )
					gl_FragColor.rgb = ReinhardToneMapping( gl_FragColor.rgb );
				#elif defined( CINEON_TONE_MAPPING )
					gl_FragColor.rgb = CineonToneMapping( gl_FragColor.rgb );
				#elif defined( ACES_FILMIC_TONE_MAPPING )
					gl_FragColor.rgb = ACESFilmicToneMapping( gl_FragColor.rgb );
				#elif defined( AGX_TONE_MAPPING )
					gl_FragColor.rgb = AgXToneMapping( gl_FragColor.rgb );
				#elif defined( NEUTRAL_TONE_MAPPING )
					gl_FragColor.rgb = NeutralToneMapping( gl_FragColor.rgb );
				#elif defined( CUSTOM_TONE_MAPPING )
					gl_FragColor.rgb = CustomToneMapping( gl_FragColor.rgb );
				#endif

				#ifdef SRGB_TRANSFER
					gl_FragColor = sRGBTransferOETF( gl_FragColor );
				#endif
			}`,depthTest:!1,depthWrite:!1}),f=new en(u,d),h=new rc(-1,1,1,-1,0,1),g=null,M=null,m=!1,p,x=null,w=[],E=!1;this.setSize=function(I,T){o.setSize(I,T),a!==null&&a.setSize(I,T),c!==null&&c.setSize(I,T);for(let y=0;y<w.length;y++){let S=w[y];S.setSize&&S.setSize(I,T)}},this.setEffects=function(I){w=I,E=w.length>0&&w[0].isRenderPass===!0;let T=o.width,y=o.height;w.length>0&&a===null&&(a=new On(T,y,{type:fi,depthBuffer:!1,stencilBuffer:!1}),c=new On(T,y,{type:fi,depthBuffer:!1,stencilBuffer:!1}));let S=r===!0&&w.some(N=>N.needsDepthTexture===!0);S!==(o.depthTexture!==null)&&(S===!0&&l===null&&(l=new tr(T,y),s===!0&&(l.format=rr,l.type=Ms)),o.dispose(),o.depthTexture=S?l:null,o.resolveDepthBuffer=S,o.storeMultisampledDepthBuffer=S);for(let N=0;N<w.length;N++){let A=w[N];A.setSize&&A.setSize(T,y),S&&A.needsDepthTexture===!0&&A.setDepthTexture&&A.setDepthTexture(l)}},this.begin=function(I,T){if(m||I.toneMapping===Ni&&w.length===0)return!1;if(x=T,T!==null){let y=T.width,S=T.height;(o.width!==y||o.height!==S)&&this.setSize(y,S)}return E===!1&&I.setRenderTarget(o),p=I.toneMapping,I.toneMapping=Ni,!0},this.hasRenderPass=function(){return E},this.end=function(I,T){I.toneMapping=p,m=!0;let y=o,S=a;for(let N=0;N<w.length;N++){let A=w[N];A.enabled!==!1&&(A.render(I,S,y,T),A.needsSwap!==!1&&(y=S,S=S===a?c:a))}if(g!==I.outputColorSpace||M!==I.toneMapping){g=I.outputColorSpace,M=I.toneMapping,d.defines={},Ke.getTransfer(g)===ot&&(d.defines.SRGB_TRANSFER="");let N=SR[M];N&&(d.defines[N]=""),d.needsUpdate=!0}d.uniforms.tDiffuse.value=y.texture,I.setRenderTarget(x),I.render(f,h),x=null,m=!1},this.isCompositing=function(){return m},this.dispose=function(){o.dispose(),a!==null&&a.dispose(),c!==null&&c.dispose(),u.dispose(),d.dispose()}}var DM=new Sn,RM=new tr(1,1),NM=new $a(1),PM=new Ba,LM=new Bu,OM=new ja,lM=[],uM=[],dM=new Float32Array(16),fM=new Float32Array(9),hM=new Float32Array(4);function Fo(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,s=lM[r];if(s===void 0&&(s=new Float32Array(r),lM[r]=s),e!==0){i.toArray(s,0);for(let o=1,a=0;o!==e;++o)a+=t,n[o].toArray(s,a)}return s}function Bt(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function Vt(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function Kd(n,e){let t=uM[e];t===void 0&&(t=new Int32Array(e),uM[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function bR(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function TR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Bt(t,e))return;n.uniform2fv(this.addr,e),Vt(t,e)}}function wR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(Bt(t,e))return;n.uniform3fv(this.addr,e),Vt(t,e)}}function IR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Bt(t,e))return;n.uniform4fv(this.addr,e),Vt(t,e)}}function CR(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Bt(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),Vt(t,e)}else{if(Bt(t,i))return;hM.set(i),n.uniformMatrix2fv(this.addr,!1,hM),Vt(t,i)}}function AR(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Bt(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),Vt(t,e)}else{if(Bt(t,i))return;fM.set(i),n.uniformMatrix3fv(this.addr,!1,fM),Vt(t,i)}}function DR(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Bt(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),Vt(t,e)}else{if(Bt(t,i))return;dM.set(i),n.uniformMatrix4fv(this.addr,!1,dM),Vt(t,i)}}function RR(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function NR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Bt(t,e))return;n.uniform2iv(this.addr,e),Vt(t,e)}}function PR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Bt(t,e))return;n.uniform3iv(this.addr,e),Vt(t,e)}}function LR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Bt(t,e))return;n.uniform4iv(this.addr,e),Vt(t,e)}}function OR(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function FR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Bt(t,e))return;n.uniform2uiv(this.addr,e),Vt(t,e)}}function kR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Bt(t,e))return;n.uniform3uiv(this.addr,e),Vt(t,e)}}function UR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Bt(t,e))return;n.uniform4uiv(this.addr,e),Vt(t,e)}}function Jd(n){return n.version===0&&(n.compareFunction=Zm,n.needsUpdate=!0),n}function BR(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let s;this.type===n.SAMPLER_2D_SHADOW?s=Jd(RM):s=DM,t.setTexture2D(e||s,r)}function VR(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||LM,r)}function HR(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let s;this.type===n.SAMPLER_CUBE_SHADOW?s=Jd(NM):s=OM,t.setTextureCube(e||s,r)}function zR(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||PM,r)}function GR(n){switch(n){case 5126:return bR;case 35664:return TR;case 35665:return wR;case 35666:return IR;case 35674:return CR;case 35675:return AR;case 35676:return DR;case 5124:case 35670:return RR;case 35667:case 35671:return NR;case 35668:case 35672:return PR;case 35669:case 35673:return LR;case 5125:return OR;case 36294:return FR;case 36295:return kR;case 36296:return UR;case 35678:case 36198:case 36298:case 36306:case 35682:return BR;case 35679:case 36299:case 36307:return VR;case 35680:case 36300:case 36308:case 36293:return HR;case 36289:case 36303:case 36311:case 36292:return zR}}function WR(n,e){n.uniform1fv(this.addr,e)}function jR(n,e){let t=Fo(e,this.size,2);n.uniform2fv(this.addr,t)}function $R(n,e){let t=Fo(e,this.size,3);n.uniform3fv(this.addr,t)}function qR(n,e){let t=Fo(e,this.size,4);n.uniform4fv(this.addr,t)}function XR(n,e){let t=Fo(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function YR(n,e){let t=Fo(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function ZR(n,e){let t=Fo(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function KR(n,e){n.uniform1iv(this.addr,e)}function JR(n,e){n.uniform2iv(this.addr,e)}function QR(n,e){n.uniform3iv(this.addr,e)}function e1(n,e){n.uniform4iv(this.addr,e)}function t1(n,e){n.uniform1uiv(this.addr,e)}function n1(n,e){n.uniform2uiv(this.addr,e)}function i1(n,e){n.uniform3uiv(this.addr,e)}function r1(n,e){n.uniform4uiv(this.addr,e)}function s1(n,e,t){let i=this.cache,r=e.length,s=Kd(t,r);Bt(i,s)||(n.uniform1iv(this.addr,s),Vt(i,s));let o;this.type===n.SAMPLER_2D_SHADOW?o=Jd(RM):o=DM;for(let a=0;a!==r;++a)t.setTexture2D(e[a]||o,s[a])}function o1(n,e,t){let i=this.cache,r=e.length,s=Kd(t,r);Bt(i,s)||(n.uniform1iv(this.addr,s),Vt(i,s));for(let o=0;o!==r;++o)t.setTexture3D(e[o]||LM,s[o])}function a1(n,e,t){let i=this.cache,r=e.length,s=Kd(t,r);Bt(i,s)||(n.uniform1iv(this.addr,s),Vt(i,s));let o;this.type===n.SAMPLER_CUBE_SHADOW?o=Jd(NM):o=OM;for(let a=0;a!==r;++a)t.setTextureCube(e[a]||o,s[a])}function c1(n,e,t){let i=this.cache,r=e.length,s=Kd(t,r);Bt(i,s)||(n.uniform1iv(this.addr,s),Vt(i,s));for(let o=0;o!==r;++o)t.setTexture2DArray(e[o]||PM,s[o])}function l1(n){switch(n){case 5126:return WR;case 35664:return jR;case 35665:return $R;case 35666:return qR;case 35674:return XR;case 35675:return YR;case 35676:return ZR;case 5124:case 35670:return KR;case 35667:case 35671:return JR;case 35668:case 35672:return QR;case 35669:case 35673:return e1;case 5125:return t1;case 36294:return n1;case 36295:return i1;case 36296:return r1;case 35678:case 36198:case 36298:case 36306:case 35682:return s1;case 35679:case 36299:case 36307:return o1;case 35680:case 36300:case 36308:case 36293:return a1;case 36289:case 36303:case 36311:case 36292:return c1}}var lg=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=GR(t.type)}},ug=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=l1(t.type)}},dg=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let s=0,o=r.length;s!==o;++s){let a=r[s];a.setValue(e,t[a.id],i)}}},ag=/(\w+)(\])?(\[|\.)?/g;function pM(n,e){n.seq.push(e),n.map[e.id]=e}function u1(n,e,t){let i=n.name,r=i.length;for(ag.lastIndex=0;;){let s=ag.exec(i),o=ag.lastIndex,a=s[1],c=s[2]==="]",l=s[3];if(c&&(a=a|0),l===void 0||l==="["&&o+2===r){pM(t,l===void 0?new lg(a,n,e):new ug(a,n,e));break}else{let d=t.map[a];d===void 0&&(d=new dg(a),pM(t,d)),t=d}}}var Oo=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let o=0;o<i;++o){let a=e.getActiveUniform(t,o),c=e.getUniformLocation(t,a.name);u1(a,c,this)}let r=[],s=[];for(let o of this.seq)o.type===e.SAMPLER_2D_SHADOW||o.type===e.SAMPLER_CUBE_SHADOW||o.type===e.SAMPLER_2D_ARRAY_SHADOW?r.push(o):s.push(o);r.length>0&&(this.seq=r.concat(s))}setValue(e,t,i,r){let s=this.map[t];s!==void 0&&s.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let s=0,o=t.length;s!==o;++s){let a=t[s],c=i[a.id];c.needsUpdate!==!1&&a.setValue(e,c.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,s=e.length;r!==s;++r){let o=e[r];o.id in t&&i.push(o)}return i}};function mM(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var d1=37297,f1=0;function h1(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),s=Math.min(e+6,t.length);for(let o=r;o<s;o++){let a=o+1;i.push(`${a===e?">":" "} ${a}: ${t[o]}`)}return i.join(`
`)}var gM=new Le;function p1(n){Ke._getMatrix(gM,Ke.workingColorSpace,n);let e=`mat3( ${gM.elements.map(t=>t.toFixed(4))} )`;switch(Ke.getTransfer(n)){case ka:return[e,"LinearTransferOETF"];case ot:return[e,"sRGBTransferOETF"];default:return Ie("WebGLProgram: Unsupported color space: ",n),[e,"LinearTransferOETF"]}}function yM(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),s=(n.getShaderInfoLog(e)||"").trim();if(i&&s==="")return"";let o=/ERROR: 0:(\d+)/.exec(s);if(o){let a=parseInt(o[1]);return t.toUpperCase()+`

`+s+`

`+h1(n.getShaderSource(e),a)}else return s}function m1(n,e){let t=p1(e);return[`vec4 ${n}( vec4 value ) {`,`	return ${t[1]}( vec4( ${t[0]} * value.rgb, value.a ) );`,"}"].join(`
`)}var g1={[Pm]:"Linear",[Lm]:"Reinhard",[Om]:"Cineon",[Fm]:"ACESFilmic",[Um]:"AgX",[Bm]:"Neutral",[km]:"Custom"};function y1(n,e){let t=g1[e];return t===void 0?(Ie("WebGLProgram: Unsupported toneMapping:",e),"vec3 "+n+"( vec3 color ) { return LinearToneMapping( color ); }"):"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var Xd=new O;function v1(){Ke.getLuminanceCoefficients(Xd);let n=Xd.x.toFixed(4),e=Xd.y.toFixed(4),t=Xd.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function _1(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(yc).join(`
`)}function x1(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function M1(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let s=n.getActiveAttrib(e,r),o=s.name,a=1;s.type===n.FLOAT_MAT2&&(a=2),s.type===n.FLOAT_MAT3&&(a=3),s.type===n.FLOAT_MAT4&&(a=4),t[o]={type:s.type,location:n.getAttribLocation(e,o),locationSize:a}}return t}function yc(n){return n!==""}function vM(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_SUN_LIGHTS/g,e.numSunLights).replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_SUN_LIGHT_SHADOWS/g,e.numSunLightShadows).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows).replace(/NUM_LIGHT_PROBE_GRIDS/g,e.numLightProbeGrids)}function _M(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var S1=/^[ \t]*#include +<([\w\d./]+)>/gm;function fg(n){return n.replace(S1,E1)}function E1(n,e){let t=We[e];if(t===void 0)throw new Error("THREE.WebGLProgram: Can not resolve #include <"+e+">");return fg(t)}var b1=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function xM(n){return n.replace(b1,T1)}function T1(n,e,t,i){let r="";for(let s=parseInt(e);s<parseInt(t);s++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+s+" ]").replace(/UNROLLED_LOOP_INDEX/g,s);return r}function MM(n){let e=`precision ${n.precision} float;
	precision ${n.precision} int;
	precision ${n.precision} sampler2D;
	precision ${n.precision} samplerCube;
	precision ${n.precision} sampler3D;
	precision ${n.precision} sampler2DArray;
	precision ${n.precision} sampler2DShadow;
	precision ${n.precision} samplerCubeShadow;
	precision ${n.precision} sampler2DArrayShadow;
	precision ${n.precision} isampler2D;
	precision ${n.precision} isampler3D;
	precision ${n.precision} isamplerCube;
	precision ${n.precision} isampler2DArray;
	precision ${n.precision} usampler2D;
	precision ${n.precision} usampler3D;
	precision ${n.precision} usamplerCube;
	precision ${n.precision} usampler2DArray;
	`;return n.precision==="highp"?e+=`
#define HIGH_PRECISION`:n.precision==="mediump"?e+=`
#define MEDIUM_PRECISION`:n.precision==="lowp"&&(e+=`
#define LOW_PRECISION`),e}var w1={[cc]:"SHADOWMAP_TYPE_PCF",[Do]:"SHADOWMAP_TYPE_VSM"};function I1(n){return w1[n.shadowMapType]||"SHADOWMAP_TYPE_BASIC"}var C1={[No]:"ENVMAP_MODE_REFRACTION"};function A1(n){return n.envMap===!1?"ENVMAP_MODE_REFLECTION":C1[n.envMapMode]||"ENVMAP_MODE_REFLECTION"}var D1={[Nm]:"ENVMAP_BLENDING_MULTIPLY",[kx]:"ENVMAP_BLENDING_MIX",[Ux]:"ENVMAP_BLENDING_ADD"};function R1(n){return n.envMap===!1?"ENVMAP_BLENDING_NONE":D1[n.combine]||"ENVMAP_BLENDING_NONE"}function N1(n,e,t,i){let r=n.getContext(),s=t.defines,o=t.vertexShader,a=t.fragmentShader,c=I1(t),l=t.envMapPMREM===!0?"ENVMAP_TYPE_PMREM":"ENVMAP_TYPE_CUBE",u=A1(t),d=R1(t),f=_1(t),h=x1(s),g=r.createProgram(),M,m,p=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(M=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,h].filter(yc).join(`
`),M.length>0&&(M+=`
`),m=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,h].filter(yc).join(`
`),m.length>0&&(m+=`
`)):(M=[MM(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,h,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.diffuseRoughnessMap?"#define USE_DIFFUSE_ROUGHNESSMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.diffuseRoughnessMapUv?"#define DIFFUSE_ROUGHNESSMAP_UV "+t.diffuseRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexNormals?"#define HAS_NORMAL":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(yc).join(`
`),m=[MM(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,h,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+l:"",t.envMap?"#define "+u:"",t.envMap?"#define "+d:"",t.envMapPMREM?"#define ENVMAP_MAX_LOD "+t.envMapMaxLod+".0":"",t.envMapPMREM?"#define ENVMAP_SIZE "+t.envMapSize+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.packedNormalMap?"#define USE_PACKED_NORMALMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.diffuseRoughness?"#define USE_DIFFUSE_ROUGHNESS":"",t.diffuseRoughnessMap?"#define USE_DIFFUSE_ROUGHNESSMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.retroreflection?"#define USE_RETROREFLECTION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor?"#define USE_COLOR":"",t.vertexAlphas||t.batchingColor?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.decodeVideoTextureEmissive?"#define DECODE_VIDEO_TEXTURE_EMISSIVE":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==Ni?"#define TONE_MAPPING":"",t.toneMapping!==Ni?We.tonemapping_pars_fragment:"",t.toneMapping!==Ni?y1("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",We.colorspace_pars_fragment,m1("linearToOutputTexel",t.outputColorSpace),v1(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(yc).join(`
`)),o=fg(o),o=vM(o,t),o=_M(o,t),a=fg(a),a=vM(a,t),a=_M(a,t),o=xM(o),a=xM(a),t.isRawShaderMaterial!==!0&&(p=`#version 300 es
`,M=[f,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+M,m=["#define varying in",t.glslVersion===Km?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===Km?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+m);let x=p+M+o,w=p+m+a,E=mM(r,r.VERTEX_SHADER,x),I=mM(r,r.FRAGMENT_SHADER,w);r.attachShader(g,E),r.attachShader(g,I),t.index0AttributeName!==void 0?r.bindAttribLocation(g,0,t.index0AttributeName):t.hasPositionAttribute===!0&&r.bindAttribLocation(g,0,"position"),r.linkProgram(g);function T(A){if(n.debug.checkShaderErrors){let L=r.getProgramInfoLog(g)||"",F=r.getShaderInfoLog(E)||"",G=r.getShaderInfoLog(I)||"",D=L.trim(),z=F.trim(),$=G.trim(),H=!0,te=!0;if(r.getProgramParameter(g,r.LINK_STATUS)===!1)if(H=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,g,E,I);else{let j=yM(r,E,"vertex"),J=yM(r,I,"fragment");Ne("WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(g,r.VALIDATE_STATUS)+`

Material Name: `+A.name+`
Material Type: `+A.type+`

Program Info Log: `+D+`
`+j+`
`+J)}else D!==""?Ie("WebGLProgram: Program Info Log:",D):(z===""||$==="")&&(te=!1);te&&(A.diagnostics={runnable:H,programLog:D,vertexShader:{log:z,prefix:M},fragmentShader:{log:$,prefix:m}})}r.deleteShader(E),r.deleteShader(I),y=new Oo(r,g),S=M1(r,g)}let y;this.getUniforms=function(){return y===void 0&&T(this),y};let S;this.getAttributes=function(){return S===void 0&&T(this),S};let N=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return N===!1&&(N=r.getProgramParameter(g,d1)),N},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(g),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=f1++,this.cacheKey=e,this.usedTimes=1,this.program=g,this.vertexShader=E,this.fragmentShader=I,this}var P1=0,hg=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e,t,i){let r=this._getShaderCacheForMaterial(e);return r.has(t)===!1&&(r.add(t),t.usedTimes++),r.has(i)===!1&&(r.add(i),i.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderStage(e){return this._getShaderStage(e.vertexShader)}getFragmentShaderStage(e){return this._getShaderStage(e.fragmentShader)}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new pg(e),t.set(e,i)),i}},pg=class{constructor(e){this.id=P1++,this.code=e,this.usedTimes=0}};function L1(n){return n===Lr||n===pc||n===mc}function O1(n,e,t,i,r,s){let o=new To,a=new hg,c=new Set,l=[],u=new Map,d=i.logarithmicDepthBuffer,f={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distance",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function h(y){return c.add(y),y===0?"uv":`uv${y}`}function g(y,S,N,A,L,F){let G=A.fog,D=L.geometry,z=y.isMeshStandardMaterial||y.isMeshLambertMaterial||y.isMeshPhongMaterial?A.environment:null,$=y.isMeshStandardMaterial||y.isMeshLambertMaterial&&!y.envMap||y.isMeshPhongMaterial&&!y.envMap,H=e.get(y.envMap||z,$),te=f[y.type],j=i.precision;y.precision!==null&&(j=i.getMaxPrecision(y.precision),j!==y.precision&&Ie("WebGLProgram.getParameters:",y.precision,"not supported, using",j,"instead."));let J=D.morphAttributes.position||D.morphAttributes.normal||D.morphAttributes.color,Q=J!==void 0?J.length:0,Me=0;D.morphAttributes.position!==void 0&&(Me=1),D.morphAttributes.normal!==void 0&&(Me=2),D.morphAttributes.color!==void 0&&(Me=3);let Fe,Y,pe,ve;if(te){let fe=Li[te];Fe=fe.vertexShader,Y=fe.fragmentShader}else{Fe=y.vertexShader,Y=y.fragmentShader;let fe=a.getVertexShaderStage(y),ct=a.getFragmentShaderStage(y);a.update(y,fe,ct),pe=fe.id,ve=ct.id}let Ce=n.getRenderTarget(),me=n.state.buffers.depth.getReversed(),Ue=L.isInstancedMesh===!0,Rt=L.isBatchedMesh===!0,Re=!!y.map,at=!!y.matcap,pt=!!H,qe=pt&&H.isPMREMTexture===!0,Yt=!!y.aoMap,kt=!!y.lightMap,vn=!!y.bumpMap&&y.wireframe===!1,bt=!!y.normalMap,pi=!!y.displacementMap,zt=!!y.emissiveMap,kn=!!y.metalnessMap,Un=!!y.roughnessMap,Bn=y.anisotropy>0,P=y.clearcoat>0,Vn=y.diffuseRoughness>0,Mt=y.dispersion>0,St=y.retroreflectivity>0,de=y.iridescence>0,Ut=y.sheen>0,Ae=y.transmission>0,Oe=Bn&&!!y.anisotropyMap,b=P&&!!y.clearcoatMap,v=P&&!!y.clearcoatNormalMap,U=P&&!!y.clearcoatRoughnessMap,ee=Vn&&!!y.diffuseRoughnessMap,K=de&&!!y.iridescenceMap,re=de&&!!y.iridescenceThicknessMap,le=Ut&&!!y.sheenColorMap,q=Ut&&!!y.sheenRoughnessMap,X=!!y.specularMap,oe=!!y.specularColorMap,C=!!y.specularIntensityMap,Z=Ae&&!!y.transmissionMap,W=Ae&&!!y.thicknessMap,ae=!!y.gradientMap,ie=!!y.alphaMap,se=y.alphaTest>0,Ee=!!y.alphaHash,ze=!!y.extensions,ne=Ni;y.toneMapped&&(Ce===null||Ce.isXRRenderTarget===!0)&&(ne=n.toneMapping);let be=qe?H.isCompressedCubeTexture?H.image[0].mipmaps:H.mipmaps:null,Be={shaderID:te,shaderType:y.type,shaderName:y.name,vertexShader:Fe,fragmentShader:Y,defines:y.defines,customVertexShaderID:pe,customFragmentShaderID:ve,isRawShaderMaterial:y.isRawShaderMaterial===!0,glslVersion:y.glslVersion,precision:j,batching:Rt,batchingColor:Rt&&L._colorsTexture!==null,instancing:Ue,instancingColor:Ue&&L.instanceColor!==null,instancingMorph:Ue&&L.morphTexture!==null,outputColorSpace:Ce===null?n.outputColorSpace:Ce.isXRRenderTarget===!0?Ce.texture.colorSpace:Ke.workingColorSpace,alphaToCoverage:!!y.alphaToCoverage,map:Re,matcap:at,envMap:pt,envMapMode:pt&&H.mapping,envMapPMREM:qe,envMapMaxLod:qe?be.length-1:null,envMapSize:qe?be[0].width:null,aoMap:Yt,lightMap:kt,bumpMap:vn,normalMap:bt,displacementMap:pi,emissiveMap:zt,normalMapObjectSpace:bt&&y.normalMapType===Hx,normalMapTangentSpace:bt&&y.normalMapType===$d,packedNormalMap:bt&&y.normalMapType===$d&&L1(y.normalMap.format),metalnessMap:kn,roughnessMap:Un,anisotropy:Bn,anisotropyMap:Oe,clearcoat:P,clearcoatMap:b,clearcoatNormalMap:v,clearcoatRoughnessMap:U,diffuseRoughness:Vn,diffuseRoughnessMap:ee,dispersion:Mt,retroreflection:St,iridescence:de,iridescenceMap:K,iridescenceThicknessMap:re,sheen:Ut,sheenColorMap:le,sheenRoughnessMap:q,specularMap:X,specularColorMap:oe,specularIntensityMap:C,transmission:Ae,transmissionMap:Z,thicknessMap:W,gradientMap:ae,opaque:y.transparent===!1&&y.blending===Ro&&y.alphaToCoverage===!1,alphaMap:ie,alphaTest:se,alphaHash:Ee,combine:y.combine,mapUv:Re&&h(y.map.channel),aoMapUv:Yt&&h(y.aoMap.channel),lightMapUv:kt&&h(y.lightMap.channel),bumpMapUv:vn&&h(y.bumpMap.channel),normalMapUv:bt&&h(y.normalMap.channel),displacementMapUv:pi&&h(y.displacementMap.channel),emissiveMapUv:zt&&h(y.emissiveMap.channel),metalnessMapUv:kn&&h(y.metalnessMap.channel),roughnessMapUv:Un&&h(y.roughnessMap.channel),anisotropyMapUv:Oe&&h(y.anisotropyMap.channel),clearcoatMapUv:b&&h(y.clearcoatMap.channel),clearcoatNormalMapUv:v&&h(y.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:U&&h(y.clearcoatRoughnessMap.channel),diffuseRoughnessMapUv:ee&&h(y.diffuseRoughnessMap.channel),iridescenceMapUv:K&&h(y.iridescenceMap.channel),iridescenceThicknessMapUv:re&&h(y.iridescenceThicknessMap.channel),sheenColorMapUv:le&&h(y.sheenColorMap.channel),sheenRoughnessMapUv:q&&h(y.sheenRoughnessMap.channel),specularMapUv:X&&h(y.specularMap.channel),specularColorMapUv:oe&&h(y.specularColorMap.channel),specularIntensityMapUv:C&&h(y.specularIntensityMap.channel),transmissionMapUv:Z&&h(y.transmissionMap.channel),thicknessMapUv:W&&h(y.thicknessMap.channel),alphaMapUv:ie&&h(y.alphaMap.channel),vertexTangents:!!D.attributes.tangent&&(bt||Bn),vertexNormals:!!D.attributes.normal,vertexColors:y.vertexColors,vertexAlphas:y.vertexColors===!0&&!!D.attributes.color&&D.attributes.color.itemSize===4,pointsUvs:L.isPoints===!0&&!!D.attributes.uv&&(Re||ie),fog:!!G,useFog:y.fog===!0,fogExp2:!!G&&G.isFogExp2,flatShading:y.wireframe===!1&&(y.flatShading===!0||D.attributes.normal===void 0&&bt===!1&&(y.isMeshLambertMaterial||y.isMeshPhongMaterial||y.isMeshStandardMaterial||y.isMeshPhysicalMaterial)),sizeAttenuation:y.sizeAttenuation===!0,logarithmicDepthBuffer:d,reversedDepthBuffer:me,skinning:L.isSkinnedMesh===!0,hasPositionAttribute:D.attributes.position!==void 0,morphTargets:D.morphAttributes.position!==void 0,morphNormals:D.morphAttributes.normal!==void 0,morphColors:D.morphAttributes.color!==void 0,morphTargetsCount:Q,morphTextureStride:Me,numSunLights:S.sun.length,numDirLights:S.directional.length,numPointLights:S.point.length,numSpotLights:S.spot.length,numSpotLightMaps:S.spotLightMap.length,numRectAreaLights:S.rectArea.length,numHemiLights:S.hemi.length,numSunLightShadows:S.sunShadowMap.length,numDirLightShadows:S.directionalShadowMap.length,numPointLightShadows:S.pointShadowMap.length,numSpotLightShadows:S.spotShadowMap.length,numSpotLightShadowsWithMaps:S.numSpotLightShadowsWithMaps,numLightProbes:S.numLightProbes,numLightProbeGrids:F.filter(fe=>fe.texture!==null).length,numClippingPlanes:s.numPlanes,numClipIntersection:s.numIntersection,dithering:y.dithering,shadowMapEnabled:n.shadowMap.enabled&&N.length>0,shadowMapType:n.shadowMap.type,toneMapping:ne,decodeVideoTexture:Re&&y.map.isVideoTexture===!0&&Ke.getTransfer(y.map.colorSpace)===ot,decodeVideoTextureEmissive:zt&&y.emissiveMap.isVideoTexture===!0&&Ke.getTransfer(y.emissiveMap.colorSpace)===ot,premultipliedAlpha:y.premultipliedAlpha,doubleSided:y.side===Ri,flipSided:y.side===gn,useDepthPacking:y.depthPacking>=0,depthPacking:y.depthPacking||0,index0AttributeName:y.index0AttributeName,extensionClipCullDistance:ze&&y.extensions.clipCullDistance===!0&&t.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(ze&&y.extensions.multiDraw===!0||Rt)&&t.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:t.has("KHR_parallel_shader_compile"),customProgramCacheKey:y.customProgramCacheKey()};return Be.vertexUv1s=c.has(1),Be.vertexUv2s=c.has(2),Be.vertexUv3s=c.has(3),c.clear(),Be}function M(y){let S=[];if(y.shaderID?S.push(y.shaderID):(S.push(y.customVertexShaderID),S.push(y.customFragmentShaderID)),y.defines!==void 0)for(let N in y.defines)S.push(N),S.push(y.defines[N]);return y.isRawShaderMaterial===!1&&(m(S,y),p(S,y),S.push(n.outputColorSpace)),S.push(y.customProgramCacheKey),S.join()}function m(y,S){y.push(S.precision),y.push(S.outputColorSpace),y.push(S.envMapMode),y.push(S.envMapPMREM),y.push(S.envMapMaxLod),y.push(S.envMapSize),y.push(S.mapUv),y.push(S.alphaMapUv),y.push(S.lightMapUv),y.push(S.aoMapUv),y.push(S.bumpMapUv),y.push(S.normalMapUv),y.push(S.displacementMapUv),y.push(S.emissiveMapUv),y.push(S.metalnessMapUv),y.push(S.roughnessMapUv),y.push(S.anisotropyMapUv),y.push(S.clearcoatMapUv),y.push(S.clearcoatNormalMapUv),y.push(S.clearcoatRoughnessMapUv),y.push(S.diffuseRoughnessMapUv),y.push(S.iridescenceMapUv),y.push(S.iridescenceThicknessMapUv),y.push(S.sheenColorMapUv),y.push(S.sheenRoughnessMapUv),y.push(S.specularMapUv),y.push(S.specularColorMapUv),y.push(S.specularIntensityMapUv),y.push(S.transmissionMapUv),y.push(S.thicknessMapUv),y.push(S.combine),y.push(S.fogExp2),y.push(S.sizeAttenuation),y.push(S.morphTargetsCount),y.push(S.numSunLights),y.push(S.numDirLights),y.push(S.numPointLights),y.push(S.numSpotLights),y.push(S.numSpotLightMaps),y.push(S.numHemiLights),y.push(S.numRectAreaLights),y.push(S.numSunLightShadows),y.push(S.numDirLightShadows),y.push(S.numPointLightShadows),y.push(S.numSpotLightShadows),y.push(S.numSpotLightShadowsWithMaps),y.push(S.numLightProbes),y.push(S.numLightProbeGrids),y.push(S.shadowMapType),y.push(S.toneMapping),y.push(S.numClippingPlanes),y.push(S.numClipIntersection),y.push(S.depthPacking)}function p(y,S){o.disableAll(),S.instancing&&o.enable(0),S.instancingColor&&o.enable(1),S.instancingMorph&&o.enable(2),S.matcap&&o.enable(3),S.envMap&&o.enable(4),S.normalMapObjectSpace&&o.enable(5),S.normalMapTangentSpace&&o.enable(6),S.clearcoat&&o.enable(7),S.iridescence&&o.enable(8),S.alphaTest&&o.enable(9),S.vertexColors&&o.enable(10),S.vertexAlphas&&o.enable(11),S.vertexUv1s&&o.enable(12),S.vertexUv2s&&o.enable(13),S.vertexUv3s&&o.enable(14),S.vertexTangents&&o.enable(15),S.anisotropy&&o.enable(16),S.alphaHash&&o.enable(17),S.batching&&o.enable(18),S.dispersion&&o.enable(19),S.retroreflection&&o.enable(24),S.diffuseRoughness&&o.enable(25),S.batchingColor&&o.enable(20),S.gradientMap&&o.enable(21),S.packedNormalMap&&o.enable(22),S.vertexNormals&&o.enable(23),y.push(o.mask),o.disableAll(),S.fog&&o.enable(0),S.useFog&&o.enable(1),S.flatShading&&o.enable(2),S.logarithmicDepthBuffer&&o.enable(3),S.reversedDepthBuffer&&o.enable(4),S.skinning&&o.enable(5),S.morphTargets&&o.enable(6),S.morphNormals&&o.enable(7),S.morphColors&&o.enable(8),S.premultipliedAlpha&&o.enable(9),S.shadowMapEnabled&&o.enable(10),S.doubleSided&&o.enable(11),S.flipSided&&o.enable(12),S.useDepthPacking&&o.enable(13),S.dithering&&o.enable(14),S.transmission&&o.enable(15),S.sheen&&o.enable(16),S.opaque&&o.enable(17),S.pointsUvs&&o.enable(18),S.decodeVideoTexture&&o.enable(19),S.decodeVideoTextureEmissive&&o.enable(20),S.alphaToCoverage&&o.enable(21),S.numLightProbeGrids>0&&o.enable(22),S.hasPositionAttribute&&o.enable(23),y.push(o.mask)}function x(y){let S=f[y.type],N;if(S){let A=Li[S];N=nM.clone(A.uniforms)}else N=y.uniforms;return N}function w(y,S){let N=u.get(S);return N!==void 0?++N.usedTimes:(N=new N1(n,S,y,r),l.push(N),u.set(S,N)),N}function E(y){if(--y.usedTimes===0){let S=l.indexOf(y);l[S]=l[l.length-1],l.pop(),u.delete(y.cacheKey),y.destroy()}}function I(y){a.remove(y)}function T(){a.dispose()}return{getParameters:g,getProgramCacheKey:M,getUniforms:x,acquireProgram:w,releaseProgram:E,releaseShaderCache:I,programs:l,dispose:T}}function F1(){let n=new WeakMap;function e(o){return n.has(o)}function t(o){let a=n.get(o);return a===void 0&&(a={},n.set(o,a)),a}function i(o){n.delete(o)}function r(o,a,c){n.get(o)[a]=c}function s(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:s}}function k1(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.materialVariant!==e.materialVariant?n.materialVariant-e.materialVariant:n.z!==e.z?n.z-e.z:n.id-e.id}function SM(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function EM(){let n=[],e=0,t=[],i=[],r=[];function s(){e=0,t.length=0,i.length=0,r.length=0}function o(f){let h=0;return f.isInstancedMesh&&(h+=2),f.isSkinnedMesh&&(h+=1),h}function a(f,h,g,M,m,p){let x=n[e];return x===void 0?(x={id:f.id,object:f,geometry:h,material:g,materialVariant:o(f),groupOrder:M,renderOrder:f.renderOrder,z:m,group:p},n[e]=x):(x.id=f.id,x.object=f,x.geometry=h,x.material=g,x.materialVariant=o(f),x.groupOrder=M,x.renderOrder=f.renderOrder,x.z=m,x.group=p),e++,x}function c(f,h,g,M,m,p,x){x.reversedDepth===!0&&(m=-m);let w=a(f,h,g,M,m,p);g.transmission>0?i.push(w):g.transparent===!0?r.push(w):t.push(w)}function l(f,h,g,M,m,p){let x=a(f,h,g,M,m,p);g.transmission>0?i.unshift(x):g.transparent===!0?r.unshift(x):t.unshift(x)}function u(f,h){t.length>1&&t.sort(f||k1),i.length>1&&i.sort(h||SM),r.length>1&&r.sort(h||SM)}function d(){for(let f=e,h=n.length;f<h;f++){let g=n[f];if(g.id===null)break;g.id=null,g.object=null,g.geometry=null,g.material=null,g.group=null}}return{opaque:t,transmissive:i,transparent:r,init:s,push:c,unshift:l,finish:d,sort:u}}function U1(){let n=new WeakMap;function e(i,r){let s=n.get(i),o;return s===void 0?(o=new EM,n.set(i,[o])):r>=s.length?(o=new EM,s.push(o)):o=s[r],o}function t(){n=new WeakMap}return{get:e,dispose:t}}function B1(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"SunLight":case"DirectionalLight":t={direction:new O,color:new He};break;case"SpotLight":t={position:new O,direction:new O,color:new He,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new O,color:new He,distance:0,decay:0};break;case"HemisphereLight":t={direction:new O,skyColor:new He,groundColor:new He};break;case"RectAreaLight":t={color:new He,position:new O,halfWidth:new O,halfHeight:new O};break}return n[e.id]=t,t}}}function V1(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"SunLight":case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new Te};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new Te};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new Te,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var H1=0;function z1(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function G1(n){let e=new B1,t=V1(),i={version:0,hash:{sunLength:-1,directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numSunShadows:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],sun:[],sunShadow:[],sunShadowMap:[],sunShadowMatrix:[],sunShadowCascade:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let l=0;l<9;l++)i.probe.push(new O);let r=new O,s=new _t,o=new _t;function a(l){let u=0,d=0,f=0;for(let F=0;F<9;F++)i.probe[F].set(0,0,0);let h=0,g=0,M=0,m=0,p=0,x=0,w=0,E=0,I=0,T=0,y=0,S=0,N=0,A=0;l.sort(z1);for(let F=0,G=l.length;F<G;F++){let D=l[F],z=D.color,$=D.intensity,H=D.distance,te=null;if(D.shadow&&D.shadow.map&&(D.shadow.map.texture.format===Lr?te=D.shadow.map.texture:te=D.shadow.map.depthTexture||D.shadow.map.texture),D.isAmbientLight)u+=z.r*$,d+=z.g*$,f+=z.b*$;else if(D.isLightProbe){for(let j=0;j<9;j++)i.probe[j].addScaledVector(D.sh.coefficients[j],$);A++}else if(D.isSunLight){let j=e.get(D);if(j.color.copy(D.color).multiplyScalar(D.intensity),D.castShadow){let J=D.shadow,Q=t.get(D);Q.shadowIntensity=J.intensity,Q.shadowBias=J.bias,Q.shadowNormalBias=J.normalBias,Q.shadowRadius=J.radius,Q.shadowMapSize.copy(J.mapSize).multiply(J.getFrameExtents()),i.sunShadow[g]=Q,i.sunShadowMap[g]=te;let Me=J.getViewportCount();for(let Fe=0;Fe<Me;Fe++)i.sunShadowMatrix[M+Fe]=J.getMatrix(Fe),i.sunShadowCascade[M+Fe]=J._cascadeData[Fe];M+=Me,g++}i.sun[h]=j,h++}else if(D.isDirectionalLight){let j=e.get(D);if(j.color.copy(D.color).multiplyScalar(D.intensity),D.castShadow){let J=D.shadow,Q=t.get(D);Q.shadowIntensity=J.intensity,Q.shadowBias=J.bias,Q.shadowNormalBias=J.normalBias,Q.shadowRadius=J.radius,Q.shadowMapSize=J.mapSize,i.directionalShadow[m]=Q,i.directionalShadowMap[m]=te,i.directionalShadowMatrix[m]=D.shadow.matrix,I++}i.directional[m]=j,m++}else if(D.isSpotLight){let j=e.get(D);j.position.setFromMatrixPosition(D.matrixWorld),j.color.copy(z).multiplyScalar($),j.distance=H,j.coneCos=Math.cos(D.angle),j.penumbraCos=Math.cos(D.angle*(1-D.penumbra)),j.decay=D.decay,i.spot[x]=j;let J=D.shadow;if(D.map&&(i.spotLightMap[S]=D.map,S++,J.updateMatrices(D),D.castShadow&&N++),i.spotLightMatrix[x]=J.matrix,D.castShadow){let Q=t.get(D);Q.shadowIntensity=J.intensity,Q.shadowBias=J.bias,Q.shadowNormalBias=J.normalBias,Q.shadowRadius=J.radius,Q.shadowMapSize=J.mapSize,i.spotShadow[x]=Q,i.spotShadowMap[x]=te,y++}x++}else if(D.isRectAreaLight){let j=e.get(D);j.color.copy(z).multiplyScalar($),j.halfWidth.set(D.width*.5,0,0),j.halfHeight.set(0,D.height*.5,0),i.rectArea[w]=j,w++}else if(D.isPointLight){let j=e.get(D);if(j.color.copy(D.color).multiplyScalar(D.intensity),j.distance=D.distance,j.decay=D.decay,D.castShadow){let J=D.shadow,Q=t.get(D);Q.shadowIntensity=J.intensity,Q.shadowBias=J.bias,Q.shadowNormalBias=J.normalBias,Q.shadowRadius=J.radius,Q.shadowMapSize=J.mapSize,Q.shadowCameraNear=J.camera.near,Q.shadowCameraFar=J.camera.far,i.pointShadow[p]=Q,i.pointShadowMap[p]=te,i.pointShadowMatrix[p]=D.shadow.matrix,T++}i.point[p]=j,p++}else if(D.isHemisphereLight){let j=e.get(D);j.skyColor.copy(D.color).multiplyScalar($),j.groundColor.copy(D.groundColor).multiplyScalar($),i.hemi[E]=j,E++}}w>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=he.LTC_FLOAT_1,i.rectAreaLTC2=he.LTC_FLOAT_2):(i.rectAreaLTC1=he.LTC_HALF_1,i.rectAreaLTC2=he.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=d,i.ambient[2]=f;let L=i.hash;(L.sunLength!==h||L.directionalLength!==m||L.pointLength!==p||L.spotLength!==x||L.rectAreaLength!==w||L.hemiLength!==E||L.numSunShadows!==g||L.numDirectionalShadows!==I||L.numPointShadows!==T||L.numSpotShadows!==y||L.numSpotMaps!==S||L.numLightProbes!==A)&&(i.sun.length=h,i.directional.length=m,i.spot.length=x,i.rectArea.length=w,i.point.length=p,i.hemi.length=E,i.sunShadow.length=g,i.sunShadowMap.length=g,i.sunShadowMatrix.length=M,i.sunShadowCascade.length=M,i.directionalShadow.length=I,i.directionalShadowMap.length=I,i.directionalShadowMatrix.length=I,i.pointShadow.length=T,i.pointShadowMap.length=T,i.pointShadowMatrix.length=T,i.spotShadow.length=y,i.spotShadowMap.length=y,i.spotLightMatrix.length=y+S-N,i.spotLightMap.length=S,i.numSpotLightShadowsWithMaps=N,i.numLightProbes=A,L.sunLength=h,L.directionalLength=m,L.pointLength=p,L.spotLength=x,L.rectAreaLength=w,L.hemiLength=E,L.numSunShadows=g,L.numDirectionalShadows=I,L.numPointShadows=T,L.numSpotShadows=y,L.numSpotMaps=S,L.numLightProbes=A,i.version=H1++)}function c(l,u){let d=0,f=0,h=0,g=0,M=0,m=0,p=u.matrixWorldInverse;for(let x=0,w=l.length;x<w;x++){let E=l[x];if(E.isSunLight){let I=i.sun[d];I.direction.setFromMatrixPosition(E.matrixWorld),I.direction.transformDirection(p),d++}else if(E.isDirectionalLight){let I=i.directional[f];I.direction.setFromMatrixPosition(E.matrixWorld),r.setFromMatrixPosition(E.target.matrixWorld),I.direction.sub(r),I.direction.transformDirection(p),f++}else if(E.isSpotLight){let I=i.spot[g];I.position.setFromMatrixPosition(E.matrixWorld),I.position.applyMatrix4(p),I.direction.setFromMatrixPosition(E.matrixWorld),r.setFromMatrixPosition(E.target.matrixWorld),I.direction.sub(r),I.direction.transformDirection(p),g++}else if(E.isRectAreaLight){let I=i.rectArea[M];I.position.setFromMatrixPosition(E.matrixWorld),I.position.applyMatrix4(p),o.identity(),s.copy(E.matrixWorld),s.premultiply(p),o.extractRotation(s),I.halfWidth.set(E.width*.5,0,0),I.halfHeight.set(0,E.height*.5,0),I.halfWidth.applyMatrix4(o),I.halfHeight.applyMatrix4(o),M++}else if(E.isPointLight){let I=i.point[h];I.position.setFromMatrixPosition(E.matrixWorld),I.position.applyMatrix4(p),h++}else if(E.isHemisphereLight){let I=i.hemi[m];I.direction.setFromMatrixPosition(E.matrixWorld),I.direction.transformDirection(p),m++}}}return{setup:a,setupView:c,state:i}}function bM(n){let e=new G1(n),t=[],i=[],r=[];function s(d){u.camera=d,t.length=0,i.length=0,r.length=0}function o(d){d.isLightProbeGrid?r.push(d):t.push(d)}function a(d){i.push(d)}function c(){e.setup(t),r.sort((d,f)=>d.priority-f.priority||d.id-f.id)}function l(d){e.setupView(t,d)}let u={lightsArray:t,shadowsArray:i,lightProbeGridArray:r,camera:null,lights:e,transmissionRenderTarget:{},textureUnits:0};return{init:s,state:u,setupLights:c,setupLightsView:l,pushLight:o,pushShadow:a}}function W1(n){let e=new WeakMap;function t(r,s=0){let o=e.get(r),a;return o===void 0?(a=new bM(n),e.set(r,[a])):s>=o.length?(a=new bM(n),o.push(a)):a=o[s],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var j1=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,$1=`uniform sampler2D shadow_pass;
uniform vec2 resolution;
uniform float radius;
void main() {
	const float samples = float( VSM_SAMPLES );
	float mean = 0.0;
	float squared_mean = 0.0;
	float uvStride = samples <= 1.0 ? 0.0 : 2.0 / ( samples - 1.0 );
	float uvStart = samples <= 1.0 ? 0.0 : - 1.0;
	for ( float i = 0.0; i < samples; i ++ ) {
		float uvOffset = uvStart + i * uvStride;
		#ifdef HORIZONTAL_PASS
			vec2 distribution = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( uvOffset, 0.0 ) * radius ) / resolution ).rg;
			mean += distribution.x;
			squared_mean += distribution.y * distribution.y + distribution.x * distribution.x;
		#else
			float depth = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( 0.0, uvOffset ) * radius ) / resolution ).r;
			mean += depth;
			squared_mean += depth * depth;
		#endif
	}
	mean = mean / samples;
	squared_mean = squared_mean / samples;
	float std_dev = sqrt( max( 0.0, squared_mean - mean * mean ) );
	gl_FragColor = vec4( mean, std_dev, 0.0, 1.0 );
}`,q1=[new O(1,0,0),new O(-1,0,0),new O(0,1,0),new O(0,-1,0),new O(0,0,1),new O(0,0,-1)],X1=[new O(0,-1,0),new O(0,-1,0),new O(0,0,1),new O(0,0,-1),new O(0,-1,0),new O(0,-1,0)],TM=new _t,gc=new O,cg=new O;function Y1(n,e,t){let i=new Cr,r=new Te,s=new Te,o=new Et,a=new $u,c=new qu,l={},u=t.maxTextureSize,d={[Pr]:gn,[gn]:Pr,[Ri]:Ri},f=new Ai({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new Te},radius:{value:4}},vertexShader:j1,fragmentShader:$1}),h=f.clone();h.defines.HORIZONTAL_PASS=1;let g=new an;g.setAttribute("position",new Ii(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let M=new en(g,f),m=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=cc;let p=this.type;this.render=function(T,y,S){if(m.enabled===!1||m.autoUpdate===!1&&m.needsUpdate===!1||T.length===0)return;this.type===od&&(Ie("WebGLShadowMap: PCFSoftShadowMap has been removed. Using PCFShadowMap instead."),this.type=cc);let N=n.getRenderTarget(),A=n.getActiveCubeFace(),L=n.getActiveMipmapLevel(),F=n.state;F.setBlending(vs),F.buffers.depth.getReversed()===!0?F.buffers.color.setClear(0,0,0,0):F.buffers.color.setClear(1,1,1,1),F.buffers.depth.setTest(!0),F.setScissorTest(!1);let G=p!==this.type;G&&y.traverse(function(D){D.material&&(Array.isArray(D.material)?D.material.forEach(z=>z.needsUpdate=!0):D.material.needsUpdate=!0)});for(let D=0,z=T.length;D<z;D++){let $=T[D],H=$.shadow;if(H===void 0){Ie("WebGLShadowMap:",$,"has no shadow.");continue}if(H.autoUpdate===!1&&H.needsUpdate===!1)continue;r.copy(H.mapSize);let te=H.getFrameExtents();r.multiply(te),s.copy(H.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(s.x=Math.floor(u/te.x),r.x=s.x*te.x,H.mapSize.x=s.x),r.y>u&&(s.y=Math.floor(u/te.y),r.y=s.y*te.y,H.mapSize.y=s.y));let j=n.state.buffers.depth.getReversed();if(H.camera._reversedDepth=j,H.map===null||G===!0){if(H.map!==null&&(H.map.depthTexture!==null&&(H.map.depthTexture.dispose(),H.map.depthTexture=null),H.map.dispose()),this.type===Do){if($.isPointLight){Ie("WebGLShadowMap: VSM shadow maps are not supported for PointLights. Use PCF or BasicShadowMap instead.");continue}H.map=new On(r.x,r.y,{format:Lr,type:fi,minFilter:mn,magFilter:mn,generateMipmaps:!1}),H.map.texture.name=$.name+".shadowMap",H.map.depthTexture=new tr(r.x,r.y,di),H.map.depthTexture.name=$.name+".shadowMapDepth",H.map.depthTexture.format=Ji,H.map.depthTexture.compareFunction=null,H.map.depthTexture.minFilter=Xt,H.map.depthTexture.magFilter=Xt}else $.isPointLight?(H.map=new vc(r.x),H.map.depthTexture=new $a(r.x,ui)):(H.map=new On(r.x,r.y),H.map.depthTexture=new tr(r.x,r.y,ui)),H.map.depthTexture.name=$.name+".shadowMap",H.map.depthTexture.format=Ji,this.type===cc?(H.map.depthTexture.compareFunction=j?Ym:Xm,H.map.depthTexture.minFilter=mn,H.map.depthTexture.magFilter=mn):(H.map.depthTexture.compareFunction=null,H.map.depthTexture.minFilter=Xt,H.map.depthTexture.magFilter=Xt);H.camera.updateProjectionMatrix()}H.map.isWebGLCubeRenderTarget!==!0&&(H.map.width!==r.x||H.map.height!==r.y)&&H.map.setSize(r.x,r.y);let J=H.map.isWebGLCubeRenderTarget?6:H.getViewportCount();$.isPointLight!==!0&&H.updateMatrices($,S);let Q=(H.camera.layers.mask&4294967294)!==0?H.camera.layers:S.layers;for(let Me=0;Me<J;Me++){let Fe=H.getCamera(Me);if($.isPointLight){let Y=H.camera,pe=H.matrix,ve=$.distance||Y.far;ve!==Y.far&&(Y.far=ve,Y.updateProjectionMatrix()),gc.setFromMatrixPosition($.matrixWorld),Y.position.copy(gc),cg.copy(Y.position),cg.add(q1[Me]),Y.up.copy(X1[Me]),Y.lookAt(cg),Y.updateMatrixWorld(),pe.makeTranslation(-gc.x,-gc.y,-gc.z),TM.multiplyMatrices(Y.projectionMatrix,Y.matrixWorldInverse),H._frustum.setFromProjectionMatrix(TM,Y.coordinateSystem,Y.reversedDepth)}if(H.map.isWebGLCubeRenderTarget)n.setRenderTarget(H.map,Me),n.clear();else{Me===0&&(n.setRenderTarget(H.map),n.clear());let Y=H.getViewport(Me);o.set(s.x*Y.x,s.y*Y.y,s.x*Y.z,s.y*Y.w),F.viewport(o)}i=H.getFrustum(Me),E(y,S,Fe,$,Q,this.type)}H.isPointLightShadow!==!0&&this.type===Do&&x(H,S),H.needsUpdate=!1}p=this.type,m.needsUpdate=!1,n.setRenderTarget(N,A,L)};function x(T,y){let S=e.update(M);f.defines.VSM_SAMPLES!==T.blurSamples&&(f.defines.VSM_SAMPLES=T.blurSamples,h.defines.VSM_SAMPLES=T.blurSamples,f.needsUpdate=!0,h.needsUpdate=!0),T.mapPass===null?T.mapPass=new On(r.x,r.y,{format:Lr,type:fi}):(T.mapPass.width!==T.map.width||T.mapPass.height!==T.map.height)&&T.mapPass.setSize(T.map.width,T.map.height),f.uniforms.shadow_pass.value=T.map.depthTexture,f.uniforms.resolution.value.set(T.map.width,T.map.height),f.uniforms.radius.value=T.radius,n.setRenderTarget(T.mapPass),n.clear(),n.renderBufferDirect(y,null,S,f,M,null),h.uniforms.shadow_pass.value=T.mapPass.texture,h.uniforms.resolution.value.set(T.map.width,T.map.height),h.uniforms.radius.value=T.radius,n.setRenderTarget(T.map),n.clear(),n.renderBufferDirect(y,null,S,h,M,null)}function w(T,y,S,N){let A=null,L=S.isPointLight===!0?T.customDistanceMaterial:T.customDepthMaterial;if(L!==void 0)A=L;else if(A=S.isPointLight===!0?c:a,n.localClippingEnabled&&y.clipShadows===!0&&Array.isArray(y.clippingPlanes)&&y.clippingPlanes.length!==0||y.displacementMap&&y.displacementScale!==0||y.alphaMap&&y.alphaTest>0||y.map&&y.alphaTest>0||y.alphaToCoverage===!0){let F=A.uuid,G=y.uuid,D=l[F];D===void 0&&(D={},l[F]=D);let z=D[G];z===void 0&&(z=A.clone(),D[G]=z,y.addEventListener("dispose",I)),A=z}if(A.visible=y.visible,A.wireframe=y.wireframe,N===Do?A.side=y.shadowSide!==null?y.shadowSide:y.side:A.side=y.shadowSide!==null?y.shadowSide:d[y.side],A.alphaMap=y.alphaMap,A.alphaTest=y.alphaToCoverage===!0?.5:y.alphaTest,A.map=y.map,A.clipShadows=y.clipShadows,A.clippingPlanes=y.clippingPlanes,A.clipIntersection=y.clipIntersection,A.displacementMap=y.displacementMap,A.displacementScale=y.displacementScale,A.displacementBias=y.displacementBias,A.wireframeLinewidth=y.wireframeLinewidth,A.linewidth=y.linewidth,S.isPointLight===!0&&A.isMeshDistanceMaterial===!0){let F=n.properties.get(A);F.light=S}return A}function E(T,y,S,N,A,L){if(T.visible===!1)return;if(T.layers.test(A)&&(T.isMesh||T.isLine||T.isPoints)&&(T.castShadow||T.receiveShadow&&L===Do)&&(!T.frustumCulled||T.intersectsFrustum(i))){T.modelViewMatrix.multiplyMatrices(S.matrixWorldInverse,T.matrixWorld);let D=e.update(T),z=T.material;if(Array.isArray(z)){let $=D.groups;for(let H=0,te=$.length;H<te;H++){let j=$[H],J=z[j.materialIndex];if(J&&J.visible){let Q=w(T,J,N,L);T.onBeforeShadow(n,T,y,S,D,Q,j),n.renderBufferDirect(S,null,D,Q,T,j),T.onAfterShadow(n,T,y,S,D,Q,j)}}}else if(z.visible){let $=w(T,z,N,L);T.onBeforeShadow(n,T,y,S,D,$,null),n.renderBufferDirect(S,null,D,$,T,null),T.onAfterShadow(n,T,y,S,D,$,null)}}let G=T.children;for(let D=0,z=G.length;D<z;D++)E(G[D],y,S,N,A,L)}function I(T){T.target.removeEventListener("dispose",I);for(let S in l){let N=l[S],A=T.target.uuid;A in N&&(N[A].dispose(),delete N[A])}}}function Z1(n,e){function t(){let C=!1,Z=new Et,W=null,ae=new Et(0,0,0,0);return{setMask:function(ie){W!==ie&&!C&&(n.colorMask(ie,ie,ie,ie),W=ie)},setLocked:function(ie){C=ie},setClear:function(ie,se,Ee,ze,ne){ne===!0&&(ie*=ze,se*=ze,Ee*=ze),Z.set(ie,se,Ee,ze),ae.equals(Z)===!1&&(n.clearColor(ie,se,Ee,ze),ae.copy(Z))},reset:function(){C=!1,W=null,ae.set(-1,0,0,0)}}}function i(){let C=!1,Z=!1,W=null,ae=null,ie=null;return{setReversed:function(se){if(Z!==se){let Ee=e.get("EXT_clip_control");se?Ee.clipControlEXT(Ee.LOWER_LEFT_EXT,Ee.ZERO_TO_ONE_EXT):Ee.clipControlEXT(Ee.LOWER_LEFT_EXT,Ee.NEGATIVE_ONE_TO_ONE_EXT),Z=se;let ze=ie;ie=null,this.setClear(ze)}},getReversed:function(){return Z},setTest:function(se){se?me(n.DEPTH_TEST):Ue(n.DEPTH_TEST)},setMask:function(se){W!==se&&!C&&(n.depthMask(se),W=se)},setFunc:function(se){if(Z&&(se=Jx[se]),ae!==se){switch(se){case Iu:n.depthFunc(n.NEVER);break;case Cu:n.depthFunc(n.ALWAYS);break;case Au:n.depthFunc(n.LESS);break;case xo:n.depthFunc(n.LEQUAL);break;case Du:n.depthFunc(n.EQUAL);break;case Ru:n.depthFunc(n.GEQUAL);break;case Nu:n.depthFunc(n.GREATER);break;case Pu:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}ae=se}},setLocked:function(se){C=se},setClear:function(se){ie!==se&&(ie=se,Z&&(se=1-se),n.clearDepth(se))},reset:function(){C=!1,W=null,ae=null,ie=null,Z=!1}}}function r(){let C=!1,Z=null,W=null,ae=null,ie=null,se=null,Ee=null,ze=null,ne=null;return{setTest:function(be){C||(be?me(n.STENCIL_TEST):Ue(n.STENCIL_TEST))},setMask:function(be){Z!==be&&!C&&(n.stencilMask(be),Z=be)},setFunc:function(be,Be,fe){(W!==be||ae!==Be||ie!==fe)&&(n.stencilFunc(be,Be,fe),W=be,ae=Be,ie=fe)},setOp:function(be,Be,fe){(se!==be||Ee!==Be||ze!==fe)&&(n.stencilOp(be,Be,fe),se=be,Ee=Be,ze=fe)},setLocked:function(be){C=be},setClear:function(be){ne!==be&&(n.clearStencil(be),ne=be)},reset:function(){C=!1,Z=null,W=null,ae=null,ie=null,se=null,Ee=null,ze=null,ne=null}}}let s=new t,o=new i,a=new r,c={},l=new WeakMap,u={},d={},f={},h=new WeakMap,g=[],M=null,m=!1,p=null,x=null,w=null,E=null,I=null,T=null,y=null,S=new He(0,0,0),N=0,A=!1,L=null,F=null,G=null,D=null,z=null,$=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),H=!1,te=0,j=n.getParameter(n.VERSION);j.indexOf("WebGL")!==-1?(te=parseFloat(/^WebGL (\d)/.exec(j)[1]),H=te>=1):j.indexOf("OpenGL ES")!==-1&&(te=parseFloat(/^OpenGL ES (\d)/.exec(j)[1]),H=te>=2);let J=null,Q={},Me=n.getParameter(n.SCISSOR_BOX),Fe=n.getParameter(n.VIEWPORT),Y=new Et().fromArray(Me),pe=new Et().fromArray(Fe);function ve(C,Z,W,ae){let ie=new Uint8Array(4),se=n.createTexture();n.bindTexture(C,se),n.texParameteri(C,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(C,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let Ee=0;Ee<W;Ee++)C===n.TEXTURE_3D||C===n.TEXTURE_2D_ARRAY?n.texImage3D(Z,0,n.RGBA,1,1,ae,0,n.RGBA,n.UNSIGNED_BYTE,ie):n.texImage2D(Z+Ee,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,ie);return se}let Ce={};Ce[n.TEXTURE_2D]=ve(n.TEXTURE_2D,n.TEXTURE_2D,1),Ce[n.TEXTURE_CUBE_MAP]=ve(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),Ce[n.TEXTURE_2D_ARRAY]=ve(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),Ce[n.TEXTURE_3D]=ve(n.TEXTURE_3D,n.TEXTURE_3D,1,1),s.setClear(0,0,0,1),o.setClear(1),a.setClear(0),me(n.DEPTH_TEST),o.setFunc(xo),vn(!1),bt(wm),me(n.CULL_FACE),Yt(vs);function me(C){u[C]!==!0&&(n.enable(C),u[C]=!0)}function Ue(C){u[C]!==!1&&(n.disable(C),u[C]=!1)}function Rt(C,Z){return f[C]!==Z?(n.bindFramebuffer(C,Z),f[C]=Z,C===n.DRAW_FRAMEBUFFER&&(f[n.FRAMEBUFFER]=Z),C===n.FRAMEBUFFER&&(f[n.DRAW_FRAMEBUFFER]=Z),!0):!1}function Re(C,Z){let W=g,ae=!1;if(C){W=h.get(Z),W===void 0&&(W=[],h.set(Z,W));let ie=C.textures;if(W.length!==ie.length||W[0]!==n.COLOR_ATTACHMENT0){for(let se=0,Ee=ie.length;se<Ee;se++)W[se]=n.COLOR_ATTACHMENT0+se;W.length=ie.length,ae=!0}}else W[0]!==n.BACK&&(W[0]=n.BACK,ae=!0);ae&&n.drawBuffers(W)}function at(C){return M!==C?(n.useProgram(C),M=C,!0):!1}let pt={[_s]:n.FUNC_ADD,[xx]:n.FUNC_SUBTRACT,[Mx]:n.FUNC_REVERSE_SUBTRACT};pt[Sx]=n.MIN,pt[Ex]=n.MAX;let qe={[bx]:n.ZERO,[Tx]:n.ONE,[wx]:n.SRC_COLOR,[Dm]:n.SRC_ALPHA,[Nx]:n.SRC_ALPHA_SATURATE,[Dx]:n.DST_COLOR,[Cx]:n.DST_ALPHA,[Ix]:n.ONE_MINUS_SRC_COLOR,[Rm]:n.ONE_MINUS_SRC_ALPHA,[Rx]:n.ONE_MINUS_DST_COLOR,[Ax]:n.ONE_MINUS_DST_ALPHA,[Px]:n.CONSTANT_COLOR,[Lx]:n.ONE_MINUS_CONSTANT_COLOR,[Ox]:n.CONSTANT_ALPHA,[Fx]:n.ONE_MINUS_CONSTANT_ALPHA};function Yt(C,Z,W,ae,ie,se,Ee,ze,ne,be){if(C===vs){m===!0&&(Ue(n.BLEND),m=!1);return}if(m===!1&&(me(n.BLEND),m=!0),C!==_x){if(C!==p||be!==A){if((x!==_s||I!==_s)&&(n.blendEquation(n.FUNC_ADD),x=_s,I=_s),be)switch(C){case Ro:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Im:n.blendFunc(n.ONE,n.ONE);break;case Cm:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case Am:n.blendFuncSeparate(n.DST_COLOR,n.ONE_MINUS_SRC_ALPHA,n.ZERO,n.ONE);break;default:Ne("WebGLState: Invalid blending: ",C);break}else switch(C){case Ro:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Im:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE,n.ONE,n.ONE);break;case Cm:Ne("WebGLState: SubtractiveBlending requires material.premultipliedAlpha = true");break;case Am:Ne("WebGLState: MultiplyBlending requires material.premultipliedAlpha = true");break;default:Ne("WebGLState: Invalid blending: ",C);break}w=null,E=null,T=null,y=null,S.set(0,0,0),N=0,p=C,A=be}return}ie=ie||Z,se=se||W,Ee=Ee||ae,(Z!==x||ie!==I)&&(n.blendEquationSeparate(pt[Z],pt[ie]),x=Z,I=ie),(W!==w||ae!==E||se!==T||Ee!==y)&&(n.blendFuncSeparate(qe[W],qe[ae],qe[se],qe[Ee]),w=W,E=ae,T=se,y=Ee),(ze.equals(S)===!1||ne!==N)&&(n.blendColor(ze.r,ze.g,ze.b,ne),S.copy(ze),N=ne),p=C,A=!1}function kt(C,Z){C.side===Ri?Ue(n.CULL_FACE):me(n.CULL_FACE);let W=C.side===gn;Z&&(W=!W),vn(W),C.blending===Ro&&C.transparent===!1?Yt(vs):Yt(C.blending,C.blendEquation,C.blendSrc,C.blendDst,C.blendEquationAlpha,C.blendSrcAlpha,C.blendDstAlpha,C.blendColor,C.blendAlpha,C.premultipliedAlpha),o.setFunc(C.depthFunc),o.setTest(C.depthTest),o.setMask(C.depthWrite),s.setMask(C.colorWrite);let ae=C.stencilWrite;a.setTest(ae),ae&&(a.setMask(C.stencilWriteMask),a.setFunc(C.stencilFunc,C.stencilRef,C.stencilFuncMask),a.setOp(C.stencilFail,C.stencilZFail,C.stencilZPass)),zt(C.polygonOffset,C.polygonOffsetFactor,C.polygonOffsetUnits),C.alphaToCoverage===!0?me(n.SAMPLE_ALPHA_TO_COVERAGE):Ue(n.SAMPLE_ALPHA_TO_COVERAGE)}function vn(C){L!==C&&(C?n.frontFace(n.CW):n.frontFace(n.CCW),L=C)}function bt(C){C!==yx?(me(n.CULL_FACE),C!==F&&(C===wm?n.cullFace(n.BACK):C===vx?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):Ue(n.CULL_FACE),F=C}function pi(C){C!==G&&(H&&n.lineWidth(C),G=C)}function zt(C,Z,W){C?(me(n.POLYGON_OFFSET_FILL),(D!==Z||z!==W)&&(D=Z,z=W,o.getReversed()&&(Z=-Z,W=-W),n.polygonOffset(Z,W))):Ue(n.POLYGON_OFFSET_FILL)}function kn(C){C?me(n.SCISSOR_TEST):Ue(n.SCISSOR_TEST)}function Un(C){C===void 0&&(C=n.TEXTURE0+$-1),J!==C&&(n.activeTexture(C),J=C)}function Bn(C,Z,W){W===void 0&&(J===null?W=n.TEXTURE0+$-1:W=J);let ae=Q[W];ae===void 0&&(ae={type:void 0,texture:void 0},Q[W]=ae),(ae.type!==C||ae.texture!==Z)&&(J!==W&&(n.activeTexture(W),J=W),n.bindTexture(C,Z||Ce[C]),ae.type=C,ae.texture=Z)}function P(){let C=Q[J];C!==void 0&&C.type!==void 0&&(n.bindTexture(C.type,null),C.type=void 0,C.texture=void 0)}function Vn(){try{n.compressedTexImage2D(...arguments)}catch(C){Ne("WebGLState:",C)}}function Mt(){try{n.compressedTexImage3D(...arguments)}catch(C){Ne("WebGLState:",C)}}function St(){try{n.texSubImage2D(...arguments)}catch(C){Ne("WebGLState:",C)}}function de(){try{n.texSubImage3D(...arguments)}catch(C){Ne("WebGLState:",C)}}function Ut(){try{n.compressedTexSubImage2D(...arguments)}catch(C){Ne("WebGLState:",C)}}function Ae(){try{n.compressedTexSubImage3D(...arguments)}catch(C){Ne("WebGLState:",C)}}function Oe(){try{n.texStorage2D(...arguments)}catch(C){Ne("WebGLState:",C)}}function b(){try{n.texStorage3D(...arguments)}catch(C){Ne("WebGLState:",C)}}function v(){try{n.texImage2D(...arguments)}catch(C){Ne("WebGLState:",C)}}function U(){try{n.texImage3D(...arguments)}catch(C){Ne("WebGLState:",C)}}function ee(C){return d[C]!==void 0?d[C]:n.getParameter(C)}function K(C,Z){d[C]!==Z&&(n.pixelStorei(C,Z),d[C]=Z)}function re(C){Y.equals(C)===!1&&(n.scissor(C.x,C.y,C.z,C.w),Y.copy(C))}function le(C){pe.equals(C)===!1&&(n.viewport(C.x,C.y,C.z,C.w),pe.copy(C))}function q(C,Z){let W=l.get(Z);W===void 0&&(W=new WeakMap,l.set(Z,W));let ae=W.get(C);ae===void 0&&(ae=n.getUniformBlockIndex(Z,C.name),n.uniformBlockBinding(Z,ae,ae),W.set(C,ae))}function X(C,Z,W){let ie=l.get(Z).get(C);c[ie]!==W&&(n.bindBufferBase(n.UNIFORM_BUFFER,ie,W),c[ie]=W)}function oe(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),o.setReversed(!1),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),n.pixelStorei(n.PACK_ALIGNMENT,4),n.pixelStorei(n.UNPACK_ALIGNMENT,4),n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,!1),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,!1),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,n.BROWSER_DEFAULT_WEBGL),n.pixelStorei(n.PACK_ROW_LENGTH,0),n.pixelStorei(n.PACK_SKIP_PIXELS,0),n.pixelStorei(n.PACK_SKIP_ROWS,0),n.pixelStorei(n.UNPACK_ROW_LENGTH,0),n.pixelStorei(n.UNPACK_IMAGE_HEIGHT,0),n.pixelStorei(n.UNPACK_SKIP_PIXELS,0),n.pixelStorei(n.UNPACK_SKIP_ROWS,0),n.pixelStorei(n.UNPACK_SKIP_IMAGES,0),u={},d={},J=null,Q={},f={},c={},h=new WeakMap,g=[],M=null,m=!1,p=null,x=null,w=null,E=null,I=null,T=null,y=null,S=new He(0,0,0),N=0,A=!1,L=null,F=null,G=null,D=null,z=null,Y.set(0,0,n.canvas.width,n.canvas.height),pe.set(0,0,n.canvas.width,n.canvas.height),s.reset(),o.reset(),a.reset()}return{buffers:{color:s,depth:o,stencil:a},enable:me,disable:Ue,bindFramebuffer:Rt,drawBuffers:Re,useProgram:at,setBlending:Yt,setMaterial:kt,setFlipSided:vn,setCullFace:bt,setLineWidth:pi,setPolygonOffset:zt,setScissorTest:kn,activeTexture:Un,bindTexture:Bn,unbindTexture:P,compressedTexImage2D:Vn,compressedTexImage3D:Mt,texImage2D:v,texImage3D:U,pixelStorei:K,getParameter:ee,updateUBOMapping:q,uniformBlockBinding:X,texStorage2D:Oe,texStorage3D:b,texSubImage2D:St,texSubImage3D:de,compressedTexSubImage2D:Ut,compressedTexSubImage3D:Ae,scissor:re,viewport:le,reset:oe}}function K1(n,e,t,i,r,s,o){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,c=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),l=new Te,u=new WeakMap,d=new Set,f,h=new WeakMap,g=new Set,M=new FinalizationRegistry(b=>g.delete(b)),m=!1;try{m=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch{}function p(b,v){return m?new OffscreenCanvas(b,v):Ua("canvas")}function x(b,v,U){let ee=1,K=Oe(b);if((K.width>U||K.height>U)&&(ee=U/Math.max(K.width,K.height)),ee<1)if(typeof HTMLImageElement<"u"&&b instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&b instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&b instanceof ImageBitmap||typeof VideoFrame<"u"&&b instanceof VideoFrame){let re=Math.floor(ee*K.width),le=Math.floor(ee*K.height);f===void 0&&(f=p(re,le));let q=v?p(re,le):f;return q.width=re,q.height=le,q.getContext("2d").drawImage(b,0,0,re,le),Ie("WebGLRenderer: Texture has been resized from ("+K.width+"x"+K.height+") to ("+re+"x"+le+")."),q}else return"data"in b&&Ie("WebGLRenderer: Image in DataTexture is too big ("+K.width+"x"+K.height+")."),b;return b}function w(b){return b.generateMipmaps}function E(b){n.generateMipmap(b)}function I(b){return b.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:b.isWebGL3DRenderTarget?n.TEXTURE_3D:b.isWebGLArrayRenderTarget||b.isCompressedArrayTexture?n.TEXTURE_2D_ARRAY:n.TEXTURE_2D}function T(b,v,U,ee,K,re=!1){if(b!==null){if(n[b]!==void 0)return n[b];Ie("WebGLRenderer: Attempt to use non-existing WebGL internal format '"+b+"'")}let le;ee&&(le=e.get("EXT_texture_norm16"),le||Ie("WebGLRenderer: Unable to use normalized textures without EXT_texture_norm16 extension"));let q=v;if(v===n.RED&&(U===n.FLOAT&&(q=n.R32F),U===n.HALF_FLOAT&&(q=n.R16F),U===n.UNSIGNED_BYTE&&(q=n.R8),U===n.UNSIGNED_SHORT&&le&&(q=le.R16_EXT),U===n.SHORT&&le&&(q=le.R16_SNORM_EXT)),v===n.RED_INTEGER&&(U===n.UNSIGNED_BYTE&&(q=n.R8UI),U===n.UNSIGNED_SHORT&&(q=n.R16UI),U===n.UNSIGNED_INT&&(q=n.R32UI),U===n.BYTE&&(q=n.R8I),U===n.SHORT&&(q=n.R16I),U===n.INT&&(q=n.R32I)),v===n.RG&&(U===n.FLOAT&&(q=n.RG32F),U===n.HALF_FLOAT&&(q=n.RG16F),U===n.UNSIGNED_BYTE&&(q=n.RG8),U===n.UNSIGNED_SHORT&&le&&(q=le.RG16_EXT),U===n.SHORT&&le&&(q=le.RG16_SNORM_EXT)),v===n.RG_INTEGER&&(U===n.UNSIGNED_BYTE&&(q=n.RG8UI),U===n.UNSIGNED_SHORT&&(q=n.RG16UI),U===n.UNSIGNED_INT&&(q=n.RG32UI),U===n.BYTE&&(q=n.RG8I),U===n.SHORT&&(q=n.RG16I),U===n.INT&&(q=n.RG32I)),v===n.RGB_INTEGER&&(U===n.UNSIGNED_BYTE&&(q=n.RGB8UI),U===n.UNSIGNED_SHORT&&(q=n.RGB16UI),U===n.UNSIGNED_INT&&(q=n.RGB32UI),U===n.BYTE&&(q=n.RGB8I),U===n.SHORT&&(q=n.RGB16I),U===n.INT&&(q=n.RGB32I)),v===n.RGBA_INTEGER&&(U===n.UNSIGNED_BYTE&&(q=n.RGBA8UI),U===n.UNSIGNED_SHORT&&(q=n.RGBA16UI),U===n.UNSIGNED_INT&&(q=n.RGBA32UI),U===n.BYTE&&(q=n.RGBA8I),U===n.SHORT&&(q=n.RGBA16I),U===n.INT&&(q=n.RGBA32I)),v===n.RGB&&(U===n.UNSIGNED_SHORT&&le&&(q=le.RGB16_EXT),U===n.SHORT&&le&&(q=le.RGB16_SNORM_EXT),U===n.UNSIGNED_INT_5_9_9_9_REV&&(q=n.RGB9_E5),U===n.UNSIGNED_INT_10F_11F_11F_REV&&(q=n.R11F_G11F_B10F)),v===n.RGBA){let X=re?ka:Ke.getTransfer(K);U===n.FLOAT&&(q=n.RGBA32F),U===n.HALF_FLOAT&&(q=n.RGBA16F),U===n.UNSIGNED_BYTE&&(q=X===ot?n.SRGB8_ALPHA8:n.RGBA8),U===n.UNSIGNED_SHORT&&le&&(q=le.RGBA16_EXT),U===n.SHORT&&le&&(q=le.RGBA16_SNORM_EXT),U===n.UNSIGNED_SHORT_4_4_4_4&&(q=n.RGBA4),U===n.UNSIGNED_SHORT_5_5_5_1&&(q=n.RGB5_A1)}return(q===n.R16F||q===n.R32F||q===n.RG16F||q===n.RG32F||q===n.RGBA16F||q===n.RGBA32F)&&e.get("EXT_color_buffer_float"),q}function y(b,v){let U;return b?v===null||v===ui||v===Ms?U=n.DEPTH24_STENCIL8:v===di?U=n.DEPTH32F_STENCIL8:v===Po&&(U=n.DEPTH24_STENCIL8,Ie("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):v===null||v===ui||v===Ms?U=n.DEPTH_COMPONENT24:v===di?U=n.DEPTH_COMPONENT32F:v===Po&&(U=n.DEPTH_COMPONENT16),U}function S(b,v){return w(b)===!0||b.isFramebufferTexture&&b.minFilter!==Xt&&b.minFilter!==mn?Math.log2(Math.max(v.width,v.height))+1:b.mipmaps.length>0?b.mipmaps.length:b.isCompressedTexture&&Array.isArray(b.image)?v.mipmaps.length:1}function N(b){let v=new WeakRef(b);i.get(b).__ref=v,g.add(v),M.register(b,v,v)}function A(b){let v=i.get(b).__ref;v!==void 0&&(g.delete(v),M.unregister(v))}function L(b){G(b.target)}function F(b){D(b.target)}function G(b){b.removeEventListener("dispose",L),A(b),$(b),b.isVideoTexture&&u.delete(b),b.isHTMLTexture&&d.delete(b)}function D(b){b.removeEventListener("dispose",F),A(b),te(b)}function z(){for(let b of g){let v=b.deref();v!==void 0&&(v.isRenderTarget===!0?D(v):G(v))}g.clear(),d.clear()}function $(b){let v=i.get(b);if(v.__webglInit===void 0)return;let U=b.source,ee=h.get(U);if(ee){let K=ee[v.__cacheKey];K.usedTimes--,K.usedTimes===0&&H(b),Object.keys(ee).length===0&&h.delete(U)}i.remove(b)}function H(b){let v=i.get(b);n.deleteTexture(v.__webglTexture);let U=b.source,ee=h.get(U);delete ee[v.__cacheKey],o.memory.textures--}function te(b){let v=i.get(b);v.__depthDisposeCallback&&v.__depthDisposeCallback();let U=b.depthTexture;if(U&&U.renderTarget===b&&G(U),b.isWebGLCubeRenderTarget)for(let K=0;K<6;K++){if(Array.isArray(v.__webglFramebuffer[K]))for(let re=0;re<v.__webglFramebuffer[K].length;re++)n.deleteFramebuffer(v.__webglFramebuffer[K][re]);else n.deleteFramebuffer(v.__webglFramebuffer[K]);v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer[K])}else{if(Array.isArray(v.__webglFramebuffer))for(let K=0;K<v.__webglFramebuffer.length;K++)n.deleteFramebuffer(v.__webglFramebuffer[K]);else n.deleteFramebuffer(v.__webglFramebuffer);if(v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer),v.__webglMultisampledFramebuffer&&n.deleteFramebuffer(v.__webglMultisampledFramebuffer),v.__webglColorRenderbuffer)for(let K=0;K<v.__webglColorRenderbuffer.length;K++)v.__webglColorRenderbuffer[K]&&n.deleteRenderbuffer(v.__webglColorRenderbuffer[K]);v.__webglDepthRenderbuffer&&n.deleteRenderbuffer(v.__webglDepthRenderbuffer)}let ee=b.textures;for(let K=0,re=ee.length;K<re;K++){let le=i.get(ee[K]);le.__webglTexture&&(n.deleteTexture(le.__webglTexture),o.memory.textures--),i.remove(ee[K])}i.remove(b)}let j=0;function J(){j=0}function Q(){return j}function Me(b){j=b}function Fe(){let b=j;return b>=r.maxTextures&&Ie("WebGLTextures: Trying to use "+(b+1)+" texture units while this GPU supports only "+r.maxTextures),j+=1,b}function Y(b){let v=[];return v.push(b.wrapS),v.push(b.wrapT),v.push(b.wrapR||0),v.push(b.magFilter),v.push(b.minFilter),v.push(b.anisotropy),v.push(b.internalFormat),v.push(b.format),v.push(b.type),v.push(b.generateMipmaps),v.push(b.mipmapsAutoUpdate),v.push(b.premultiplyAlpha),v.push(b.flipY),v.push(b.unpackAlignment),v.push(b.colorSpace),v.join()}function pe(b,v){let U=i.get(b);if(b.isVideoTexture&&Ut(b),b.isRenderTargetTexture===!1&&b.isExternalTexture!==!0&&b.version>0&&U.__version!==b.version){let ee=b.image;if(ee===null)Ie("WebGLRenderer: Texture marked for update but no image data found.");else if(ee.complete===!1)Ie("WebGLRenderer: Texture marked for update but image is incomplete");else{kt(U,b,v);return}}else b.isExternalTexture&&(U.__webglTexture=b.sourceTexture?b.sourceTexture:null);t.bindTexture(n.TEXTURE_2D,U.__webglTexture,n.TEXTURE0+v)}function ve(b,v){let U=i.get(b);if(b.isRenderTargetTexture===!1&&b.version>0&&U.__version!==b.version){kt(U,b,v);return}else b.isExternalTexture&&(U.__webglTexture=b.sourceTexture?b.sourceTexture:null);t.bindTexture(n.TEXTURE_2D_ARRAY,U.__webglTexture,n.TEXTURE0+v)}function Ce(b,v){let U=i.get(b);if(b.isRenderTargetTexture===!1&&b.version>0&&U.__version!==b.version){kt(U,b,v);return}t.bindTexture(n.TEXTURE_3D,U.__webglTexture,n.TEXTURE0+v)}function me(b,v){let U=i.get(b);if(b.version>0&&U.__version!==b.version){vn(U,b,v);return}t.bindTexture(n.TEXTURE_CUBE_MAP,U.__webglTexture,n.TEXTURE0+v)}let Ue={[Lu]:n.REPEAT,[wi]:n.CLAMP_TO_EDGE,[Ou]:n.MIRRORED_REPEAT},Rt={[Xt]:n.NEAREST,[Bx]:n.NEAREST_MIPMAP_NEAREST,[lc]:n.NEAREST_MIPMAP_LINEAR,[mn]:n.LINEAR,[ld]:n.LINEAR_MIPMAP_NEAREST,[ir]:n.LINEAR_MIPMAP_LINEAR},Re={[Gx]:n.NEVER,[Zm]:n.ALWAYS,[Wx]:n.LESS,[Xm]:n.LEQUAL,[jx]:n.EQUAL,[Ym]:n.GEQUAL,[$x]:n.GREATER,[qx]:n.NOTEQUAL};function at(b,v){if(v.type===di&&e.has("OES_texture_float_linear")===!1&&(v.magFilter===mn||v.magFilter===ld||v.magFilter===lc||v.magFilter===ir||v.minFilter===mn||v.minFilter===ld||v.minFilter===lc||v.minFilter===ir)&&Ie("WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(b,n.TEXTURE_WRAP_S,Ue[v.wrapS]),n.texParameteri(b,n.TEXTURE_WRAP_T,Ue[v.wrapT]),(b===n.TEXTURE_3D||b===n.TEXTURE_2D_ARRAY)&&n.texParameteri(b,n.TEXTURE_WRAP_R,Ue[v.wrapR]),n.texParameteri(b,n.TEXTURE_MAG_FILTER,Rt[v.magFilter]),n.texParameteri(b,n.TEXTURE_MIN_FILTER,Rt[v.minFilter]),v.compareFunction&&(n.texParameteri(b,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(b,n.TEXTURE_COMPARE_FUNC,Re[v.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(v.magFilter===Xt||v.minFilter!==lc&&v.minFilter!==ir||v.type===di&&e.has("OES_texture_float_linear")===!1)return;if(v.anisotropy>1||i.get(v).__currentAnisotropy){let U=e.get("EXT_texture_filter_anisotropic");n.texParameterf(b,U.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(v.anisotropy,r.getMaxAnisotropy())),i.get(v).__currentAnisotropy=v.anisotropy}}}function pt(b,v){let U=!1;b.__webglInit===void 0&&(b.__webglInit=!0,v.addEventListener("dispose",L),N(v));let ee=v.source,K=h.get(ee);K===void 0&&(K={},h.set(ee,K));let re=Y(v);if(re!==b.__cacheKey){K[re]===void 0&&(K[re]={texture:n.createTexture(),usedTimes:0},o.memory.textures++,U=!0),K[re].usedTimes++;let le=K[b.__cacheKey];le!==void 0&&(K[b.__cacheKey].usedTimes--,le.usedTimes===0&&H(v)),b.__cacheKey=re,b.__webglTexture=K[re].texture}return U}function qe(b,v,U){return Math.floor(Math.floor(b/U)/v)}function Yt(b,v,U,ee){let re=b.updateRanges;if(re.length===0)t.texSubImage2D(n.TEXTURE_2D,0,0,0,v.width,v.height,U,ee,v.data);else{re.sort((C,Z)=>C.start-Z.start);let le=0;for(let C=1;C<re.length;C++){let Z=re[le],W=re[C],ae=Z.start+Z.count,ie=qe(W.start,v.width,4),se=qe(Z.start,v.width,4);W.start<=ae+1&&ie===se&&qe(W.start+W.count-1,v.width,4)===ie?Z.count=Math.max(Z.count,W.start+W.count-Z.start):(++le,re[le]=W)}re.length=le+1;let q=t.getParameter(n.UNPACK_ROW_LENGTH),X=t.getParameter(n.UNPACK_SKIP_PIXELS),oe=t.getParameter(n.UNPACK_SKIP_ROWS);t.pixelStorei(n.UNPACK_ROW_LENGTH,v.width);for(let C=0,Z=re.length;C<Z;C++){let W=re[C],ae=Math.floor(W.start/4),ie=Math.ceil(W.count/4),se=ae%v.width,Ee=Math.floor(ae/v.width),ze=ie,ne=1;t.pixelStorei(n.UNPACK_SKIP_PIXELS,se),t.pixelStorei(n.UNPACK_SKIP_ROWS,Ee),t.texSubImage2D(n.TEXTURE_2D,0,se,Ee,ze,ne,U,ee,v.data)}b.clearUpdateRanges(),t.pixelStorei(n.UNPACK_ROW_LENGTH,q),t.pixelStorei(n.UNPACK_SKIP_PIXELS,X),t.pixelStorei(n.UNPACK_SKIP_ROWS,oe)}}function kt(b,v,U){let ee=n.TEXTURE_2D;(v.isDataArrayTexture||v.isCompressedArrayTexture)&&(ee=n.TEXTURE_2D_ARRAY),v.isData3DTexture&&(ee=n.TEXTURE_3D);let K=pt(b,v),re=v.source;t.bindTexture(ee,b.__webglTexture,n.TEXTURE0+U);let le=i.get(re);if(re.version!==le.__version||K===!0){if(t.activeTexture(n.TEXTURE0+U),(typeof ImageBitmap<"u"&&v.image instanceof ImageBitmap)===!1){let ne=Ke.getPrimaries(Ke.workingColorSpace),be=v.colorSpace===sr?null:Ke.getPrimaries(v.colorSpace),Be=v.colorSpace===sr||ne===be?n.NONE:n.BROWSER_DEFAULT_WEBGL;t.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),t.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),t.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,Be)}t.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment);let X=x(v.image,!1,r.maxTextureSize);X=Ae(v,X);let oe=s.convert(v.format,v.colorSpace),C=s.convert(v.type),Z=T(v.internalFormat,oe,C,v.normalized,v.colorSpace,v.isVideoTexture);at(ee,v);let W,ae=v.mipmaps,ie=v.isVideoTexture!==!0,se=le.__version===void 0||K===!0,Ee=re.dataReady,ze=S(v,X);if(v.isDepthTexture)Z=y(v.format===rr,v.type),se&&(ie?t.texStorage2D(n.TEXTURE_2D,1,Z,X.width,X.height):t.texImage2D(n.TEXTURE_2D,0,Z,X.width,X.height,0,oe,C,null));else if(v.isDataTexture)if(ae.length>0){ie&&se&&t.texStorage2D(n.TEXTURE_2D,ze,Z,ae[0].width,ae[0].height);for(let ne=0,be=ae.length;ne<be;ne++)W=ae[ne],ie?Ee&&t.texSubImage2D(n.TEXTURE_2D,ne,0,0,W.width,W.height,oe,C,W.data):t.texImage2D(n.TEXTURE_2D,ne,Z,W.width,W.height,0,oe,C,W.data);v.generateMipmaps=!1}else ie?(se&&t.texStorage2D(n.TEXTURE_2D,ze,Z,X.width,X.height),Ee&&Yt(v,X,oe,C)):t.texImage2D(n.TEXTURE_2D,0,Z,X.width,X.height,0,oe,C,X.data);else if(v.isCompressedTexture)if(v.isCompressedArrayTexture){ie&&se&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ze,Z,ae[0].width,ae[0].height,X.depth);for(let ne=0,be=ae.length;ne<be;ne++)if(W=ae[ne],v.format!==hi)if(oe!==null)if(ie){if(Ee)if(v.layerUpdates.size>0){let Be=og(W.width,W.height,v.format,v.type);for(let fe of v.layerUpdates){let ct=W.data.subarray(fe*Be/W.data.BYTES_PER_ELEMENT,(fe+1)*Be/W.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,ne,0,0,fe,W.width,W.height,1,oe,ct)}}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,ne,0,0,0,W.width,W.height,X.depth,oe,W.data)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,ne,Z,W.width,W.height,X.depth,0,W.data,0,0);else Ie("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else ie?Ee&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,ne,0,0,0,W.width,W.height,X.depth,oe,C,W.data):t.texImage3D(n.TEXTURE_2D_ARRAY,ne,Z,W.width,W.height,X.depth,0,oe,C,W.data);v.layerUpdates.size>0&&v.clearLayerUpdates()}else{ie&&se&&t.texStorage2D(n.TEXTURE_2D,ze,Z,ae[0].width,ae[0].height);for(let ne=0,be=ae.length;ne<be;ne++)W=ae[ne],v.format!==hi?oe!==null?ie?Ee&&t.compressedTexSubImage2D(n.TEXTURE_2D,ne,0,0,W.width,W.height,oe,W.data):t.compressedTexImage2D(n.TEXTURE_2D,ne,Z,W.width,W.height,0,W.data):Ie("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):ie?Ee&&t.texSubImage2D(n.TEXTURE_2D,ne,0,0,W.width,W.height,oe,C,W.data):t.texImage2D(n.TEXTURE_2D,ne,Z,W.width,W.height,0,oe,C,W.data)}else if(v.isDataArrayTexture)if(ie){if(se&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ze,Z,X.width,X.height,X.depth),Ee)if(v.layerUpdates.size>0){let ne=og(X.width,X.height,v.format,v.type);for(let be of v.layerUpdates){let Be=X.data.subarray(be*ne/X.data.BYTES_PER_ELEMENT,(be+1)*ne/X.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,be,X.width,X.height,1,oe,C,Be)}v.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,X.width,X.height,X.depth,oe,C,X.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,Z,X.width,X.height,X.depth,0,oe,C,X.data);else if(v.isData3DTexture)ie?(se&&t.texStorage3D(n.TEXTURE_3D,ze,Z,X.width,X.height,X.depth),Ee&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,X.width,X.height,X.depth,oe,C,X.data)):t.texImage3D(n.TEXTURE_3D,0,Z,X.width,X.height,X.depth,0,oe,C,X.data);else if(v.isFramebufferTexture){if(se)if(ie)t.texStorage2D(n.TEXTURE_2D,ze,Z,X.width,X.height);else{let ne=X.width,be=X.height;for(let Be=0;Be<ze;Be++)t.texImage2D(n.TEXTURE_2D,Be,Z,ne,be,0,oe,C,null),ne>>=1,be>>=1}}else if(v.isHTMLTexture){if("texElementImage2D"in n){let ne=n.canvas;if(ne.hasAttribute("layoutsubtree")||ne.setAttribute("layoutsubtree","true"),X.parentNode!==ne){ne.appendChild(X),d.add(v),ne.onpaint=be=>{let Be=be.changedElements;for(let fe of d)Be.includes(fe.image)&&(fe.needsUpdate=!0)},ne.requestPaint();return}if(n.texElementImage2D.length===3)n.texElementImage2D(n.TEXTURE_2D,n.RGBA8,X);else{let Be=n.RGBA,fe=n.RGBA,ct=n.UNSIGNED_BYTE;n.texElementImage2D(n.TEXTURE_2D,0,Be,fe,ct,X)}n.texParameteri(n.TEXTURE_2D,n.TEXTURE_MIN_FILTER,n.LINEAR),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_WRAP_S,n.CLAMP_TO_EDGE),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_WRAP_T,n.CLAMP_TO_EDGE)}}else if(ae.length>0){if(ie&&se){let ne=Oe(ae[0]);t.texStorage2D(n.TEXTURE_2D,ze,Z,ne.width,ne.height)}for(let ne=0,be=ae.length;ne<be;ne++)W=ae[ne],ie?Ee&&t.texSubImage2D(n.TEXTURE_2D,ne,0,0,oe,C,W):t.texImage2D(n.TEXTURE_2D,ne,Z,oe,C,W);v.generateMipmaps=!1}else if(ie){if(se){let ne=Oe(X);t.texStorage2D(n.TEXTURE_2D,ze,Z,ne.width,ne.height)}Ee&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,oe,C,X)}else t.texImage2D(n.TEXTURE_2D,0,Z,oe,C,X);w(v)&&v.mipmapsAutoUpdate===!0&&E(ee),le.__version=re.version,v.onUpdate&&v.onUpdate(v)}b.__version=v.version}function vn(b,v,U){if(v.image.length!==6)return;let ee=pt(b,v),K=v.source;t.bindTexture(n.TEXTURE_CUBE_MAP,b.__webglTexture,n.TEXTURE0+U);let re=i.get(K);if(K.version!==re.__version||ee===!0){t.activeTexture(n.TEXTURE0+U);let le=Ke.getPrimaries(Ke.workingColorSpace),q=v.colorSpace===sr?null:Ke.getPrimaries(v.colorSpace),X=v.colorSpace===sr||le===q?n.NONE:n.BROWSER_DEFAULT_WEBGL;t.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),t.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),t.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment),t.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,X);let oe=v.isCompressedTexture||v.image[0].isCompressedTexture,C=v.image[0]&&v.image[0].isDataTexture,Z=[];for(let fe=0;fe<6;fe++)!oe&&!C?Z[fe]=x(v.image[fe],!0,r.maxCubemapSize):Z[fe]=C?v.image[fe].image:v.image[fe],Z[fe]=Ae(v,Z[fe]);let W=Z[0],ae=s.convert(v.format,v.colorSpace),ie=s.convert(v.type),se=T(v.internalFormat,ae,ie,v.normalized,v.colorSpace),Ee=v.isVideoTexture!==!0,ze=re.__version===void 0||ee===!0,ne=K.dataReady,be=S(v,W);at(n.TEXTURE_CUBE_MAP,v);let Be;if(v.isDepthTexture){if(se=y(v.format===rr,v.type),ze)if(Ee)t.texStorage2D(n.TEXTURE_CUBE_MAP,1,se,W.width,W.height);else for(let fe=0;fe<6;fe++)t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+fe,0,se,W.width,W.height,0,ae,ie,null)}else if(oe){Ee&&ze&&t.texStorage2D(n.TEXTURE_CUBE_MAP,be,se,W.width,W.height);for(let fe=0;fe<6;fe++){Be=Z[fe].mipmaps;for(let ct=0;ct<Be.length;ct++){let Gt=Be[ct];v.format!==hi?ae!==null?Ee?ne&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+fe,ct,0,0,Gt.width,Gt.height,ae,Gt.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+fe,ct,se,Gt.width,Gt.height,0,Gt.data):Ie("WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):Ee?ne&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+fe,ct,0,0,Gt.width,Gt.height,ae,ie,Gt.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+fe,ct,se,Gt.width,Gt.height,0,ae,ie,Gt.data)}}}else{if(Be=v.mipmaps,Ee&&ze){Be.length>0&&be++;let fe=Oe(Z[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,be,se,fe.width,fe.height)}for(let fe=0;fe<6;fe++)if(C){Ee?ne&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+fe,0,0,0,Z[fe].width,Z[fe].height,ae,ie,Z[fe].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+fe,0,se,Z[fe].width,Z[fe].height,0,ae,ie,Z[fe].data);for(let ct=0;ct<Be.length;ct++){let or=Be[ct].image[fe].image;Ee?ne&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+fe,ct+1,0,0,or.width,or.height,ae,ie,or.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+fe,ct+1,se,or.width,or.height,0,ae,ie,or.data)}}else{Ee?ne&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+fe,0,0,0,ae,ie,Z[fe]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+fe,0,se,ae,ie,Z[fe]);for(let ct=0;ct<Be.length;ct++){let Gt=Be[ct];Ee?ne&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+fe,ct+1,0,0,ae,ie,Gt.image[fe]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+fe,ct+1,se,ae,ie,Gt.image[fe])}}}w(v)&&v.mipmapsAutoUpdate===!0&&E(n.TEXTURE_CUBE_MAP),re.__version=K.version,v.onUpdate&&v.onUpdate(v)}b.__version=v.version}function bt(b,v,U,ee,K,re){let le=s.convert(U.format,U.colorSpace),q=s.convert(U.type),X=T(U.internalFormat,le,q,U.normalized,U.colorSpace),oe=i.get(v),C=i.get(U);if(C.__renderTarget=v,!oe.__hasExternalTextures){let Z=Math.max(1,v.width>>re),W=Math.max(1,v.height>>re);K===n.TEXTURE_3D||K===n.TEXTURE_2D_ARRAY?t.texImage3D(K,re,X,Z,W,v.depth,0,le,q,null):t.texImage2D(K,re,X,Z,W,0,le,q,null)}t.bindFramebuffer(n.FRAMEBUFFER,b),de(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,ee,K,C.__webglTexture,0,St(v)):(K===n.TEXTURE_2D||K>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&K<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,ee,K,C.__webglTexture,re),t.bindFramebuffer(n.FRAMEBUFFER,null)}function pi(b,v,U){if(n.bindRenderbuffer(n.RENDERBUFFER,b),v.depthBuffer){let ee=v.depthTexture,K=ee&&ee.isDepthTexture?ee.type:null,re=y(v.stencilBuffer,K),le=v.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;de(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,St(v),re,v.width,v.height):U?n.renderbufferStorageMultisample(n.RENDERBUFFER,St(v),re,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,re,v.width,v.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,le,n.RENDERBUFFER,b)}else{let ee=v.textures;for(let K=0;K<ee.length;K++){let re=ee[K],le=s.convert(re.format,re.colorSpace),q=s.convert(re.type),X=T(re.internalFormat,le,q,re.normalized,re.colorSpace);de(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,St(v),X,v.width,v.height):U?n.renderbufferStorageMultisample(n.RENDERBUFFER,St(v),X,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,X,v.width,v.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function zt(b,v,U){let ee=v.isWebGLCubeRenderTarget===!0;if(t.bindFramebuffer(n.FRAMEBUFFER,b),!(v.depthTexture&&v.depthTexture.isDepthTexture))throw new Error("THREE.WebGLTextures: renderTarget.depthTexture must be an instance of THREE.DepthTexture.");let K=i.get(v.depthTexture);K.__renderTarget=v;let re=ee?v.depthTexture.image[0]:v.depthTexture.image;(!K.__webglTexture||re.width!==v.width||re.height!==v.height)&&(re.width=v.width,re.height=v.height,v.depthTexture.needsUpdate=!0),ee?me(v.depthTexture,0):pe(v.depthTexture,0);let le=K.__webglTexture,q=St(v),X=ee?n.TEXTURE_CUBE_MAP_POSITIVE_X+U:n.TEXTURE_2D,oe=v.depthTexture.format===rr?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;if(v.depthTexture.format===Ji)de(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,oe,X,le,0,q):n.framebufferTexture2D(n.FRAMEBUFFER,oe,X,le,0);else if(v.depthTexture.format===rr)de(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,oe,X,le,0,q):n.framebufferTexture2D(n.FRAMEBUFFER,oe,X,le,0);else throw new Error("THREE.WebGLTextures: Unknown depthTexture format.")}function kn(b){let v=i.get(b),U=b.isWebGLCubeRenderTarget===!0;if(v.__boundDepthTexture!==b.depthTexture){let ee=b.depthTexture;if(v.__depthDisposeCallback&&v.__depthDisposeCallback(),ee){let K=()=>{delete v.__boundDepthTexture,delete v.__depthDisposeCallback,ee.removeEventListener("dispose",K)};ee.addEventListener("dispose",K),v.__depthDisposeCallback=K}v.__boundDepthTexture=ee}if(b.depthTexture&&!v.__autoAllocateDepthBuffer)if(U)for(let ee=0;ee<6;ee++)zt(v.__webglFramebuffer[ee],b,ee);else b.texture.mipmaps.length>0?zt(v.__webglFramebuffer[0],b,0):zt(v.__webglFramebuffer,b,0);else if(U){v.__webglDepthbuffer=[];for(let ee=0;ee<6;ee++)if(t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[ee]),v.__webglDepthbuffer[ee]===void 0)v.__webglDepthbuffer[ee]=n.createRenderbuffer(),pi(v.__webglDepthbuffer[ee],b,!1);else{let K=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,re=v.__webglDepthbuffer[ee];n.bindRenderbuffer(n.RENDERBUFFER,re),n.framebufferRenderbuffer(n.FRAMEBUFFER,K,n.RENDERBUFFER,re)}}else if(b.texture.mipmaps.length>0?t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[0]):t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer),v.__webglDepthbuffer===void 0)v.__webglDepthbuffer=n.createRenderbuffer(),pi(v.__webglDepthbuffer,b,!1);else{let K=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,re=v.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,re),n.framebufferRenderbuffer(n.FRAMEBUFFER,K,n.RENDERBUFFER,re)}t.bindFramebuffer(n.FRAMEBUFFER,null)}function Un(b,v,U){let ee=i.get(b);v!==void 0&&bt(ee.__webglFramebuffer,b,b.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),U!==void 0&&kn(b)}function Bn(b){let v=b.texture,U=i.get(b),ee=i.get(v);b.addEventListener("dispose",F),N(b);let K=b.textures,re=b.isWebGLCubeRenderTarget===!0,le=K.length>1;if(le||(ee.__webglTexture===void 0&&(ee.__webglTexture=n.createTexture()),ee.__version=v.version,o.memory.textures++),re){U.__webglFramebuffer=[];for(let q=0;q<6;q++)if(v.mipmaps.length>0){U.__webglFramebuffer[q]=[];for(let X=0;X<v.mipmaps.length;X++)U.__webglFramebuffer[q][X]=n.createFramebuffer()}else U.__webglFramebuffer[q]=n.createFramebuffer()}else{if(v.mipmaps.length>0){U.__webglFramebuffer=[];for(let q=0;q<v.mipmaps.length;q++)U.__webglFramebuffer[q]=n.createFramebuffer()}else U.__webglFramebuffer=n.createFramebuffer();if(le)for(let q=0,X=K.length;q<X;q++){let oe=i.get(K[q]);oe.__webglTexture===void 0&&(oe.__webglTexture=n.createTexture(),o.memory.textures++)}if(b.samples>0&&de(b)===!1){U.__webglMultisampledFramebuffer=n.createFramebuffer(),U.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,U.__webglMultisampledFramebuffer);for(let q=0;q<K.length;q++){let X=K[q];U.__webglColorRenderbuffer[q]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,U.__webglColorRenderbuffer[q]);let oe=s.convert(X.format,X.colorSpace),C=s.convert(X.type),Z=T(X.internalFormat,oe,C,X.normalized,X.colorSpace,b.isXRRenderTarget===!0),W=St(b);n.renderbufferStorageMultisample(n.RENDERBUFFER,W,Z,b.width,b.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+q,n.RENDERBUFFER,U.__webglColorRenderbuffer[q])}n.bindRenderbuffer(n.RENDERBUFFER,null),b.depthBuffer&&(U.__webglDepthRenderbuffer=n.createRenderbuffer(),pi(U.__webglDepthRenderbuffer,b,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if(re){t.bindTexture(n.TEXTURE_CUBE_MAP,ee.__webglTexture),at(n.TEXTURE_CUBE_MAP,v);for(let q=0;q<6;q++)if(v.mipmaps.length>0)for(let X=0;X<v.mipmaps.length;X++)bt(U.__webglFramebuffer[q][X],b,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+q,X);else bt(U.__webglFramebuffer[q],b,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+q,0);w(v)?E(n.TEXTURE_CUBE_MAP):v.mipmaps.length>0&&n.texParameteri(n.TEXTURE_CUBE_MAP,n.TEXTURE_MAX_LEVEL,v.mipmaps.length-1),t.unbindTexture()}else if(le){for(let q=0,X=K.length;q<X;q++){let oe=K[q],C=i.get(oe),Z=n.TEXTURE_2D;(b.isWebGL3DRenderTarget||b.isWebGLArrayRenderTarget)&&(Z=b.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(Z,C.__webglTexture),at(Z,oe),bt(U.__webglFramebuffer,b,oe,n.COLOR_ATTACHMENT0+q,Z,0),w(oe)&&E(Z)}t.unbindTexture()}else{let q=n.TEXTURE_2D;if((b.isWebGL3DRenderTarget||b.isWebGLArrayRenderTarget)&&(q=b.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(q,ee.__webglTexture),at(q,v),v.mipmaps.length>0)for(let X=0;X<v.mipmaps.length;X++)bt(U.__webglFramebuffer[X],b,v,n.COLOR_ATTACHMENT0,q,X);else bt(U.__webglFramebuffer,b,v,n.COLOR_ATTACHMENT0,q,0);w(v)?E(q):v.mipmaps.length>0&&n.texParameteri(q,n.TEXTURE_MAX_LEVEL,v.mipmaps.length-1),t.unbindTexture()}b.depthBuffer&&kn(b)}function P(b){let v=b.textures;for(let U=0,ee=v.length;U<ee;U++){let K=v[U];if(w(K)&&K.mipmapsAutoUpdate===!0){let re=I(b),le=i.get(K).__webglTexture;t.bindTexture(re,le),E(re),t.unbindTexture()}}}let Vn=[];function Mt(b){if(b.samples>0){if(de(b)===!1){let v=b.textures,U=b.width,ee=b.height,K=n.COLOR_BUFFER_BIT,re=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,le=i.get(b),q=v.length>1;if(q)for(let oe=0;oe<v.length;oe++)t.bindFramebuffer(n.FRAMEBUFFER,le.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+oe,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,le.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+oe,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,le.__webglMultisampledFramebuffer),b.texture.mipmaps.length>0?t.bindFramebuffer(n.DRAW_FRAMEBUFFER,le.__webglFramebuffer[0]):t.bindFramebuffer(n.DRAW_FRAMEBUFFER,le.__webglFramebuffer);for(let oe=0;oe<v.length;oe++){if(b.resolveDepthBuffer&&(b.depthBuffer&&(K|=n.DEPTH_BUFFER_BIT),b.stencilBuffer&&b.resolveStencilBuffer&&(K|=n.STENCIL_BUFFER_BIT)),q){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,le.__webglColorRenderbuffer[oe]);let C=i.get(v[oe]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,C,0)}n.blitFramebuffer(0,0,U,ee,0,0,U,ee,K,n.NEAREST),c===!0&&(Vn.length=0,Vn.push(n.COLOR_ATTACHMENT0+oe),b.depthBuffer&&b.storeMultisampledDepthBuffer===!1&&Vn.push(re),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,Vn))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),q)for(let oe=0;oe<v.length;oe++){t.bindFramebuffer(n.FRAMEBUFFER,le.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+oe,n.RENDERBUFFER,le.__webglColorRenderbuffer[oe]);let C=i.get(v[oe]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,le.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+oe,n.TEXTURE_2D,C,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,le.__webglMultisampledFramebuffer)}else if(b.depthBuffer&&b.storeMultisampledDepthBuffer===!1&&c){let v=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[v])}}}function St(b){return Math.min(r.maxSamples,b.samples)}function de(b){let v=i.get(b);return b.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&v.__useRenderToTexture!==!1}function Ut(b){let v=o.render.frame;u.get(b)!==v&&(u.set(b,v),b.update())}function Ae(b,v){let U=b.colorSpace,ee=b.format,K=b.type;return b.isCompressedTexture===!0||b.isVideoTexture===!0||U!==Fa&&U!==sr&&(Ke.getTransfer(U)===ot?(ee!==hi||K!==En)&&Ie("WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):Ne("WebGLTextures: Unsupported texture color space:",U)),v}function Oe(b){return typeof HTMLImageElement<"u"&&b instanceof HTMLImageElement?(l.width=b.naturalWidth||b.width,l.height=b.naturalHeight||b.height):typeof VideoFrame<"u"&&b instanceof VideoFrame?(l.width=b.displayWidth,l.height=b.displayHeight):(l.width=b.width,l.height=b.height),l}this.allocateTextureUnit=Fe,this.resetTextureUnits=J,this.getTextureUnits=Q,this.setTextureUnits=Me,this.setTexture2D=pe,this.setTexture2DArray=ve,this.setTexture3D=Ce,this.setTextureCube=me,this.rebindTextures=Un,this.setupRenderTarget=Bn,this.updateRenderTargetMipmap=P,this.updateMultisampleRenderTarget=Mt,this.setupDepthRenderbuffer=kn,this.setupFrameBufferTexture=bt,this.useMultisampledRTT=de,this.dispose=z}function J1(n,e){function t(i,r=sr){let s,o=Ke.getTransfer(r);if(i===En)return n.UNSIGNED_BYTE;if(i===dd)return n.UNSIGNED_SHORT_4_4_4_4;if(i===fd)return n.UNSIGNED_SHORT_5_5_5_1;if(i===Gm)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===Wm)return n.UNSIGNED_INT_10F_11F_11F_REV;if(i===Hm)return n.BYTE;if(i===zm)return n.SHORT;if(i===Po)return n.UNSIGNED_SHORT;if(i===ud)return n.INT;if(i===ui)return n.UNSIGNED_INT;if(i===di)return n.FLOAT;if(i===fi)return n.HALF_FLOAT;if(i===jm)return n.ALPHA;if(i===$m)return n.RGB;if(i===hi)return n.RGBA;if(i===Ji)return n.DEPTH_COMPONENT;if(i===rr)return n.DEPTH_STENCIL;if(i===qm)return n.RED;if(i===hd)return n.RED_INTEGER;if(i===Lr)return n.RG;if(i===pd)return n.RG_INTEGER;if(i===md)return n.RGBA_INTEGER;if(i===uc||i===dc||i===fc||i===hc)if(o===ot)if(s=e.get("WEBGL_compressed_texture_s3tc_srgb"),s!==null){if(i===uc)return s.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===dc)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===fc)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===hc)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(s=e.get("WEBGL_compressed_texture_s3tc"),s!==null){if(i===uc)return s.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===dc)return s.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===fc)return s.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===hc)return s.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===gd||i===yd||i===vd||i===_d)if(s=e.get("WEBGL_compressed_texture_pvrtc"),s!==null){if(i===gd)return s.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===yd)return s.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===vd)return s.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===_d)return s.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===xd||i===Md||i===Sd||i===Ed||i===bd||i===pc||i===Td)if(s=e.get("WEBGL_compressed_texture_etc"),s!==null){if(i===xd||i===Md)return o===ot?s.COMPRESSED_SRGB8_ETC2:s.COMPRESSED_RGB8_ETC2;if(i===Sd)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:s.COMPRESSED_RGBA8_ETC2_EAC;if(i===Ed)return s.COMPRESSED_R11_EAC;if(i===bd)return s.COMPRESSED_SIGNED_R11_EAC;if(i===pc)return s.COMPRESSED_RG11_EAC;if(i===Td)return s.COMPRESSED_SIGNED_RG11_EAC}else return null;if(i===wd||i===Id||i===Cd||i===Ad||i===Dd||i===Rd||i===Nd||i===Pd||i===Ld||i===Od||i===Fd||i===kd||i===Ud||i===Bd)if(s=e.get("WEBGL_compressed_texture_astc"),s!==null){if(i===wd)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:s.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===Id)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:s.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===Cd)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:s.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===Ad)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:s.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===Dd)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:s.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===Rd)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:s.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===Nd)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:s.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===Pd)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:s.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===Ld)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:s.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===Od)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:s.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===Fd)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:s.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===kd)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:s.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===Ud)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:s.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===Bd)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:s.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===Vd||i===Hd||i===zd)if(s=e.get("EXT_texture_compression_bptc"),s!==null){if(i===Vd)return o===ot?s.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:s.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===Hd)return s.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===zd)return s.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===Gd||i===Wd||i===mc||i===jd)if(s=e.get("EXT_texture_compression_rgtc"),s!==null){if(i===Gd)return s.COMPRESSED_RED_RGTC1_EXT;if(i===Wd)return s.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===mc)return s.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===jd)return s.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===Ms?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var Q1=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,eN=`
uniform sampler2DArray depthColor;
uniform float depthWidth;
uniform float depthHeight;

void main() {

	vec2 coord = vec2( gl_FragCoord.x / depthWidth, gl_FragCoord.y / depthHeight );

	if ( coord.x >= 1.0 ) {

		gl_FragDepth = texture( depthColor, vec3( coord.x - 1.0, coord.y, 1 ) ).r;

	} else {

		gl_FragDepth = texture( depthColor, vec3( coord.x, coord.y, 0 ) ).r;

	}

}`,mg=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t){if(this.texture===null){let i=new qa(e.texture);(e.depthNear!==t.depthNear||e.depthFar!==t.depthFar)&&(this.depthNear=e.depthNear,this.depthFar=e.depthFar),this.texture=i}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new Ai({vertexShader:Q1,fragmentShader:eN,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new en(new Ka(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},gg=class extends ci{constructor(e,t){super();let i=this,r=null,s=1,o=null,a="local-floor",c=1,l=null,u=null,d=null,f=null,h=null,g=null,M=typeof XRWebGLBinding<"u",m=new mg,p={},x=t.getContextAttributes(),w=null,E=null,I=[],T=[],y=new Te,S=null,N=null,A=new hn;A.viewport=new Et;let L=new hn;L.viewport=new Et;let F=[A,L],G=new sd,D=null,z=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function(Y){let pe=I[Y];return pe===void 0&&(pe=new wo,I[Y]=pe),pe.getTargetRaySpace()},this.getControllerGrip=function(Y){let pe=I[Y];return pe===void 0&&(pe=new wo,I[Y]=pe),pe.getGripSpace()},this.getHand=function(Y){let pe=I[Y];return pe===void 0&&(pe=new wo,I[Y]=pe),pe.getHandSpace()};function $(Y){let pe=T.indexOf(Y.inputSource);if(pe===-1)return;let ve=I[pe];ve!==void 0&&(ve.update(Y.inputSource,Y.frame,l||o),ve.dispatchEvent({type:Y.type,data:Y.inputSource}))}function H(){r.removeEventListener("select",$),r.removeEventListener("selectstart",$),r.removeEventListener("selectend",$),r.removeEventListener("squeeze",$),r.removeEventListener("squeezestart",$),r.removeEventListener("squeezeend",$),r.removeEventListener("end",H),r.removeEventListener("inputsourceschange",te);for(let Y=0;Y<I.length;Y++){let pe=T[Y];pe!==null&&(T[Y]=null,I[Y].disconnect(pe))}D=null,z=null,m.reset();for(let Y in p)delete p[Y];if(e.setRenderTarget(w),h=null,f=null,d=null,r=null,E=null,Fe.stop(),i.isPresenting=!1,e.setPixelRatio(S),e.setSize(y.width,y.height,!1),N!==null){let Y=N.camera;Y.fov=N.fov,Y.zoom=N.zoom,Y.updateProjectionMatrix(),N=null}i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function(Y){s=Y,i.isPresenting===!0&&Ie("WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function(Y){a=Y,i.isPresenting===!0&&Ie("WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return l||o},this.setReferenceSpace=function(Y){l=Y},this.getBaseLayer=function(){return f!==null?f:h},this.getBinding=function(){return d===null&&M&&(d=new XRWebGLBinding(r,t)),d},this.getFrame=function(){return g},this.getSession=function(){return r},this.setSession=async function(Y){if(r=Y,r!==null){if(w=e.getRenderTarget(),r.addEventListener("select",$),r.addEventListener("selectstart",$),r.addEventListener("selectend",$),r.addEventListener("squeeze",$),r.addEventListener("squeezestart",$),r.addEventListener("squeezeend",$),r.addEventListener("end",H),r.addEventListener("inputsourceschange",te),x.xrCompatible!==!0&&await t.makeXRCompatible(),S=e.getPixelRatio(),e.getSize(y),M&&"createProjectionLayer"in XRWebGLBinding.prototype){let ve=null,Ce=null,me=null;x.depth&&(me=x.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,ve=x.stencil?rr:Ji,Ce=x.stencil?Ms:ui);let Ue={colorFormat:t.RGBA8,depthFormat:me,scaleFactor:s};d=this.getBinding(),f=d.createProjectionLayer(Ue),r.updateRenderState({layers:[f]}),e.setPixelRatio(1),e.setSize(f.textureWidth,f.textureHeight,!1),E=new On(f.textureWidth,f.textureHeight,{format:hi,type:En,depthTexture:new tr(f.textureWidth,f.textureHeight,Ce,void 0,void 0,void 0,void 0,void 0,void 0,ve),stencilBuffer:x.stencil,colorSpace:e.outputColorSpace,samples:x.antialias?4:0,resolveDepthBuffer:f.ignoreDepthValues===!1,resolveStencilBuffer:f.ignoreDepthValues===!1,storeMultisampledDepthBuffer:f.ignoreDepthValues===!1,storeMultisampledStencilBuffer:f.ignoreDepthValues===!1})}else{let ve={antialias:x.antialias,alpha:!0,depth:x.depth,stencil:x.stencil,framebufferScaleFactor:s};h=new XRWebGLLayer(r,t,ve),r.updateRenderState({baseLayer:h}),e.setPixelRatio(1),e.setSize(h.framebufferWidth,h.framebufferHeight,!1),E=new On(h.framebufferWidth,h.framebufferHeight,{format:hi,type:En,colorSpace:e.outputColorSpace,stencilBuffer:x.stencil,resolveDepthBuffer:h.ignoreDepthValues===!1,resolveStencilBuffer:h.ignoreDepthValues===!1,storeMultisampledDepthBuffer:h.ignoreDepthValues===!1,storeMultisampledStencilBuffer:h.ignoreDepthValues===!1})}E.isXRRenderTarget=!0,this.setFoveation(c),l=null,o=await r.requestReferenceSpace(a),Fe.setContext(r),Fe.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return m.getDepthTexture()};function te(Y){for(let pe=0;pe<Y.removed.length;pe++){let ve=Y.removed[pe],Ce=T.indexOf(ve);Ce>=0&&(T[Ce]=null,I[Ce].disconnect(ve))}for(let pe=0;pe<Y.added.length;pe++){let ve=Y.added[pe],Ce=T.indexOf(ve);if(Ce===-1){for(let Ue=0;Ue<I.length;Ue++)if(Ue>=T.length){T.push(ve),Ce=Ue;break}else if(T[Ue]===null){T[Ue]=ve,Ce=Ue;break}if(Ce===-1)break}let me=I[Ce];me&&me.connect(ve)}}function j(Y,pe){pe===null?Y.matrixWorld.copy(Y.matrix):Y.matrixWorld.multiplyMatrices(pe.matrixWorld,Y.matrix),Y.matrixWorldInverse.copy(Y.matrixWorld).invert()}this.updateCamera=function(Y){if(r===null)return;let pe=Y.near,ve=Y.far;m.texture!==null&&(m.depthNear>0&&(pe=m.depthNear),m.depthFar>0&&(ve=m.depthFar)),G.near=L.near=A.near=pe,G.far=L.far=A.far=ve,(D!==G.near||z!==G.far)&&(r.updateRenderState({depthNear:G.near,depthFar:G.far}),D=G.near,z=G.far),G.layers.mask=Y.layers.mask|6,A.layers.mask=G.layers.mask&-5,L.layers.mask=G.layers.mask&-3;let Ce=Y.parent,me=G.cameras;j(G,Ce);for(let Ue=0;Ue<me.length;Ue++)j(me[Ue],Ce);G.projectionMatrix.copy(A.projectionMatrix),G.projectionMatrixInverse.copy(A.projectionMatrixInverse),N===null&&Y.isPerspectiveCamera&&(N={camera:Y,fov:Y.fov,zoom:Y.zoom}),J(Y,G,Ce)};function J(Y,pe,ve){ve===null?Y.matrix.copy(pe.matrixWorld):(Y.matrix.copy(ve.matrixWorld),Y.matrix.invert(),Y.matrix.multiply(pe.matrixWorld)),Y.matrix.decompose(Y.position,Y.quaternion,Y.scale),Y.updateMatrixWorld(!0),Y.projectionMatrix.copy(pe.projectionMatrix),Y.projectionMatrixInverse.copy(pe.projectionMatrixInverse),Y.isPerspectiveCamera&&(Y.fov=Eo*2*Math.atan(1/Y.projectionMatrix.elements[5]),Y.zoom=1)}this.getCamera=function(){return G},this.getFoveation=function(){if(!(f===null&&h===null))return c},this.setFoveation=function(Y){c=Y,f!==null&&(f.fixedFoveation=Y),h!==null&&h.fixedFoveation!==void 0&&(h.fixedFoveation=Y)},this.hasDepthSensing=function(){return m.texture!==null},this.getDepthSensingMesh=function(){return m.getMesh(G)},this.getCameraTexture=function(Y){return p[Y]};let Q=null;function Me(Y,pe){if(u=pe.getViewerPose(l||o),g=pe,u!==null){let ve=u.views;h!==null&&(e.setRenderTargetFramebuffer(E,h.framebuffer),e.setRenderTarget(E));let Ce=!1;ve.length!==G.cameras.length&&(G.cameras.length=0,Ce=!0);for(let Re=0;Re<ve.length;Re++){let at=ve[Re],pt=null;if(h!==null)pt=h.getViewport(at);else{let Yt=d.getViewSubImage(f,at);pt=Yt.viewport,Re===0&&(e.setRenderTargetTextures(E,Yt.colorTexture,Yt.depthStencilTexture),e.setRenderTarget(E))}let qe=F[Re];qe===void 0&&(qe=new hn,qe.layers.enable(Re),qe.viewport=new Et,F[Re]=qe),qe.matrix.fromArray(at.transform.matrix),qe.matrix.decompose(qe.position,qe.quaternion,qe.scale),qe.projectionMatrix.fromArray(at.projectionMatrix),qe.projectionMatrixInverse.copy(qe.projectionMatrix).invert(),qe.viewport.set(pt.x,pt.y,pt.width,pt.height),Re===0&&(G.matrix.copy(qe.matrix),G.matrix.decompose(G.position,G.quaternion,G.scale)),Ce===!0&&G.cameras.push(qe)}let me=r.enabledFeatures;if(me&&me.includes("depth-sensing")&&r.depthUsage=="gpu-optimized"&&M){d=i.getBinding();let Re=d.getDepthInformation(ve[0]);Re&&Re.isValid&&Re.texture&&m.init(Re,r.renderState)}if(me&&me.includes("camera-access")&&M){e.state.unbindTexture(),d=i.getBinding();for(let Re=0;Re<ve.length;Re++){let at=ve[Re].camera;if(at){let pt=p[at];pt||(pt=new qa,p[at]=pt);let qe=d.getCameraImage(at);pt.sourceTexture=qe}}}}for(let ve=0;ve<I.length;ve++){let Ce=T[ve],me=I[ve];Ce!==null&&me!==void 0&&me.update(Ce,pe,l||o)}Q&&Q(Y,pe),pe.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:pe}),g=null}let Fe=new wM;Fe.setAnimationLoop(Me),this.setAnimationLoop=function(Y){Q=Y},this.dispose=function(){}}},tN=new _t,FM=new Le;FM.set(-1,0,0,0,1,0,0,0,1);function nN(n,e){function t(m,p){m.matrixAutoUpdate===!0&&m.updateMatrix(),p.value.copy(m.matrix)}function i(m,p){p.color.getRGB(m.fogColor.value,ig(n)),p.isFog?(m.fogNear.value=p.near,m.fogFar.value=p.far):p.isFogExp2&&(m.fogDensity.value=p.density)}function r(m,p,x,w,E){p.isNodeMaterial?p.uniformsNeedUpdate=!1:p.isMeshBasicMaterial?s(m,p):p.isMeshLambertMaterial?(s(m,p),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)):p.isMeshToonMaterial?(s(m,p),d(m,p)):p.isMeshPhongMaterial?(s(m,p),u(m,p),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)):p.isMeshStandardMaterial?(s(m,p),f(m,p),p.isMeshPhysicalMaterial&&h(m,p,E)):p.isMeshMatcapMaterial?(s(m,p),g(m,p)):p.isMeshDepthMaterial?s(m,p):p.isMeshDistanceMaterial?(s(m,p),M(m,p)):p.isMeshNormalMaterial?s(m,p):p.isLineBasicMaterial?(o(m,p),p.isLineDashedMaterial&&a(m,p)):p.isPointsMaterial?c(m,p,x,w):p.isSpriteMaterial?l(m,p):p.isShadowMaterial?(m.color.value.copy(p.color),m.opacity.value=p.opacity):p.isShaderMaterial&&(p.uniformsNeedUpdate=!1)}function s(m,p){m.opacity.value=p.opacity,p.color&&m.diffuse.value.copy(p.color),p.emissive&&m.emissive.value.copy(p.emissive).multiplyScalar(p.emissiveIntensity),p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.bumpMap&&(m.bumpMap.value=p.bumpMap,t(p.bumpMap,m.bumpMapTransform),m.bumpScale.value=p.bumpScale,p.side===gn&&(m.bumpScale.value*=-1)),p.normalMap&&(m.normalMap.value=p.normalMap,t(p.normalMap,m.normalMapTransform),m.normalScale.value.copy(p.normalScale),p.side===gn&&m.normalScale.value.negate()),p.displacementMap&&(m.displacementMap.value=p.displacementMap,t(p.displacementMap,m.displacementMapTransform),m.displacementScale.value=p.displacementScale,m.displacementBias.value=p.displacementBias),p.emissiveMap&&(m.emissiveMap.value=p.emissiveMap,t(p.emissiveMap,m.emissiveMapTransform)),p.specularMap&&(m.specularMap.value=p.specularMap,t(p.specularMap,m.specularMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest);let x=e.get(p),w=x.envMap,E=x.envMapRotation;w&&(m.envMap.value=w,m.envMapRotation.value.setFromMatrix4(tN.makeRotationFromEuler(E)).transpose(),w.isCubeTexture&&w.isRenderTargetTexture===!1&&m.envMapRotation.value.premultiply(FM),m.reflectivity.value=p.reflectivity,m.ior.value=p.ior,m.refractionRatio.value=p.refractionRatio),p.lightMap&&(m.lightMap.value=p.lightMap,m.lightMapIntensity.value=p.lightMapIntensity,t(p.lightMap,m.lightMapTransform)),p.aoMap&&(m.aoMap.value=p.aoMap,m.aoMapIntensity.value=p.aoMapIntensity,t(p.aoMap,m.aoMapTransform))}function o(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform))}function a(m,p){m.dashSize.value=p.dashSize,m.totalSize.value=p.dashSize+p.gapSize,m.scale.value=p.scale}function c(m,p,x,w){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.size.value=p.size*x,m.scale.value=w*.5,p.map&&(m.map.value=p.map,t(p.map,m.uvTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function l(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.rotation.value=p.rotation,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function u(m,p){m.specular.value.copy(p.specular),m.shininess.value=Math.max(p.shininess,1e-4)}function d(m,p){p.gradientMap&&(m.gradientMap.value=p.gradientMap)}function f(m,p){m.metalness.value=p.metalness,p.metalnessMap&&(m.metalnessMap.value=p.metalnessMap,t(p.metalnessMap,m.metalnessMapTransform)),m.roughness.value=p.roughness,p.roughnessMap&&(m.roughnessMap.value=p.roughnessMap,t(p.roughnessMap,m.roughnessMapTransform)),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)}function h(m,p,x){m.ior.value=p.ior,p.diffuseRoughness>0&&(m.diffuseRoughness.value=p.diffuseRoughness,p.diffuseRoughnessMap&&(m.diffuseRoughnessMap.value=p.diffuseRoughnessMap,t(p.diffuseRoughnessMap,m.diffuseRoughnessMapTransform))),p.sheen>0&&(m.sheenColor.value.copy(p.sheenColor).multiplyScalar(p.sheen),m.sheenRoughness.value=p.sheenRoughness,p.sheenColorMap&&(m.sheenColorMap.value=p.sheenColorMap,t(p.sheenColorMap,m.sheenColorMapTransform)),p.sheenRoughnessMap&&(m.sheenRoughnessMap.value=p.sheenRoughnessMap,t(p.sheenRoughnessMap,m.sheenRoughnessMapTransform))),p.clearcoat>0&&(m.clearcoat.value=p.clearcoat,m.clearcoatRoughness.value=p.clearcoatRoughness,p.clearcoatMap&&(m.clearcoatMap.value=p.clearcoatMap,t(p.clearcoatMap,m.clearcoatMapTransform)),p.clearcoatRoughnessMap&&(m.clearcoatRoughnessMap.value=p.clearcoatRoughnessMap,t(p.clearcoatRoughnessMap,m.clearcoatRoughnessMapTransform)),p.clearcoatNormalMap&&(m.clearcoatNormalMap.value=p.clearcoatNormalMap,t(p.clearcoatNormalMap,m.clearcoatNormalMapTransform),m.clearcoatNormalScale.value.copy(p.clearcoatNormalScale),p.side===gn&&m.clearcoatNormalScale.value.negate())),p.dispersion>0&&(m.dispersion.value=p.dispersion),p.retroreflectivity>0&&(m.retroreflectivity.value=p.retroreflectivity),p.iridescence>0&&(m.iridescence.value=p.iridescence,m.iridescenceIOR.value=p.iridescenceIOR,m.iridescenceThicknessMinimum.value=p.iridescenceThicknessMinimum,m.iridescenceThicknessMaximum.value=p.iridescenceThicknessMaximum,p.iridescenceMap&&(m.iridescenceMap.value=p.iridescenceMap,t(p.iridescenceMap,m.iridescenceMapTransform)),p.iridescenceThicknessMap&&(m.iridescenceThicknessMap.value=p.iridescenceThicknessMap,t(p.iridescenceThicknessMap,m.iridescenceThicknessMapTransform))),p.transmission>0&&(m.transmission.value=p.transmission,m.transmissionSamplerMap.value=x.texture,m.transmissionSamplerSize.value.set(x.width,x.height),p.transmissionMap&&(m.transmissionMap.value=p.transmissionMap,t(p.transmissionMap,m.transmissionMapTransform)),m.thickness.value=p.thickness,p.thicknessMap&&(m.thicknessMap.value=p.thicknessMap,t(p.thicknessMap,m.thicknessMapTransform)),m.attenuationDistance.value=p.attenuationDistance,m.attenuationColor.value.copy(p.attenuationColor)),p.anisotropy>0&&(m.anisotropyVector.value.set(p.anisotropy*Math.cos(p.anisotropyRotation),p.anisotropy*Math.sin(p.anisotropyRotation)),p.anisotropyMap&&(m.anisotropyMap.value=p.anisotropyMap,t(p.anisotropyMap,m.anisotropyMapTransform))),m.specularIntensity.value=p.specularIntensity,m.specularColor.value.copy(p.specularColor),p.specularColorMap&&(m.specularColorMap.value=p.specularColorMap,t(p.specularColorMap,m.specularColorMapTransform)),p.specularIntensityMap&&(m.specularIntensityMap.value=p.specularIntensityMap,t(p.specularIntensityMap,m.specularIntensityMapTransform))}function g(m,p){p.matcap&&(m.matcap.value=p.matcap)}function M(m,p){let x=e.get(p).light;m.referencePosition.value.setFromMatrixPosition(x.matrixWorld),m.nearDistance.value=x.shadow.camera.near,m.farDistance.value=x.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function iN(n,e,t,i){let r={},s={};function o(x,w){let E=w.program;i.uniformBlockBinding(x,E,r[x.id])}function a(x,w){let E=r[x.id];E===void 0&&(g(x),E=c(x),r[x.id]=E,x.addEventListener("dispose",m));let I=w.program;i.updateUBOMapping(x,I);let T=e.render.frame;s[x.id]!==T&&(l(x),s[x.id]=T)}function c(x){let w=n.createBuffer(),E=x.__size,I=x.usage;return n.bindBuffer(n.UNIFORM_BUFFER,w),n.bufferData(n.UNIFORM_BUFFER,E,I),n.bindBuffer(n.UNIFORM_BUFFER,null),w}function l(x){let w=r[x.id],E=x.uniforms,I=x.__cache;n.bindBuffer(n.UNIFORM_BUFFER,w);for(let T=0,y=E.length;T<y;T++){let S=E[T];if(Array.isArray(S))for(let N=0,A=S.length;N<A;N++)u(S[N],T,N,I);else u(S,T,0,I)}n.bindBuffer(n.UNIFORM_BUFFER,null)}function u(x,w,E,I){if(f(x,w,E,I)===!0){let T=x.__offset,y=x.value;if(Array.isArray(y)){let S=0;for(let N=0;N<y.length;N++){let A=y[N],L=M(A,!0);d(A,x.__data,S),S+=L.storage/Float32Array.BYTES_PER_ELEMENT}}else d(y,x.__data,0);n.bufferSubData(n.UNIFORM_BUFFER,T,x.__data)}}function d(x,w,E){typeof x=="number"||typeof x=="boolean"?w[E]=x:x.isMatrix3?(w[E]=x.elements[0],w[E+1]=x.elements[1],w[E+2]=x.elements[2],w[E+3]=0,w[E+4]=x.elements[3],w[E+5]=x.elements[4],w[E+6]=x.elements[5],w[E+7]=0,w[E+8]=x.elements[6],w[E+9]=x.elements[7],w[E+10]=x.elements[8],w[E+11]=0):ArrayBuffer.isView(x)?w.set(x.subarray(0,w.length-E),E):x.toArray(w,E)}function f(x,w,E,I){let T=x.value,y=w+"_"+E;if(Array.isArray(T)){I[y]===void 0&&(I[y]=[]);let S=I[y],N=!1;for(let A=0;A<T.length;A++)h(T[A],S,A)===!0&&(N=!0);return N}return h(T,I,y)}function h(x,w,E){if(w[E]===void 0)return typeof x=="number"||typeof x=="boolean"?w[E]=x:ArrayBuffer.isView(x)?w[E]=x.slice():w[E]=x.clone(),!0;{let I=w[E];if(typeof x=="number"||typeof x=="boolean"){if(I!==x)return w[E]=x,!0}else{if(ArrayBuffer.isView(x))return!0;if(I.equals(x)===!1)return I.copy(x),!0}}return!1}function g(x){let w=x.uniforms,E=0,I=16;for(let y=0,S=w.length;y<S;y++){let N=Array.isArray(w[y])?w[y]:[w[y]];for(let A=0,L=N.length;A<L;A++){let F=N[A],G=Array.isArray(F.value)?F.value:[F.value],D=Array.isArray(w[y])||Array.isArray(F.value),z=E;for(let H=0,te=G.length;H<te;H++){let j=G[H],J=M(j,D),Q=E%I,Me=Q%J.boundary,Fe=Q+Me;E+=Me,Fe!==0&&I-Fe<J.storage&&(E+=I-Fe),H===0&&(z=E),E+=J.storage}let $=F.type==="int"?Int32Array:F.type==="uint"?Uint32Array:Float32Array;F.__data=new $((E-z)/Float32Array.BYTES_PER_ELEMENT),F.__offset=z}}let T=E%I;return T>0&&(E+=I-T),x.__size=E,x.__cache={},this}function M(x,w){let E={boundary:0,storage:0};return typeof x=="number"||typeof x=="boolean"?(E.boundary=4,E.storage=4):x.isVector2?(E.boundary=8,E.storage=8):x.isVector3||x.isColor?(E.boundary=16,E.storage=12):x.isVector4?(E.boundary=16,E.storage=16):x.isMatrix3?(E.boundary=16,E.storage=48):x.isMatrix4?(E.boundary=16,E.storage=64):x.isTexture?Ie("WebGLRenderer: Texture samplers can not be part of an uniforms group."):ArrayBuffer.isView(x)?(E.boundary=16,E.storage=x.byteLength):Ie("WebGLRenderer: Unsupported uniform value type.",x),w===!0&&(E.boundary=16,E.storage=Math.ceil(E.storage/16)*16),E}function m(x){let w=x.target;w.removeEventListener("dispose",m),n.deleteBuffer(r[w.id]),delete r[w.id],delete s[w.id]}function p(){for(let x in r)n.deleteBuffer(r[x]);r={},s={}}return{bind:o,update:a,dispose:p}}var rN=new Uint16Array([12469,15057,12620,14925,13266,14620,13807,14376,14323,13990,14545,13625,14713,13328,14840,12882,14931,12528,14996,12233,15039,11829,15066,11525,15080,11295,15085,10976,15082,10705,15073,10495,13880,14564,13898,14542,13977,14430,14158,14124,14393,13732,14556,13410,14702,12996,14814,12596,14891,12291,14937,11834,14957,11489,14958,11194,14943,10803,14921,10506,14893,10278,14858,9960,14484,14039,14487,14025,14499,13941,14524,13740,14574,13468,14654,13106,14743,12678,14818,12344,14867,11893,14889,11509,14893,11180,14881,10751,14852,10428,14812,10128,14765,9754,14712,9466,14764,13480,14764,13475,14766,13440,14766,13347,14769,13070,14786,12713,14816,12387,14844,11957,14860,11549,14868,11215,14855,10751,14825,10403,14782,10044,14729,9651,14666,9352,14599,9029,14967,12835,14966,12831,14963,12804,14954,12723,14936,12564,14917,12347,14900,11958,14886,11569,14878,11247,14859,10765,14828,10401,14784,10011,14727,9600,14660,9289,14586,8893,14508,8533,15111,12234,15110,12234,15104,12216,15092,12156,15067,12010,15028,11776,14981,11500,14942,11205,14902,10752,14861,10393,14812,9991,14752,9570,14682,9252,14603,8808,14519,8445,14431,8145,15209,11449,15208,11451,15202,11451,15190,11438,15163,11384,15117,11274,15055,10979,14994,10648,14932,10343,14871,9936,14803,9532,14729,9218,14645,8742,14556,8381,14461,8020,14365,7603,15273,10603,15272,10607,15267,10619,15256,10631,15231,10614,15182,10535,15118,10389,15042,10167,14963,9787,14883,9447,14800,9115,14710,8665,14615,8318,14514,7911,14411,7507,14279,7198,15314,9675,15313,9683,15309,9712,15298,9759,15277,9797,15229,9773,15166,9668,15084,9487,14995,9274,14898,8910,14800,8539,14697,8234,14590,7790,14479,7409,14367,7067,14178,6621,15337,8619,15337,8631,15333,8677,15325,8769,15305,8871,15264,8940,15202,8909,15119,8775,15022,8565,14916,8328,14804,8009,14688,7614,14569,7287,14448,6888,14321,6483,14088,6171,15350,7402,15350,7419,15347,7480,15340,7613,15322,7804,15287,7973,15229,8057,15148,8012,15046,7846,14933,7611,14810,7357,14682,7069,14552,6656,14421,6316,14251,5948,14007,5528,15356,5942,15356,5977,15353,6119,15348,6294,15332,6551,15302,6824,15249,7044,15171,7122,15070,7050,14949,6861,14818,6611,14679,6349,14538,6067,14398,5651,14189,5311,13935,4958,15359,4123,15359,4153,15356,4296,15353,4646,15338,5160,15311,5508,15263,5829,15188,6042,15088,6094,14966,6001,14826,5796,14678,5543,14527,5287,14377,4985,14133,4586,13869,4257,15360,1563,15360,1642,15358,2076,15354,2636,15341,3350,15317,4019,15273,4429,15203,4732,15105,4911,14981,4932,14836,4818,14679,4621,14517,4386,14359,4156,14083,3795,13808,3437,15360,122,15360,137,15358,285,15355,636,15344,1274,15322,2177,15281,2765,15215,3223,15120,3451,14995,3569,14846,3567,14681,3466,14511,3305,14344,3121,14037,2800,13753,2467,15360,0,15360,1,15359,21,15355,89,15346,253,15325,479,15287,796,15225,1148,15133,1492,15008,1749,14856,1882,14685,1886,14506,1783,14324,1608,13996,1398,13702,1183]),Pi=null;function sN(){return Pi===null&&(Pi=new Vu(rN,16,16,Lr,fi),Pi.name="DFG_LUT",Pi.minFilter=mn,Pi.magFilter=mn,Pi.wrapS=wi,Pi.wrapT=wi,Pi.generateMipmaps=!1,Pi.needsUpdate=!0),Pi}var Zd=class{constructor(e={}){let{canvas:t=Yx(),context:i=null,depth:r=!0,stencil:s=!1,alpha:o=!1,antialias:a=!1,premultipliedAlpha:c=!0,preserveDrawingBuffer:l=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:d=!1,reversedDepthBuffer:f=!1,outputBufferType:h=En}=e;this.isWebGLRenderer=!0;let g;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");g=i.getContextAttributes().alpha}else g=o;let M=h,m=new Set([md,pd,hd]),p=new Set([En,ui,Po,Ms,dd,fd]),x=new Uint32Array(4),w=new Int32Array(4),E={textures:[],min:[],max:[],resolution:[]},I=null,T=null,y=[],S=[],N=null;this.domElement=t,this.debug={checkShaderErrors:!0,diagnostics:{keywords:!1},onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this.toneMapping=Ni,this.toneMappingExposure=1,this.transmissionResolutionScale=1;let A=this,L=!1,F=null,G=null,D=null,z=null;this._outputColorSpace=Pn;let $=0,H=0,te=null,j=-1,J=null,Q=new Et,Me=new Et,Fe=null,Y=new He(0),pe=0,ve=t.width,Ce=t.height,me=1,Ue=null,Rt=null,Re=new Et(0,0,ve,Ce),at=new Et(0,0,ve,Ce),pt=!1,qe=new Cr,Yt=new Hu,kt=!1,vn=!1,bt=new _t,pi=new O,zt=new Et,kn={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},Un=!1;function Bn(){return te===null?me:1}let P=i;function Vn(_,R){return t.getContext(_,R)}let Mt,St,de,Ut,Ae,Oe,b,v,U,ee,K,re,le,q,X,oe,C,Z,W,ae,ie,se,Ee;try{let _={alpha:!0,depth:r,stencil:s,antialias:a,premultipliedAlpha:c,preserveDrawingBuffer:l,powerPreference:u,failIfMajorPerformanceCaveat:d};if("setAttribute"in t&&t.setAttribute("data-engine",`three.js r${"187"}`),t.addEventListener("webglcontextlost",be,!1),t.addEventListener("webglcontextrestored",Be,!1),t.addEventListener("webglcontextcreationerror",fe,!1),P===null){let R="webgl2";if(P=Vn(R,_),P===null)throw Vn(R)?new Error("THREE.WebGLRenderer: Error creating WebGL context with your selected attributes."):new Error("THREE.WebGLRenderer: Error creating WebGL context.")}ze()}catch(_){throw t.removeEventListener("webglcontextlost",be,!1),t.removeEventListener("webglcontextrestored",Be,!1),t.removeEventListener("webglcontextcreationerror",fe,!1),Ne("WebGLRenderer: "+_.message),_}function ze(){Mt=new gR(P),Mt.init(),ie=new J1(P,Mt),St=new sR(P,Mt,e,ie),de=new Z1(P,Mt),St.reversedDepthBuffer&&f&&de.buffers.depth.setReversed(!0),G=P.createFramebuffer(),D=P.createFramebuffer(),z=P.createFramebuffer(),Ut=new _R(P),Ae=new F1,Oe=new K1(P,Mt,de,Ae,St,ie,Ut),b=new mR(A),v=new MC(P),se=new iR(P,v),U=new yR(P,v,Ut,se),ee=new MR(P,U,v,se,Ut),Z=new xR(P,St,Oe),X=new oR(Ae),K=new O1(A,b,Mt,St,se,X),re=new nN(A,Ae),le=new U1,q=new W1(Mt),C=new nR(A,b,de,ee,g,c),oe=new Y1(A,ee,St),Ee=new iN(P,Ut,St,de),W=new rR(P,Mt,Ut),ae=new vR(P,Mt,Ut),Ut.programs=K.programs,A.capabilities=St,A.extensions=Mt,A.properties=Ae,A.renderLists=le,A.shadowMap=oe,A.state=de,A.info=Ut}M!==En&&(N=new ER(M,t.width,t.height,a,r,s));let ne=new gg(A,P);this.xr=ne,this.getContext=function(){return P},this.getContextAttributes=function(){return P.getContextAttributes()},this.forceContextLoss=function(){let _=Mt.get("WEBGL_lose_context");_&&_.loseContext()},this.forceContextRestore=function(){let _=Mt.get("WEBGL_lose_context");_&&_.restoreContext()},this.getPixelRatio=function(){return me},this.setPixelRatio=function(_){_!==void 0&&(me=_,this.setSize(ve,Ce,!1))},this.getSize=function(_){return _.set(ve,Ce)},this.setSize=function(_,R,V=!0){if(ne.isPresenting){Ie("WebGLRenderer: Can't change size while VR device is presenting.");return}ve=_,Ce=R,t.width=Math.floor(_*me),t.height=Math.floor(R*me),V===!0&&(t.style.width=_+"px",t.style.height=R+"px"),N!==null&&N.setSize(t.width,t.height),this.setViewport(0,0,_,R)},this.getDrawingBufferSize=function(_){return _.set(ve*me,Ce*me).floor()},this.setDrawingBufferSize=function(_,R,V){ve=_,Ce=R,me=V,t.width=Math.floor(_*V),t.height=Math.floor(R*V),this.setViewport(0,0,_,R)},this.setEffects=function(_){if(M===En){Ne("WebGLRenderer: setEffects() requires outputBufferType set to HalfFloatType or FloatType.");return}if(_){for(let R=0;R<_.length;R++)if(_[R].isOutputPass===!0){Ie("WebGLRenderer: OutputPass is not needed in setEffects(). Tone mapping and color space conversion are applied automatically.");break}}N.setEffects(_||[])},this.getCurrentViewport=function(_){return _.copy(Q)},this.getViewport=function(_){return _.copy(Re)},this.setViewport=function(_,R,V,k){_.isVector4?Re.set(_.x,_.y,_.z,_.w):Re.set(_,R,V,k),de.viewport(Q.copy(Re).multiplyScalar(Bn()).round())},this.getScissor=function(_){return _.copy(at)},this.setScissor=function(_,R,V,k){_.isVector4?at.set(_.x,_.y,_.z,_.w):at.set(_,R,V,k),de.scissor(Me.copy(at).multiplyScalar(Bn()).round())},this.getScissorTest=function(){return pt},this.setScissorTest=function(_){de.setScissorTest(pt=_)},this.setOpaqueSort=function(_){Ue=_},this.setTransparentSort=function(_){Rt=_},this.getClearColor=function(_){return _.copy(C.getClearColor())},this.setClearColor=function(){C.setClearColor(...arguments)},this.getClearAlpha=function(){return C.getClearAlpha()},this.setClearAlpha=function(){C.setClearAlpha(...arguments)},this.clear=function(_=!0,R=!0,V=!0){let k=0;if(_){let B=!1;if(te!==null){let ue=te.texture.format;B=m.has(ue)}if(B){let ue=te.texture.type,ye=p.has(ue),ce=C.getClearColor(),ge=C.getClearAlpha(),Se=ce.r,ke=ce.g,Xe=ce.b;ye?(x[0]=Se,x[1]=ke,x[2]=Xe,x[3]=ge,P.clearBufferuiv(P.COLOR,0,x)):(w[0]=Se,w[1]=ke,w[2]=Xe,w[3]=ge,P.clearBufferiv(P.COLOR,0,w))}else k|=P.COLOR_BUFFER_BIT}R&&(k|=P.DEPTH_BUFFER_BIT,this.state.buffers.depth.setMask(!0)),V&&(k|=P.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),k!==0&&P.clear(k)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.setNodesHandler=function(_){_.setRenderer(this),F=_},this.dispose=function(){t.removeEventListener("webglcontextlost",be,!1),t.removeEventListener("webglcontextrestored",Be,!1),t.removeEventListener("webglcontextcreationerror",fe,!1),C.dispose(),le.dispose(),q.dispose(),Oe.dispose(),Ae.dispose(),b.dispose(),ee.dispose(),U.dispose(),se.dispose(),Ee.dispose(),K.dispose(),ne.dispose(),ne.removeEventListener("sessionstart",wg),ne.removeEventListener("sessionend",Ig),Fr.stop()};function be(_){_.preventDefault(),Jm("WebGLRenderer: Context Lost."),L=!0}function Be(){Jm("WebGLRenderer: Context Restored."),L=!1;let _=Ut.autoReset,R=oe.enabled,V=oe.autoUpdate,k=oe.needsUpdate,B=oe.type;ze(),Ut.autoReset=_,oe.enabled=R,oe.autoUpdate=V,oe.needsUpdate=k,oe.type=B}function fe(_){Ne("WebGLRenderer: A WebGL context could not be created. Reason: ",_.statusMessage)}function ct(_){let R=_.target;R.removeEventListener("dispose",ct),Gt(R)}function Gt(_){or(_),Ae.remove(_)}function or(_){let R=Ae.get(_).programs;R!==void 0&&(R.forEach(function(V){K.releaseProgram(V)}),_.isShaderMaterial&&K.releaseShaderCache(_))}this.renderBufferDirect=function(_,R,V,k,B,ue){R===null&&(R=kn);let ye=B.isMesh&&B.matrixWorld.determinantAffine()<0,ce=KM(_,R,V,k,B);de.setMaterial(k,ye);let ge=V.index,Se=1;if(k.wireframe===!0){if(ge=U.getWireframeAttribute(V),ge===void 0)return;Se=2}let ke=V.drawRange,Xe=V.attributes.position,_e=ke.start*Se,Ye=(ke.start+ke.count)*Se;ue!==null&&(_e=Math.max(_e,ue.start*Se),Ye=Math.min(Ye,(ue.start+ue.count)*Se)),ge!==null?(_e=Math.max(_e,0),Ye=Math.min(Ye,ge.count)):Xe!=null&&(_e=Math.max(_e,0),Ye=Math.min(Ye,Xe.count));let Nt=Ye-_e;if(Nt<0||Nt===1/0)return;se.setup(B,k,ce,V,ge);let yt,mt=W;if(ge!==null&&(yt=v.get(ge),mt=ae,mt.setIndex(yt)),B.isMesh)k.wireframe===!0?(de.setLineWidth(k.wireframeLinewidth*Bn()),mt.setMode(P.LINES)):mt.setMode(P.TRIANGLES);else if(B.isLine){let nn=k.linewidth;nn===void 0&&(nn=1),de.setLineWidth(nn*Bn()),B.isLineSegments?mt.setMode(P.LINES):B.isLineLoop?mt.setMode(P.LINE_LOOP):mt.setMode(P.LINE_STRIP)}else B.isPoints?mt.setMode(P.POINTS):B.isSprite&&mt.setMode(P.TRIANGLES);if(B.isBatchedMesh)if(Mt.get("WEBGL_multi_draw"))mt.renderMultiDraw(B._multiDrawStarts,B._multiDrawCounts,B._multiDrawCount);else{let nn=B._multiDrawStarts,xe=B._multiDrawCounts,un=B._multiDrawCount,nt=ge?v.get(ge).bytesPerElement:1,kr=Ae.get(k).currentProgram.getUniforms();for(let rn=0;rn<un;rn++)kr.setValue(P,"_gl_DrawID",rn),mt.render(nn[rn]/nt,xe[rn])}else if(B.isInstancedMesh)mt.renderInstances(_e,Nt,B.count);else if(V.isInstancedBufferGeometry){let nn=V._maxInstanceCount!==void 0?V._maxInstanceCount:1/0,xe=Math.min(V.instanceCount,nn);mt.renderInstances(_e,Nt,xe)}else mt.render(_e,Nt)};function Tg(_,R,V,k){F!==null&&_.isNodeMaterial&&F.setObject(k,_),kt===!0&&X.setState(_,V,!1),_.transparent===!0&&_.side===Ri&&_.forceSinglePass===!1?(_.side=gn,_.needsUpdate=!0,Uo(_,R,k),_.side=Pr,_.needsUpdate=!0,Uo(_,R,k),_.side=Ri):Uo(_,R,k)}this.compile=function(_,R,V=null){V===null&&(V=_),F!==null&&F.renderStart(_,R,V),T=q.get(V),T.init(R),S.push(T),V.traverseVisible(function(B){B.isLight&&B.layers.test(R.layers)&&(T.pushLight(B),B.castShadow&&T.pushShadow(B))}),_!==V&&_.traverseVisible(function(B){B.isLight&&B.layers.test(R.layers)&&(T.pushLight(B),B.castShadow&&T.pushShadow(B))}),T.setupLights(),F!==null&&F.updateLights(T.state.lightsArray),vn=this.localClippingEnabled,kt=X.init(this.clippingPlanes,vn),kt===!0&&X.setGlobalState(this.clippingPlanes,R),F!==null&&oe.render(T.state.shadowsArray,V,R);let k=new Set;return _.traverse(function(B){if(!(B.isMesh||B.isPoints||B.isLine||B.isSprite))return;let ue=B.material;if(ue)if(Array.isArray(ue))for(let ye=0;ye<ue.length;ye++){let ce=ue[ye];Tg(ce,V,R,B),k.add(ce)}else Tg(ue,V,R,B),k.add(ue)}),T=S.pop(),F!==null&&F.renderEnd(),k},this.compileAsync=function(_,R,V=null){let k=this.compile(_,R,V);return new Promise(B=>{function ue(){if(k.forEach(function(ye){let ge=Ae.get(ye).currentProgram;(ge===void 0||ge.isReady())&&k.delete(ye)}),k.size===0){B(_);return}setTimeout(ue,10)}Mt.get("KHR_parallel_shader_compile")!==null?ue():setTimeout(ue,10)})};let of=null;function YM(_){of&&of(_)}function wg(){Fr.stop()}function Ig(){Fr.start()}let Fr=new wM;Fr.setAnimationLoop(YM),typeof self<"u"&&Fr.setContext(self),this.setAnimationLoop=function(_){of=_,ne.setAnimationLoop(_),_===null?Fr.stop():Fr.start()},ne.addEventListener("sessionstart",wg),ne.addEventListener("sessionend",Ig),this.render=function(_,R){if(R!==void 0&&R.isCamera!==!0){Ne("WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(L===!0)return;F!==null&&F.renderStart(_,R);let V=ne.enabled===!0&&ne.isPresenting===!0,k=N!==null&&(te===null||V)&&N.begin(A,te);if(_.matrixWorldAutoUpdate===!0&&_.updateMatrixWorld(),R.parent===null&&R.matrixWorldAutoUpdate===!0&&R.updateMatrixWorld(),ne.enabled===!0&&ne.isPresenting===!0&&(N===null||N.isCompositing()===!1)&&(ne.cameraAutoUpdate===!0&&ne.updateCamera(R),R=ne.getCamera()),_.isScene===!0&&_.onBeforeRender(A,_,R,te),T=q.get(_,S.length),T.init(R),T.state.textureUnits=Oe.getTextureUnits(),S.push(T),bt.multiplyMatrices(R.projectionMatrix,R.matrixWorldInverse),R.isArrayCamera?Yt.setFromArrayCamera(R):qe.setFromProjectionMatrix(bt,Xn,R.reversedDepth),vn=this.localClippingEnabled,kt=X.init(this.clippingPlanes,vn),I=le.get(_,y.length),I.init(),y.push(I),this.info.render.frame++,ne.enabled===!0&&ne.isPresenting===!0){let ye=A.xr.getDepthSensingMesh();ye!==null&&af(ye,R,-1/0,A.sortObjects)}af(_,R,0,A.sortObjects),I.finish(),F!==null&&F.updateLights(T.state.lightsArray),A.sortObjects===!0&&I.sort(Ue,Rt),Un=ne.enabled===!1||ne.isPresenting===!1||ne.hasDepthSensing()===!1,Un&&C.addToRenderList(I,_),this.info.autoReset===!0&&this.info.reset(),kt===!0&&X.beginShadows();let B=T.state.shadowsArray;if(oe.render(B,_,R),kt===!0&&X.endShadows(),(k&&N.hasRenderPass())===!1){let ye=I.opaque,ce=I.transmissive;if(T.setupLights(),R.isArrayCamera){let ge=R.cameras;if(ce.length>0)for(let Se=0,ke=ge.length;Se<ke;Se++){let Xe=ge[Se];Ag(ye,ce,_,Xe)}Un&&C.render(_);for(let Se=0,ke=ge.length;Se<ke;Se++){let Xe=ge[Se];Cg(I,_,Xe,Xe.viewport)}}else ce.length>0&&Ag(ye,ce,_,R),Un&&C.render(_),Cg(I,_,R)}te!==null&&H===0&&(Oe.updateMultisampleRenderTarget(te),Oe.updateRenderTargetMipmap(te)),k&&N.end(A),_.isScene===!0&&_.onAfterRender(A,_,R),se.resetDefaultState(),j=-1,J=null,S.pop(),S.length>0?(T=S[S.length-1],Oe.setTextureUnits(T.state.textureUnits),kt===!0&&X.setGlobalState(A.clippingPlanes,T.state.camera)):T=null,y.pop(),y.length>0?I=y[y.length-1]:I=null,F!==null&&F.renderEnd()};function af(_,R,V,k){if(_.visible===!1)return;if(_.layers.test(R.layers)){if(_.isGroup)V=_.renderOrder;else if(_.isLOD)_.autoUpdate===!0&&_.update(R);else if(_.isLight)T.pushLight(_),_.castShadow&&T.pushShadow(_);else if(_.isSprite){let ye=R.isArrayCamera?Yt:qe;if(!_.frustumCulled||_.intersectsFrustum(ye)){k&&zt.setFromMatrixPosition(_.matrixWorld).applyMatrix4(bt);let ce=ee.update(_),ge=_.material;ge.visible&&I.push(_,ce,ge,V,zt.z,null,R)}}else if(_.isMesh||_.isLine||_.isPoints){let ye=R.isArrayCamera?Yt:qe;if(!_.frustumCulled||_.intersectsFrustum(ye)){let ce=ee.update(_),ge=_.material;if(k&&(_.boundingSphere!==void 0?(_.boundingSphere===null&&_.computeBoundingSphere(),zt.copy(_.boundingSphere.center)):(ce.boundingSphere===null&&ce.computeBoundingSphere(),zt.copy(ce.boundingSphere.center)),zt.applyMatrix4(_.matrixWorld).applyMatrix4(bt)),Array.isArray(ge)){let Se=ce.groups;for(let ke=0,Xe=Se.length;ke<Xe;ke++){let _e=Se[ke],Ye=ge[_e.materialIndex];Ye&&Ye.visible&&I.push(_,ce,Ye,V,zt.z,_e,R)}}else ge.visible&&I.push(_,ce,ge,V,zt.z,null,R)}}}let ue=_.children;for(let ye=0,ce=ue.length;ye<ce;ye++)af(ue[ye],R,V,k)}function Cg(_,R,V,k){let{opaque:B,transmissive:ue,transparent:ye}=_;T.setupLightsView(V),kt===!0&&X.setGlobalState(A.clippingPlanes,V),k&&de.viewport(Q.copy(k)),B.length>0&&Mc(B,R,V),ue.length>0&&Mc(ue,R,V),ye.length>0&&Mc(ye,R,V),de.buffers.depth.setTest(!0),de.buffers.depth.setMask(!0),de.buffers.color.setMask(!0),de.setPolygonOffset(!1)}function Ag(_,R,V,k){if((V.isScene===!0?V.overrideMaterial:null)!==null)return;if(T.state.transmissionRenderTarget[k.id]===void 0){let _e=Mt.has("EXT_color_buffer_half_float")||Mt.has("EXT_color_buffer_float");T.state.transmissionRenderTarget[k.id]=new On(1,1,{generateMipmaps:!0,type:_e?fi:En,minFilter:ir,samples:Math.max(4,St.samples),stencilBuffer:s,resolveDepthBuffer:!1,resolveStencilBuffer:!1,storeMultisampledDepthBuffer:!1,storeMultisampledStencilBuffer:!1,colorSpace:Ke.workingColorSpace})}let ue=T.state.transmissionRenderTarget[k.id],ye=k.viewport||Q;ue.setSize(ye.z*A.transmissionResolutionScale,ye.w*A.transmissionResolutionScale);let ce=A.getRenderTarget(),ge=A.getActiveCubeFace(),Se=A.getActiveMipmapLevel();A.setRenderTarget(ue),A.getClearColor(Y),pe=A.getClearAlpha(),pe<1&&A.setClearColor(16777215,.5),A.clear(),Un&&C.render(V);let ke=A.toneMapping;A.toneMapping=Ni;let Xe=k.viewport;if(k.viewport!==void 0&&(k.viewport=void 0),T.setupLightsView(k),kt===!0&&X.setGlobalState(A.clippingPlanes,k),Mc(_,V,k),Oe.updateMultisampleRenderTarget(ue),Oe.updateRenderTargetMipmap(ue),Mt.has("WEBGL_multisampled_render_to_texture")===!1){let _e=!1;for(let Ye=0,Nt=R.length;Ye<Nt;Ye++){let yt=R[Ye],{object:mt,geometry:nn,material:xe,group:un}=yt;if(xe.side===Ri&&mt.layers.test(k.layers)){let nt=xe.side;xe.side=gn,xe.needsUpdate=!0,Dg(mt,V,k,nn,xe,un),xe.side=nt,xe.needsUpdate=!0,_e=!0}}_e===!0&&(Oe.updateMultisampleRenderTarget(ue),Oe.updateRenderTargetMipmap(ue))}A.setRenderTarget(ce,ge,Se),A.setClearColor(Y,pe),Xe!==void 0&&(k.viewport=Xe),A.toneMapping=ke}function Mc(_,R,V){let k=R.isScene===!0?R.overrideMaterial:null;for(let B=0,ue=_.length;B<ue;B++){let ye=_[B],{object:ce,geometry:ge,group:Se}=ye,ke=ye.material;ke.allowOverride===!0&&k!==null&&(ke=k),ce.layers.test(V.layers)&&Dg(ce,R,V,ge,ke,Se)}}function Dg(_,R,V,k,B,ue){F!==null&&B.isNodeMaterial&&F.setObject(_,B),_.onBeforeRender(A,R,V,k,B,ue),_.modelViewMatrix.multiplyMatrices(V.matrixWorldInverse,_.matrixWorld),_.normalMatrix.getNormalMatrix(_.modelViewMatrix),B.onBeforeRender(A,R,V,k,_,ue),B.transparent===!0&&B.side===Ri&&B.forceSinglePass===!1?(B.side=gn,B.needsUpdate=!0,A.renderBufferDirect(V,R,k,B,_,ue),B.side=Pr,B.needsUpdate=!0,A.renderBufferDirect(V,R,k,B,_,ue),B.side=Ri):A.renderBufferDirect(V,R,k,B,_,ue),_.onAfterRender(A,R,V,k,B,ue)}function Rg(_){let R=0;for(let V of _)V.texture!==null&&R++;return R}function Uo(_,R,V){R.isScene!==!0&&(R=kn);let k=Ae.get(_),B=T.state.lights,ue=T.state.shadowsArray,ye=B.state.version,ce=K.getParameters(_,B.state,ue,R,V,T.state.lightProbeGridArray),ge=K.getProgramCacheKey(ce),Se=k.programs;k.environment=_.isMeshStandardMaterial||_.isMeshLambertMaterial||_.isMeshPhongMaterial?R.environment:null,k.fog=R.fog;let ke=_.isMeshStandardMaterial||_.isMeshLambertMaterial&&!_.envMap||_.isMeshPhongMaterial&&!_.envMap;k.envMap=b.get(_.envMap||k.environment,ke),k.envMapRotation=k.environment!==null&&_.envMap===null?R.environmentRotation:_.envMapRotation,Se===void 0&&(_.addEventListener("dispose",ct),Se=new Map,k.programs=Se,k.programVariants=new Map);let Xe=Se.get(ge);if(Xe!==void 0){if(k.currentProgram===Xe&&k.lightsStateVersion===ye)return Pg(_,ce),Xe}else ce.uniforms=K.getUniforms(_),F!==null&&_.isNodeMaterial&&F.build(_,V,ce),_.onBeforeCompile(ce,A),Xe=K.acquireProgram(ce,ge),Se.set(ge,Xe),k.uniforms=ce.uniforms;let _e=k.uniforms;return(!_.isShaderMaterial&&!_.isRawShaderMaterial||_.clipping===!0)&&(_e.clippingPlanes=X.uniform),Pg(_,ce),k.needsLights=QM(_),k.lightsStateVersion=ye,k.needsLights&&(_e.ambientLightColor.value=B.state.ambient,_e.lightProbe.value=B.state.probe,_e.sunLights.value=B.state.sun,_e.sunLightShadows.value=B.state.sunShadow,_e.directionalLights.value=B.state.directional,_e.directionalLightShadows.value=B.state.directionalShadow,_e.spotLights.value=B.state.spot,_e.spotLightShadows.value=B.state.spotShadow,_e.rectAreaLights.value=B.state.rectArea,_e.ltc_1.value=B.state.rectAreaLTC1,_e.ltc_2.value=B.state.rectAreaLTC2,_e.pointLights.value=B.state.point,_e.pointLightShadows.value=B.state.pointShadow,_e.hemisphereLights.value=B.state.hemi,_e.sunShadowMatrix.value=B.state.sunShadowMatrix,_e.sunShadowCascade.value=B.state.sunShadowCascade,_e.directionalShadowMatrix.value=B.state.directionalShadowMatrix,_e.spotLightMatrix.value=B.state.spotLightMatrix,_e.spotLightMap.value=B.state.spotLightMap,_e.pointShadowMatrix.value=B.state.pointShadowMatrix),k.numLightProbeGrids=Rg(T.state.lightProbeGridArray),k.currentProgram=Xe,k.uniformsList=null,Xe}function ZM(_,R,V,k,B){let ue=Ae.get(_),ye=ue.currentProgram.id+","+k+","+B,ce=ue.programVariants.get(ye);return ce===void 0?(ce=Uo(_,R,V),ue.programVariants.set(ye,ce)):(ue.currentProgram=ce,ue.uniformsList=null,ue.outputColorSpace=k,ue.toneMapping=B),ce}function Ng(_){if(_.uniformsList===null){let R=_.currentProgram.getUniforms();_.uniformsList=Oo.seqWithValue(R.seq,_.uniforms)}return _.uniformsList}function Pg(_,R){let V=Ae.get(_);V.outputColorSpace=R.outputColorSpace,V.batching=R.batching,V.batchingColor=R.batchingColor,V.instancing=R.instancing,V.instancingColor=R.instancingColor,V.instancingMorph=R.instancingMorph,V.skinning=R.skinning,V.morphTargets=R.morphTargets,V.morphNormals=R.morphNormals,V.morphColors=R.morphColors,V.morphTargetsCount=R.morphTargetsCount,V.numClippingPlanes=R.numClippingPlanes,V.numIntersection=R.numClipIntersection,V.vertexAlphas=R.vertexAlphas,V.vertexTangents=R.vertexTangents,V.toneMapping=R.toneMapping}function KM(_,R,V,k,B){R.isScene!==!0&&(R=kn),Oe.resetTextureUnits();let ue=R.fog,ye=k.isMeshStandardMaterial||k.isMeshLambertMaterial||k.isMeshPhongMaterial?R.environment:null,ce=te===null?A.outputColorSpace:te.isXRRenderTarget===!0?te.texture.colorSpace:Ke.workingColorSpace,ge=k.isMeshStandardMaterial||k.isMeshLambertMaterial&&!k.envMap||k.isMeshPhongMaterial&&!k.envMap,Se=b.get(k.envMap||ye,ge),ke=k.vertexColors===!0&&!!V.attributes.color&&V.attributes.color.itemSize===4,Xe=!!V.attributes.tangent&&(!!k.normalMap||k.anisotropy>0),_e=!!V.morphAttributes.position,Ye=!!V.morphAttributes.normal,Nt=!!V.morphAttributes.color,yt=Ni;k.toneMapped&&(te===null||te.isXRRenderTarget===!0)&&(yt=A.toneMapping);let mt=V.morphAttributes.position||V.morphAttributes.normal||V.morphAttributes.color,nn=mt!==void 0?mt.length:0,xe=Ae.get(k),un=T.state.lights;if(kt===!0&&(vn===!0||_!==J)){let Tt=_===J&&k.id===j;X.setState(k,_,Tt)}let nt=!1,kr=!1;k.version===xe.__version?xe.needsLights&&xe.lightsStateVersion!==un.state.version||B.isBatchedMesh&&xe.batching===!1||!B.isBatchedMesh&&xe.batching===!0||B.isBatchedMesh&&xe.batchingColor===!0&&B._colorsTexture===null||B.isBatchedMesh&&xe.batchingColor===!1&&B._colorsTexture!==null||B.isInstancedMesh&&xe.instancing===!1||!B.isInstancedMesh&&xe.instancing===!0||B.isSkinnedMesh&&xe.skinning===!1||!B.isSkinnedMesh&&xe.skinning===!0||B.isInstancedMesh&&xe.instancingColor===!0&&B.instanceColor===null||B.isInstancedMesh&&xe.instancingColor===!1&&B.instanceColor!==null||B.isInstancedMesh&&xe.instancingMorph===!0&&B.morphTexture===null||B.isInstancedMesh&&xe.instancingMorph===!1&&B.morphTexture!==null||xe.envMap!==Se||k.fog===!0&&xe.fog!==ue||xe.numClippingPlanes!==void 0&&(xe.numClippingPlanes!==X.numPlanes||xe.numIntersection!==X.numIntersection)||xe.vertexAlphas!==ke||xe.vertexTangents!==Xe||xe.morphTargets!==_e||xe.morphNormals!==Ye||xe.morphColors!==Nt||xe.morphTargetsCount!==nn||xe.numLightProbeGrids!==Rg(T.state.lightProbeGridArray)?nt=!0:(xe.outputColorSpace!==ce||xe.toneMapping!==yt)&&(kr=!0):(nt=!0,xe.__version=k.version);let rn=xe.currentProgram;(nt===!0||kr===!0)&&(rn=kr===!0?ZM(k,R,B,ce,yt):Uo(k,R,B),F&&k.isNodeMaterial&&F.onUpdateProgram(k,rn,xe));let Sc=!1,ar=!1,Ur=!1,Qe=rn.getUniforms(),Zt=xe.uniforms;if(de.useProgram(rn.program)&&(Sc=!0,ar=!0,Ur=!0),k.id!==j&&(j=k.id,ar=!0),Sc||J!==_){de.buffers.depth.getReversed()&&_.reversedDepth!==!0&&(_._reversedDepth=!0,_.updateProjectionMatrix()),Qe.setValue(P,"projectionMatrix",_.projectionMatrix),Qe.setValue(P,"viewMatrix",_.matrixWorldInverse);let Wt=Qe.map.cameraPosition;Wt!==void 0&&Wt.setValue(P,pi.setFromMatrixPosition(_.matrixWorld)),St.logarithmicDepthBuffer&&Qe.setValue(P,"logDepthBufFC",2/(Math.log(_.far+1)/Math.LN2)),(k.isMeshPhongMaterial||k.isMeshToonMaterial||k.isMeshLambertMaterial||k.isMeshBasicMaterial||k.isMeshStandardMaterial||k.isShaderMaterial)&&Qe.setValue(P,"isOrthographic",_.isOrthographicCamera===!0),J!==_&&(J=_,ar=!0,Ur=!0)}if(xe.needsLights&&(un.state.sunShadowMap.length>0&&Qe.setValue(P,"sunShadowMap",un.state.sunShadowMap,Oe),un.state.directionalShadowMap.length>0&&Qe.setValue(P,"directionalShadowMap",un.state.directionalShadowMap,Oe),un.state.spotShadowMap.length>0&&Qe.setValue(P,"spotShadowMap",un.state.spotShadowMap,Oe),un.state.pointShadowMap.length>0&&Qe.setValue(P,"pointShadowMap",un.state.pointShadowMap,Oe)),B.isSkinnedMesh){Qe.setOptional(P,B,"bindMatrix"),Qe.setOptional(P,B,"bindMatrixInverse");let Tt=B.skeleton;Tt&&(Tt.boneTexture===null&&Tt.computeBoneTexture(),Qe.setValue(P,"boneTexture",Tt.boneTexture,Oe))}B.isBatchedMesh&&(Qe.setOptional(P,B,"batchingTexture"),Qe.setValue(P,"batchingTexture",B._matricesTexture,Oe),Qe.setOptional(P,B,"batchingIdTexture"),Qe.setValue(P,"batchingIdTexture",B._indirectTexture,Oe),Qe.setOptional(P,B,"batchingColorTexture"),B._colorsTexture!==null&&Qe.setValue(P,"batchingColorTexture",B._colorsTexture,Oe));let Kn=V.morphAttributes;if((Kn.position!==void 0||Kn.normal!==void 0||Kn.color!==void 0)&&Z.update(B,V,rn),(ar||xe.receiveShadow!==B.receiveShadow)&&(xe.receiveShadow=B.receiveShadow,Qe.setValue(P,"receiveShadow",B.receiveShadow)),(k.isMeshStandardMaterial||k.isMeshLambertMaterial||k.isMeshPhongMaterial)&&k.envMap===null&&R.environment!==null&&(Zt.envMapIntensity.value=R.environmentIntensity),Zt.dfgLUT!==void 0&&(Zt.dfgLUT.value=sN()),ar){if(Qe.setValue(P,"toneMappingExposure",A.toneMappingExposure),xe.needsLights&&JM(Zt,Ur),ue&&k.fog===!0&&re.refreshFogUniforms(Zt,ue),re.refreshMaterialUniforms(Zt,k,me,Ce,T.state.transmissionRenderTarget[_.id]),xe.needsLights&&xe.numLightProbeGrids>0){let Tt=E,Wt=0;for(let Oi of T.state.lightProbeGridArray)Oi.texture!==null&&(Tt.textures[Wt]=Oi.texture,Tt.min[Wt]=Oi.boundingBox.min,Tt.max[Wt]=Oi.boundingBox.max,Tt.resolution[Wt]=Oi.resolution,Wt++);for(let Oi in Tt)Tt[Oi].length=Wt;Zt.probesSH.value=Tt.textures,Zt.probesMin.value=Tt.min,Zt.probesMax.value=Tt.max,Zt.probesResolution.value=Tt.resolution}Oo.upload(P,Ng(xe),Zt,Oe)}if(k.isShaderMaterial&&k.uniformsNeedUpdate===!0&&(Oo.upload(P,Ng(xe),Zt,Oe),k.uniformsNeedUpdate=!1),k.isSpriteMaterial&&Qe.setValue(P,"center",B.center),Qe.setValue(P,"modelViewMatrix",B.modelViewMatrix),Qe.setValue(P,"normalMatrix",B.normalMatrix),Qe.setValue(P,"modelMatrix",B.matrixWorld),k.uniformsGroups!==void 0){let Tt=k.uniformsGroups;for(let Wt=0,Oi=Tt.length;Wt<Oi;Wt++){let Og=Tt[Wt];Ee.update(Og,rn),Ee.bind(Og,rn)}}return rn}function JM(_,R){_.ambientLightColor.needsUpdate=R,_.lightProbe.needsUpdate=R,_.sunLights.needsUpdate=R,_.sunLightShadows.needsUpdate=R,_.directionalLights.needsUpdate=R,_.directionalLightShadows.needsUpdate=R,_.pointLights.needsUpdate=R,_.pointLightShadows.needsUpdate=R,_.spotLights.needsUpdate=R,_.spotLightShadows.needsUpdate=R,_.rectAreaLights.needsUpdate=R,_.hemisphereLights.needsUpdate=R}function QM(_){return _.isMeshLambertMaterial||_.isMeshToonMaterial||_.isMeshPhongMaterial||_.isMeshStandardMaterial||_.isShadowMaterial||_.isShaderMaterial&&_.lights===!0}this.getActiveCubeFace=function(){return $},this.getActiveMipmapLevel=function(){return H},this.getRenderTarget=function(){return te},this.setRenderTargetTextures=function(_,R,V){let k=Ae.get(_);k.__autoAllocateDepthBuffer=_.resolveDepthBuffer===!1,k.__autoAllocateDepthBuffer===!1&&(k.__useRenderToTexture=!1),Ae.get(_.texture).__webglTexture=R,Ae.get(_.depthTexture).__webglTexture=k.__autoAllocateDepthBuffer?void 0:V,k.__hasExternalTextures=!0},this.setRenderTargetFramebuffer=function(_,R){let V=Ae.get(_);V.__webglFramebuffer=R,V.__useDefaultFramebuffer=R===void 0},this.setRenderTarget=function(_,R=0,V=0){te=_,$=R,H=V;let k=null,B=!1,ue=!1;if(_){let ce=Ae.get(_);if(ce.__useDefaultFramebuffer!==void 0){de.bindFramebuffer(P.FRAMEBUFFER,ce.__webglFramebuffer),Q.copy(_.viewport),Me.copy(_.scissor),Fe=_.scissorTest,de.viewport(Q),de.scissor(Me),de.setScissorTest(Fe),j=-1;return}else if(ce.__webglFramebuffer===void 0)Oe.setupRenderTarget(_);else if(ce.__hasExternalTextures)Oe.rebindTextures(_,Ae.get(_.texture).__webglTexture,Ae.get(_.depthTexture).__webglTexture);else if(_.depthBuffer){let ke=_.depthTexture;if(ce.__boundDepthTexture!==ke){if(ke!==null&&Ae.has(ke)&&(_.width!==ke.image.width||_.height!==ke.image.height))throw new Error("THREE.WebGLRenderer: Attached DepthTexture is initialized to the incorrect size.");Oe.setupDepthRenderbuffer(_)}}let ge=_.texture;(ge.isData3DTexture||ge.isDataArrayTexture||ge.isCompressedArrayTexture)&&(ue=!0);let Se=Ae.get(_).__webglFramebuffer;_.isWebGLCubeRenderTarget?(Array.isArray(Se[R])?k=Se[R][V]:k=Se[R],B=!0):_.samples>0&&Oe.useMultisampledRTT(_)===!1?k=Ae.get(_).__webglMultisampledFramebuffer:Array.isArray(Se)?k=Se[V]:k=Se,Q.copy(_.viewport),Me.copy(_.scissor),Fe=_.scissorTest}else Q.copy(Re).multiplyScalar(me).floor(),Me.copy(at).multiplyScalar(me).floor(),Fe=pt;if(V!==0&&(k=G),de.bindFramebuffer(P.FRAMEBUFFER,k)&&de.drawBuffers(_,k),de.viewport(Q),de.scissor(Me),de.setScissorTest(Fe),B){let ce=Ae.get(_.texture);P.framebufferTexture2D(P.FRAMEBUFFER,P.COLOR_ATTACHMENT0,P.TEXTURE_CUBE_MAP_POSITIVE_X+R,ce.__webglTexture,V)}else if(ue){let ce=R;for(let ge=0;ge<_.textures.length;ge++){let Se=Ae.get(_.textures[ge]);P.framebufferTextureLayer(P.FRAMEBUFFER,P.COLOR_ATTACHMENT0+ge,Se.__webglTexture,V,ce)}}else if(_!==null&&V!==0){let ce=Ae.get(_.texture);P.framebufferTexture2D(P.FRAMEBUFFER,P.COLOR_ATTACHMENT0,P.TEXTURE_2D,ce.__webglTexture,V)}j=-1};function Lg(_){let R=Ae.get(_);return(R.__readFormat!==_.format||R.__readType!==_.type)&&(R.__readFormat=_.format,R.__readType=_.type,R.__formatReadable=St.textureFormatReadable(_.format),R.__typeReadable=St.textureTypeReadable(_.type)),R}this.readRenderTargetPixels=function(_,R,V,k,B,ue,ye,ce=0){if(!(_&&_.isWebGLRenderTarget)){Ne("WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let ge=Ae.get(_).__webglFramebuffer;if(_.isWebGLCubeRenderTarget&&ye!==void 0&&(ge=ge[ye],Array.isArray(ge)&&(ge=ge[0])),ge){de.bindFramebuffer(P.FRAMEBUFFER,ge);try{let Se=_.textures[ce],ke=Se.format,Xe=Se.type;_.textures.length>1&&P.readBuffer(P.COLOR_ATTACHMENT0+ce);let _e=Lg(Se);if(_e.__formatReadable===!1){Ne("WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(_e.__typeReadable===!1){Ne("WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}R>=0&&R<=_.width-k&&V>=0&&V<=_.height-B&&P.readPixels(R,V,k,B,ie.convert(ke),ie.convert(Xe),ue)}finally{let Se=te!==null?Ae.get(te).__webglFramebuffer:null;de.bindFramebuffer(P.FRAMEBUFFER,Se)}}},this.readRenderTargetPixelsAsync=async function(_,R,V,k,B,ue,ye,ce=0){if(!(_&&_.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let ge=Ae.get(_).__webglFramebuffer;if(_.isWebGLCubeRenderTarget&&ye!==void 0&&(ge=ge[ye],Array.isArray(ge)&&(ge=ge[0])),ge)if(R>=0&&R<=_.width-k&&V>=0&&V<=_.height-B){de.bindFramebuffer(P.FRAMEBUFFER,ge);let Se=_.textures[ce],ke=Se.format,Xe=Se.type;_.textures.length>1&&P.readBuffer(P.COLOR_ATTACHMENT0+ce);let _e=Lg(Se);if(_e.__formatReadable===!1)throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(_e.__typeReadable===!1)throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");let Ye=P.createBuffer();P.bindBuffer(P.PIXEL_PACK_BUFFER,Ye),P.bufferData(P.PIXEL_PACK_BUFFER,ue.byteLength,P.STREAM_READ),P.readPixels(R,V,k,B,ie.convert(ke),ie.convert(Xe),0),P.bindBuffer(P.PIXEL_PACK_BUFFER,null);let Nt=te!==null?Ae.get(te).__webglFramebuffer:null;de.bindFramebuffer(P.FRAMEBUFFER,Nt);let yt=P.fenceSync(P.SYNC_GPU_COMMANDS_COMPLETE,0);return P.flush(),await Kx(P,yt,4),P.bindBuffer(P.PIXEL_PACK_BUFFER,Ye),P.getBufferSubData(P.PIXEL_PACK_BUFFER,0,ue),P.bindBuffer(P.PIXEL_PACK_BUFFER,null),P.deleteBuffer(Ye),P.deleteSync(yt),ue}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")},this.copyFramebufferToTexture=function(_,R=null,V=0){let k=Math.pow(2,-V),B=Math.floor(_.image.width*k),ue=Math.floor(_.image.height*k),ye=R!==null?R.x:0,ce=R!==null?R.y:0;Oe.setTexture2D(_,0),P.copyTexSubImage2D(P.TEXTURE_2D,V,0,0,ye,ce,B,ue),de.unbindTexture()},this.copyTextureToTexture=function(_,R,V=null,k=null,B=0,ue=0){let ye,ce,ge,Se,ke,Xe,_e,Ye,Nt,yt=_.isCompressedTexture?_.mipmaps[ue]:_.image;if(V!==null)ye=V.max.x-V.min.x,ce=V.max.y-V.min.y,ge=V.isBox3?V.max.z-V.min.z:1,Se=V.min.x,ke=V.min.y,Xe=V.isBox3?V.min.z:0;else{let Qe=Math.pow(2,-B);ye=Math.floor(yt.width*Qe),ce=Math.floor(yt.height*Qe),_.isDataArrayTexture?ge=yt.depth:_.isData3DTexture?ge=Math.floor(yt.depth*Qe):ge=1,Se=0,ke=0,Xe=0}k!==null?(_e=k.x,Ye=k.y,Nt=k.z):(_e=0,Ye=0,Nt=0);let mt=ie.convert(R.format),nn=ie.convert(R.type),xe;R.isData3DTexture?(Oe.setTexture3D(R,0),xe=P.TEXTURE_3D):R.isDataArrayTexture||R.isCompressedArrayTexture?(Oe.setTexture2DArray(R,0),xe=P.TEXTURE_2D_ARRAY):(Oe.setTexture2D(R,0),xe=P.TEXTURE_2D),de.activeTexture(P.TEXTURE0),de.pixelStorei(P.UNPACK_FLIP_Y_WEBGL,R.flipY),de.pixelStorei(P.UNPACK_PREMULTIPLY_ALPHA_WEBGL,R.premultiplyAlpha),de.pixelStorei(P.UNPACK_ALIGNMENT,R.unpackAlignment);let un=de.getParameter(P.UNPACK_ROW_LENGTH),nt=de.getParameter(P.UNPACK_IMAGE_HEIGHT),kr=de.getParameter(P.UNPACK_SKIP_PIXELS),rn=de.getParameter(P.UNPACK_SKIP_ROWS),Sc=de.getParameter(P.UNPACK_SKIP_IMAGES);de.pixelStorei(P.UNPACK_ROW_LENGTH,yt.width),de.pixelStorei(P.UNPACK_IMAGE_HEIGHT,yt.height),de.pixelStorei(P.UNPACK_SKIP_PIXELS,Se),de.pixelStorei(P.UNPACK_SKIP_ROWS,ke),de.pixelStorei(P.UNPACK_SKIP_IMAGES,Xe);let ar=_.isDataArrayTexture||_.isData3DTexture,Ur=R.isDataArrayTexture||R.isData3DTexture;if(_.isDepthTexture){let Qe=Ae.get(_),Zt=Ae.get(R),Kn=Ae.get(Qe.__renderTarget),Tt=Ae.get(Zt.__renderTarget);de.bindFramebuffer(P.READ_FRAMEBUFFER,Kn.__webglFramebuffer),de.bindFramebuffer(P.DRAW_FRAMEBUFFER,Tt.__webglFramebuffer);for(let Wt=0;Wt<ge;Wt++)ar&&(P.framebufferTextureLayer(P.READ_FRAMEBUFFER,P.COLOR_ATTACHMENT0,Ae.get(_).__webglTexture,B,Xe+Wt),P.framebufferTextureLayer(P.DRAW_FRAMEBUFFER,P.COLOR_ATTACHMENT0,Ae.get(R).__webglTexture,ue,Nt+Wt)),P.blitFramebuffer(Se,ke,Se+ye,ke+ce,_e,Ye,_e+ye,Ye+ce,P.DEPTH_BUFFER_BIT,P.NEAREST);de.bindFramebuffer(P.READ_FRAMEBUFFER,null),de.bindFramebuffer(P.DRAW_FRAMEBUFFER,null)}else if(B!==0||_.isRenderTargetTexture||Ae.has(_)){let Qe=Ae.get(_),Zt=Ae.get(R);de.bindFramebuffer(P.READ_FRAMEBUFFER,D),de.bindFramebuffer(P.DRAW_FRAMEBUFFER,z);for(let Kn=0;Kn<ge;Kn++)ar?P.framebufferTextureLayer(P.READ_FRAMEBUFFER,P.COLOR_ATTACHMENT0,Qe.__webglTexture,B,Xe+Kn):P.framebufferTexture2D(P.READ_FRAMEBUFFER,P.COLOR_ATTACHMENT0,P.TEXTURE_2D,Qe.__webglTexture,B),Ur?P.framebufferTextureLayer(P.DRAW_FRAMEBUFFER,P.COLOR_ATTACHMENT0,Zt.__webglTexture,ue,Nt+Kn):P.framebufferTexture2D(P.DRAW_FRAMEBUFFER,P.COLOR_ATTACHMENT0,P.TEXTURE_2D,Zt.__webglTexture,ue),B!==0?P.blitFramebuffer(Se,ke,Se+ye,ke+ce,_e,Ye,_e+ye,Ye+ce,P.COLOR_BUFFER_BIT,P.NEAREST):Ur?P.copyTexSubImage3D(xe,ue,_e,Ye,Nt+Kn,Se,ke,ye,ce):P.copyTexSubImage2D(xe,ue,_e,Ye,Se,ke,ye,ce);de.bindFramebuffer(P.READ_FRAMEBUFFER,null),de.bindFramebuffer(P.DRAW_FRAMEBUFFER,null)}else Ur?_.isDataTexture||_.isData3DTexture?P.texSubImage3D(xe,ue,_e,Ye,Nt,ye,ce,ge,mt,nn,yt.data):R.isCompressedArrayTexture?P.compressedTexSubImage3D(xe,ue,_e,Ye,Nt,ye,ce,ge,mt,yt.data):P.texSubImage3D(xe,ue,_e,Ye,Nt,ye,ce,ge,mt,nn,yt):_.isDataTexture?P.texSubImage2D(P.TEXTURE_2D,ue,_e,Ye,ye,ce,mt,nn,yt.data):_.isCompressedTexture?P.compressedTexSubImage2D(P.TEXTURE_2D,ue,_e,Ye,yt.width,yt.height,mt,yt.data):P.texSubImage2D(P.TEXTURE_2D,ue,_e,Ye,ye,ce,mt,nn,yt);de.pixelStorei(P.UNPACK_ROW_LENGTH,un),de.pixelStorei(P.UNPACK_IMAGE_HEIGHT,nt),de.pixelStorei(P.UNPACK_SKIP_PIXELS,kr),de.pixelStorei(P.UNPACK_SKIP_ROWS,rn),de.pixelStorei(P.UNPACK_SKIP_IMAGES,Sc),ue===0&&R.generateMipmaps===!0&&R.mipmapsAutoUpdate===!0&&P.generateMipmap(xe),de.unbindTexture()},this.initRenderTarget=function(_){Ae.get(_).__webglFramebuffer===void 0&&Oe.setupRenderTarget(_)},this.initTexture=function(_){_.isCubeTexture?Oe.setTextureCube(_,0):_.isData3DTexture?Oe.setTexture3D(_,0):_.isDataArrayTexture||_.isCompressedArrayTexture?Oe.setTexture2DArray(_,0):Oe.setTexture2D(_,0),de.unbindTexture()},this.resetState=function(){$=0,H=0,te=null,de.reset(),se.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return Xn}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorSpace=Ke._getDrawingBufferColorSpace(e),t.unpackColorSpace=Ke._getUnpackColorSpace()}};var kM={type:"change"},_g={type:"start"},BM={type:"end"},Qd=new Ir,UM=new fn,oN=Math.cos(70*ng.DEG2RAD),Ht=new O,bn=2*Math.PI,st={NONE:-1,ROTATE:0,DOLLY:1,PAN:2,TOUCH_ROTATE:3,TOUCH_PAN:4,TOUCH_DOLLY_PAN:5,TOUCH_DOLLY_ROTATE:6},vg=1e-6,ef=class extends ac{constructor(e,t=null){super(e,t),this.state=st.NONE,this.target=new O,this.cursor=new O,this.minDistance=0,this.maxDistance=1/0,this.minZoom=0,this.maxZoom=1/0,this.minTargetRadius=0,this.maxTargetRadius=1/0,this.minPolarAngle=0,this.maxPolarAngle=Math.PI,this.minAzimuthAngle=-1/0,this.maxAzimuthAngle=1/0,this.enableDamping=!1,this.dampingFactor=.05,this.enableZoom=!0,this.zoomSpeed=1,this.enableRotate=!0,this.rotateSpeed=1,this.keyRotateSpeed=1,this.enablePan=!0,this.panSpeed=1,this.screenSpacePanning=!0,this.keyPanSpeed=7,this.zoomToCursor=!1,this.autoRotate=!1,this.autoRotateSpeed=2,this.keys={LEFT:"ArrowLeft",UP:"ArrowUp",RIGHT:"ArrowRight",BOTTOM:"ArrowDown"},this.mouseButtons={LEFT:Yn.ROTATE,MIDDLE:Yn.DOLLY,RIGHT:Yn.PAN},this.touches={ONE:li.ROTATE,TWO:li.DOLLY_PAN},this.target0=this.target.clone(),this.position0=this.object.position.clone(),this.zoom0=this.object.zoom,this._cursorStyle="auto",this._domElementKeyEvents=null,this._lastPosition=new O,this._lastQuaternion=new Ln,this._lastTargetPosition=new O,this._quat=new Ln().setFromUnitVectors(e.up,new O(0,1,0)),this._quatInverse=this._quat.clone().invert(),this._spherical=new Ao,this._sphericalDelta=new Ao,this._scale=1,this._panOffset=new O,this._rotateStart=new Te,this._rotateEnd=new Te,this._rotateDelta=new Te,this._panStart=new Te,this._panEnd=new Te,this._panDelta=new Te,this._dollyStart=new Te,this._dollyEnd=new Te,this._dollyDelta=new Te,this._dollyDirection=new O,this._mouse=new Te,this._performCursorZoom=!1,this._pointers=[],this._pointerPositions={},this._controlActive=!1,this._onPointerMove=cN.bind(this),this._onPointerDown=aN.bind(this),this._onPointerUp=lN.bind(this),this._onContextMenu=gN.bind(this),this._onMouseWheel=fN.bind(this),this._onKeyDown=hN.bind(this),this._onTouchStart=pN.bind(this),this._onTouchMove=mN.bind(this),this._onMouseDown=uN.bind(this),this._onMouseMove=dN.bind(this),this._interceptControlDown=yN.bind(this),this._interceptControlUp=vN.bind(this),this.domElement!==null&&this.connect(this.domElement),this.update()}set cursorStyle(e){this._cursorStyle=e,e==="grab"?this.domElement.style.cursor="grab":this.domElement.style.cursor="auto"}get cursorStyle(){return this._cursorStyle}connect(e){super.connect(e),this.domElement.addEventListener("pointerdown",this._onPointerDown),this.domElement.addEventListener("pointercancel",this._onPointerUp),this.domElement.addEventListener("contextmenu",this._onContextMenu),this.domElement.addEventListener("wheel",this._onMouseWheel,{passive:!1}),this.domElement.ownerDocument.addEventListener("keydown",this._interceptControlDown,{passive:!0,capture:!0}),this.domElement.style.touchAction="none"}disconnect(){this.state=st.NONE,this.domElement.removeEventListener("pointerdown",this._onPointerDown),this.domElement.ownerDocument.removeEventListener("pointermove",this._onPointerMove),this.domElement.ownerDocument.removeEventListener("pointerup",this._onPointerUp),this.domElement.removeEventListener("pointercancel",this._onPointerUp),this.domElement.removeEventListener("wheel",this._onMouseWheel),this.domElement.removeEventListener("contextmenu",this._onContextMenu),this.stopListenToKeyEvents(),this.domElement.ownerDocument.removeEventListener("keydown",this._interceptControlDown,{capture:!0}),this.domElement.ownerDocument.removeEventListener("keyup",this._interceptControlUp,{capture:!0}),this._controlActive=!1,this._pointers.length=0,this._pointerPositions={},this.domElement.style.touchAction="",this.domElement.style.cursor="auto"}dispose(){this.disconnect()}getPolarAngle(){return this._spherical.phi}getAzimuthalAngle(){return this._spherical.theta}getDistance(){return this.object.position.distanceTo(this.target)}listenToKeyEvents(e){e.addEventListener("keydown",this._onKeyDown),this._domElementKeyEvents=e}stopListenToKeyEvents(){this._domElementKeyEvents!==null&&(this._domElementKeyEvents.removeEventListener("keydown",this._onKeyDown),this._domElementKeyEvents=null)}saveState(){this.target0.copy(this.target),this.position0.copy(this.object.position),this.zoom0=this.object.zoom}reset(){this.target.copy(this.target0),this.object.position.copy(this.position0),this.object.zoom=this.zoom0,this.object.updateProjectionMatrix(),this.dispatchEvent(kM),this.update(),this.state=st.NONE}pan(e,t){this._pan(e,t),this.update()}dollyIn(e){this._dollyIn(e),this.update()}dollyOut(e){this._dollyOut(e),this.update()}rotateLeft(e){this._rotateLeft(e),this.update()}rotateUp(e){this._rotateUp(e),this.update()}update(e=null){let t=this.object.position;Ht.copy(t).sub(this.target),Ht.applyQuaternion(this._quat),this._spherical.setFromVector3(Ht),this.autoRotate&&this.state===st.NONE&&this._rotateLeft(this._getAutoRotationAngle(e)),this.enableDamping?(this._spherical.theta+=this._sphericalDelta.theta*this.dampingFactor,this._spherical.phi+=this._sphericalDelta.phi*this.dampingFactor):(this._spherical.theta+=this._sphericalDelta.theta,this._spherical.phi+=this._sphericalDelta.phi);let i=this.minAzimuthAngle,r=this.maxAzimuthAngle;isFinite(i)&&isFinite(r)&&(i<-Math.PI?i+=bn:i>Math.PI&&(i-=bn),r<-Math.PI?r+=bn:r>Math.PI&&(r-=bn),i<=r?this._spherical.theta=Math.max(i,Math.min(r,this._spherical.theta)):this._spherical.theta=this._spherical.theta>(i+r)/2?Math.max(i,this._spherical.theta):Math.min(r,this._spherical.theta)),this._spherical.phi=Math.max(this.minPolarAngle,Math.min(this.maxPolarAngle,this._spherical.phi)),this._spherical.makeSafe(),this.enableDamping===!0?this.target.addScaledVector(this._panOffset,this.dampingFactor):this.target.add(this._panOffset),this.target.sub(this.cursor),this.target.clampLength(this.minTargetRadius,this.maxTargetRadius),this.target.add(this.cursor);let s=!1;if(this.zoomToCursor&&this._performCursorZoom||this.object.isOrthographicCamera)this._spherical.radius=this._clampDistance(this._spherical.radius);else{let o=this._spherical.radius;this._spherical.radius=this._clampDistance(this._spherical.radius*this._scale),s=o!=this._spherical.radius}if(Ht.setFromSpherical(this._spherical),Ht.applyQuaternion(this._quatInverse),t.copy(this.target).add(Ht),this.object.lookAt(this.target),this.enableDamping===!0?(this._sphericalDelta.theta*=1-this.dampingFactor,this._sphericalDelta.phi*=1-this.dampingFactor,this._panOffset.multiplyScalar(1-this.dampingFactor)):(this._sphericalDelta.set(0,0,0),this._panOffset.set(0,0,0)),this.zoomToCursor&&this._performCursorZoom){let o=null;if(this.object.isPerspectiveCamera){let a=Ht.length();o=this._clampDistance(a*this._scale);let c=a-o;this.object.position.addScaledVector(this._dollyDirection,c),this.object.updateMatrixWorld(),s=!!c}else if(this.object.isOrthographicCamera){let a=new O(this._mouse.x,this._mouse.y,0);a.unproject(this.object);let c=this.object.zoom;this.object.zoom=Math.max(this.minZoom,Math.min(this.maxZoom,this.object.zoom/this._scale)),this.object.updateProjectionMatrix(),s=c!==this.object.zoom;let l=new O(this._mouse.x,this._mouse.y,0);l.unproject(this.object),this.object.position.sub(l).add(a),this.object.updateMatrixWorld(),o=Ht.length()}else console.warn("WARNING: OrbitControls.js encountered an unknown camera type - zoom to cursor disabled."),this.zoomToCursor=!1;o!==null&&(this.screenSpacePanning?this.target.set(0,0,-1).transformDirection(this.object.matrix).multiplyScalar(o).add(this.object.position):(Qd.origin.copy(this.object.position),Qd.direction.set(0,0,-1).transformDirection(this.object.matrix),Math.abs(this.object.up.dot(Qd.direction))<oN?this.object.lookAt(this.target):(UM.setFromNormalAndCoplanarPoint(this.object.up,this.target),Qd.intersectPlane(UM,this.target))))}else if(this.object.isOrthographicCamera){let o=this.object.zoom;this.object.zoom=Math.max(this.minZoom,Math.min(this.maxZoom,this.object.zoom/this._scale)),o!==this.object.zoom&&(this.object.updateProjectionMatrix(),s=!0)}return this._scale=1,this._performCursorZoom=!1,s||this._lastPosition.distanceToSquared(this.object.position)>vg||8*(1-this._lastQuaternion.dot(this.object.quaternion))>vg||this._lastTargetPosition.distanceToSquared(this.target)>vg?(this.dispatchEvent(kM),this._lastPosition.copy(this.object.position),this._lastQuaternion.copy(this.object.quaternion),this._lastTargetPosition.copy(this.target),!0):!1}_getAutoRotationAngle(e){return e!==null?bn/60*this.autoRotateSpeed*e:bn/60/60*this.autoRotateSpeed}_getZoomScale(e){let t=Math.abs(e*.01);return Math.pow(.95,this.zoomSpeed*t)}_rotateLeft(e){this._sphericalDelta.theta-=e}_rotateUp(e){this._sphericalDelta.phi-=e}_panLeft(e,t){Ht.setFromMatrixColumn(t,0),Ht.multiplyScalar(-e),this._panOffset.add(Ht)}_panUp(e,t){this.screenSpacePanning===!0?Ht.setFromMatrixColumn(t,1):(Ht.setFromMatrixColumn(t,0),Ht.crossVectors(this.object.up,Ht)),Ht.multiplyScalar(e),this._panOffset.add(Ht)}_pan(e,t){let i=this.domElement;if(this.object.isPerspectiveCamera){let r=this.object.position;Ht.copy(r).sub(this.target);let s=Ht.length();s*=Math.tan(this.object.fov/2*Math.PI/180),this._panLeft(2*e*s/i.clientHeight,this.object.matrix),this._panUp(2*t*s/i.clientHeight,this.object.matrix)}else this.object.isOrthographicCamera?(this._panLeft(e*(this.object.right-this.object.left)/this.object.zoom/i.clientWidth,this.object.matrix),this._panUp(t*(this.object.top-this.object.bottom)/this.object.zoom/i.clientHeight,this.object.matrix)):(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - pan disabled."),this.enablePan=!1)}_dollyOut(e){this.object.isPerspectiveCamera||this.object.isOrthographicCamera?this._scale/=e:(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - dolly/zoom disabled."),this.enableZoom=!1)}_dollyIn(e){this.object.isPerspectiveCamera||this.object.isOrthographicCamera?this._scale*=e:(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - dolly/zoom disabled."),this.enableZoom=!1)}_updateZoomParameters(e,t){if(!this.zoomToCursor)return;this._performCursorZoom=!0;let i=this.domElement.getBoundingClientRect(),r=e-i.left,s=t-i.top,o=i.width,a=i.height;this._mouse.x=r/o*2-1,this._mouse.y=-(s/a)*2+1,this._dollyDirection.set(this._mouse.x,this._mouse.y,1).unproject(this.object).sub(this.object.position).normalize()}_clampDistance(e){return Math.max(this.minDistance,Math.min(this.maxDistance,e))}_handleMouseDownRotate(e){this._rotateStart.set(e.clientX,e.clientY)}_handleMouseDownDolly(e){this._updateZoomParameters(e.clientX,e.clientY),this._dollyStart.set(e.clientX,e.clientY)}_handleMouseDownPan(e){this._panStart.set(e.clientX,e.clientY)}_handleMouseMoveRotate(e){this._rotateEnd.set(e.clientX,e.clientY),this._rotateDelta.subVectors(this._rotateEnd,this._rotateStart).multiplyScalar(this.rotateSpeed);let t=this.domElement;this._rotateLeft(bn*this._rotateDelta.x/t.clientHeight),this._rotateUp(bn*this._rotateDelta.y/t.clientHeight),this._rotateStart.copy(this._rotateEnd),this.update()}_handleMouseMoveDolly(e){this._dollyEnd.set(e.clientX,e.clientY),this._dollyDelta.subVectors(this._dollyEnd,this._dollyStart),this._dollyDelta.y>0?this._dollyOut(this._getZoomScale(this._dollyDelta.y)):this._dollyDelta.y<0&&this._dollyIn(this._getZoomScale(this._dollyDelta.y)),this._dollyStart.copy(this._dollyEnd),this.zoomToCursor&&(this._performCursorZoom=!0,this._dollyDirection.set(this._mouse.x,this._mouse.y,1).unproject(this.object).sub(this.object.position).normalize()),this.update()}_handleMouseMovePan(e){this._panEnd.set(e.clientX,e.clientY),this._panDelta.subVectors(this._panEnd,this._panStart).multiplyScalar(this.panSpeed),this._pan(this._panDelta.x,this._panDelta.y),this._panStart.copy(this._panEnd),this.update()}_handleMouseWheel(e){this._updateZoomParameters(e.clientX,e.clientY),e.deltaY<0?this._dollyIn(this._getZoomScale(e.deltaY)):e.deltaY>0&&this._dollyOut(this._getZoomScale(e.deltaY)),this.update()}_handleKeyDown(e){let t=!1;switch(e.code){case this.keys.UP:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateUp(bn*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(0,this.keyPanSpeed),t=!0;break;case this.keys.BOTTOM:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateUp(-bn*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(0,-this.keyPanSpeed),t=!0;break;case this.keys.LEFT:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateLeft(bn*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(this.keyPanSpeed,0),t=!0;break;case this.keys.RIGHT:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateLeft(-bn*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(-this.keyPanSpeed,0),t=!0;break}t&&(e.preventDefault(),this.update())}_handleTouchStartRotate(e){if(this._pointers.length===1)this._rotateStart.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._rotateStart.set(i,r)}}_handleTouchStartPan(e){if(this._pointers.length===1)this._panStart.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._panStart.set(i,r)}}_handleTouchStartDolly(e){let t=this._getSecondPointerPosition(e),i=e.pageX-t.x,r=e.pageY-t.y,s=Math.sqrt(i*i+r*r);this._dollyStart.set(0,s)}_handleTouchStartDollyPan(e){this.enableZoom&&this._handleTouchStartDolly(e),this.enablePan&&this._handleTouchStartPan(e)}_handleTouchStartDollyRotate(e){this.enableZoom&&this._handleTouchStartDolly(e),this.enableRotate&&this._handleTouchStartRotate(e)}_handleTouchMoveRotate(e){if(this._pointers.length==1)this._rotateEnd.set(e.pageX,e.pageY);else{let i=this._getSecondPointerPosition(e),r=.5*(e.pageX+i.x),s=.5*(e.pageY+i.y);this._rotateEnd.set(r,s)}this._rotateDelta.subVectors(this._rotateEnd,this._rotateStart).multiplyScalar(this.rotateSpeed);let t=this.domElement;this._rotateLeft(bn*this._rotateDelta.x/t.clientHeight),this._rotateUp(bn*this._rotateDelta.y/t.clientHeight),this._rotateStart.copy(this._rotateEnd)}_handleTouchMovePan(e){if(this._pointers.length===1)this._panEnd.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._panEnd.set(i,r)}this._panDelta.subVectors(this._panEnd,this._panStart).multiplyScalar(this.panSpeed),this._pan(this._panDelta.x,this._panDelta.y),this._panStart.copy(this._panEnd)}_handleTouchMoveDolly(e){let t=this._getSecondPointerPosition(e),i=e.pageX-t.x,r=e.pageY-t.y,s=Math.sqrt(i*i+r*r);this._dollyEnd.set(0,s),this._dollyDelta.set(0,Math.pow(this._dollyEnd.y/this._dollyStart.y,this.zoomSpeed)),this._dollyOut(this._dollyDelta.y),this._dollyStart.copy(this._dollyEnd);let o=(e.pageX+t.x)*.5,a=(e.pageY+t.y)*.5;this._updateZoomParameters(o,a)}_handleTouchMoveDollyPan(e){this.enableZoom&&this._handleTouchMoveDolly(e),this.enablePan&&this._handleTouchMovePan(e)}_handleTouchMoveDollyRotate(e){this.enableZoom&&this._handleTouchMoveDolly(e),this.enableRotate&&this._handleTouchMoveRotate(e)}_addPointer(e){this._pointers.push(e.pointerId)}_removePointer(e){delete this._pointerPositions[e.pointerId];for(let t=0;t<this._pointers.length;t++)if(this._pointers[t]==e.pointerId){this._pointers.splice(t,1);return}}_isTrackingPointer(e){for(let t=0;t<this._pointers.length;t++)if(this._pointers[t]==e.pointerId)return!0;return!1}_trackPointer(e){let t=this._pointerPositions[e.pointerId];t===void 0&&(t=new Te,this._pointerPositions[e.pointerId]=t),t.set(e.pageX,e.pageY)}_getSecondPointerPosition(e){let t=e.pointerId===this._pointers[0]?this._pointers[1]:this._pointers[0];return this._pointerPositions[t]}_customWheelEvent(e){let t=e.deltaMode,i={clientX:e.clientX,clientY:e.clientY,deltaY:e.deltaY};switch(t){case 1:i.deltaY*=16;break;case 2:i.deltaY*=100;break}return e.ctrlKey&&!this._controlActive&&(i.deltaY*=10),i}};function aN(n){this.enabled!==!1&&(this._pointers.length===0&&(this.domElement.setPointerCapture(n.pointerId),this.domElement.ownerDocument.addEventListener("pointermove",this._onPointerMove),this.domElement.ownerDocument.addEventListener("pointerup",this._onPointerUp)),!this._isTrackingPointer(n)&&(this._addPointer(n),n.pointerType==="touch"?this._onTouchStart(n):this._onMouseDown(n),this._cursorStyle==="grab"&&(this.domElement.style.cursor="grabbing")))}function cN(n){this.enabled!==!1&&(n.pointerType==="touch"?this._onTouchMove(n):this._onMouseMove(n))}function lN(n){switch(this._removePointer(n),this._pointers.length){case 0:this.domElement.releasePointerCapture(n.pointerId),this.domElement.ownerDocument.removeEventListener("pointermove",this._onPointerMove),this.domElement.ownerDocument.removeEventListener("pointerup",this._onPointerUp),this.dispatchEvent(BM),this.state=st.NONE,this._cursorStyle==="grab"&&(this.domElement.style.cursor="grab");break;case 1:let e=this._pointers[0],t=this._pointerPositions[e];this._onTouchStart({pointerId:e,pageX:t.x,pageY:t.y});break}}function uN(n){let e;switch(n.button){case 0:e=this.mouseButtons.LEFT;break;case 1:e=this.mouseButtons.MIDDLE;break;case 2:e=this.mouseButtons.RIGHT;break;default:e=-1}switch(e){case Yn.DOLLY:if(this.enableZoom===!1)return;this._handleMouseDownDolly(n),this.state=st.DOLLY;break;case Yn.ROTATE:if(n.ctrlKey||n.metaKey||n.shiftKey){if(this.enablePan===!1)return;this._handleMouseDownPan(n),this.state=st.PAN}else{if(this.enableRotate===!1)return;this._handleMouseDownRotate(n),this.state=st.ROTATE}break;case Yn.PAN:if(n.ctrlKey||n.metaKey||n.shiftKey){if(this.enableRotate===!1)return;this._handleMouseDownRotate(n),this.state=st.ROTATE}else{if(this.enablePan===!1)return;this._handleMouseDownPan(n),this.state=st.PAN}break;default:this.state=st.NONE}this.state!==st.NONE&&this.dispatchEvent(_g)}function dN(n){switch(this.state){case st.ROTATE:if(this.enableRotate===!1)return;this._handleMouseMoveRotate(n);break;case st.DOLLY:if(this.enableZoom===!1)return;this._handleMouseMoveDolly(n);break;case st.PAN:if(this.enablePan===!1)return;this._handleMouseMovePan(n);break}}function fN(n){this.enabled===!1||this.enableZoom===!1||this.state!==st.NONE||(n.preventDefault(),this.dispatchEvent(_g),this._handleMouseWheel(this._customWheelEvent(n)),this.dispatchEvent(BM))}function hN(n){this.enabled!==!1&&this._handleKeyDown(n)}function pN(n){switch(this._trackPointer(n),this._pointers.length){case 1:switch(this.touches.ONE){case li.ROTATE:if(this.enableRotate===!1){this.state=st.NONE;break}this._handleTouchStartRotate(n),this.state=st.TOUCH_ROTATE;break;case li.PAN:if(this.enablePan===!1){this.state=st.NONE;break}this._handleTouchStartPan(n),this.state=st.TOUCH_PAN;break;default:this.state=st.NONE}break;case 2:switch(this.touches.TWO){case li.DOLLY_PAN:if(this.enableZoom===!1&&this.enablePan===!1)return;this._handleTouchStartDollyPan(n),this.state=st.TOUCH_DOLLY_PAN;break;case li.DOLLY_ROTATE:if(this.enableZoom===!1&&this.enableRotate===!1)return;this._handleTouchStartDollyRotate(n),this.state=st.TOUCH_DOLLY_ROTATE;break;default:this.state=st.NONE}break;default:this.state=st.NONE}this.state!==st.NONE&&this.dispatchEvent(_g)}function mN(n){switch(this._trackPointer(n),this.state){case st.TOUCH_ROTATE:if(this.enableRotate===!1)return;this._handleTouchMoveRotate(n),this.update();break;case st.TOUCH_PAN:if(this.enablePan===!1)return;this._handleTouchMovePan(n),this.update();break;case st.TOUCH_DOLLY_PAN:if(this.enableZoom===!1&&this.enablePan===!1)return;this._handleTouchMoveDollyPan(n),this.update();break;case st.TOUCH_DOLLY_ROTATE:if(this.enableZoom===!1&&this.enableRotate===!1)return;this._handleTouchMoveDollyRotate(n),this.update();break;default:this.state=st.NONE}}function gN(n){this.enabled!==!1&&n.preventDefault()}function yN(n){n.key==="Control"&&(this._controlActive=!0,this.domElement.ownerDocument.addEventListener("keyup",this._interceptControlUp,{passive:!0,capture:!0}))}function vN(n){n.key==="Control"&&(this._controlActive=!1,this.domElement.ownerDocument.removeEventListener("keyup",this._interceptControlUp,{passive:!0,capture:!0}))}var xc=new Ci(.2,.22,.06,20),VM=new nr(.13,.42,14),HM=new ys(.09,14,10),_N=new Za(.17),xN=new Qa(.1,.03,8,18),MN=new Ja(.15),ko=new Di({color:"#2a241c",roughness:.6}),zM=new Map,SN=n=>{let e=zM.get(n);return e||zM.set(n,e=new Di({color:n,roughness:.45,metalness:.15})),e};function Zn(n,e,t,i){let r=new en(e,t);return r.position.y=i,r.castShadow=!0,n.add(r),r}function GM(n,e){let t=new pn,i=SN(e);switch(n){case"heroi":Zn(t,xc,ko,.03),Zn(t,VM,i,.27),Zn(t,HM,i,.5);break;case"pessoa":Zn(t,xc,i,.03),Zn(t,VM,ko,.2).scale.setScalar(.7),Zn(t,HM,ko,.37).scale.setScalar(.7);break;case"inimigo":Zn(t,xc,ko,.03),Zn(t,_N,i,.27).scale.set(1,1.5,1);break;case"busca":Zn(t,xc,i,.03),Zn(t,xN,ko,.075).rotation.x=Math.PI/2;break;case"ameaca":Zn(t,xc,i,.03),Zn(t,MN,ko,.13);break}return t}var Mg=new Ci(1,1,1,6),WM=new nr(1,1,7),jM=new nr(1,1,5),xg=new ys(1,10,8),tf=new Ar(1,1,1),EN=new nr(1,1,4),bN=new Ci(1,1,1,6),yn=(n,e={})=>new Di(Ve({color:n,roughness:.9,flatShading:!0},e)),TN=Object.fromEntries(Object.keys($n).map(n=>[n,n==="agua"?yn($n[n].color,{roughness:.25,metalness:.1}):yn($n[n].color)])),tn={soil:yn("#5b4a36"),leaf:yn("#2f6b34"),leafDark:yn("#255a2c"),trunk:yn("#6b4a2b"),hill:yn("#86904a"),rock:yn("#77726d"),snow:yn("#eef1f3"),stone:yn("#b3b6ab"),path:yn("#8f744b"),wall:yn("#d9c9a3"),roof:yn("#9c4a32"),outline:yn("#2a241c"),divider:yn("#f1e7cf")};function ln(n,e,t,i,r,s,o,a,c,l=0){let u=new en(e,t);return u.position.set(i,r,s),u.scale.set(o,a,c),u.rotation.y=l,u.castShadow=!0,u.receiveShadow=!0,n.add(u),u}function Or(n,e,t=.42,i=.68){let r=n()*Math.PI*2;return Array.from({length:e},(s,o)=>{let a=r+o/e*Math.PI*2+(n()-.5)*.5,c=t+n()*(i-t);return[Math.cos(a)*c,Math.sin(a)*c]})}function wN(n,e,t,i){switch(e){case"floresta":for(let[r,s]of Or(i,7)){let o=.45+i()*.35;ln(n,bN,tn.trunk,r,t+.08,s,.04,.16,.04),ln(n,WM,i()>.5?tn.leaf:tn.leafDark,r,t+.14+o/2,s,.17,o,.17,i()*6)}break;case"colina":for(let[r,s]of Or(i,3,.3,.5)){let o=.32+i()*.16;ln(n,xg,tn.hill,r,t-.02,s,o,o*.55,o)}break;case"montanha":for(let[r,s]of Or(i,3,.38,.5)){let o=.8+i()*.6,a=.34+i()*.1,c=i()*6;ln(n,jM,tn.rock,r,t+o/2,s,a,o,a,c),ln(n,jM,tn.snow,r,t+o*.86,s,a*.3,o*.29,a*.3,c)}break;case"agua":for(let[r,s]of Or(i,2,.5,.7))ln(n,xg,tn.rock,r,t,s,.09,.06,.09);break;case"estrada":ln(n,Mg,tn.path,0,t+.005,0,.55,.02,.55);for(let[r,s]of Or(i,5,.66,.78))ln(n,xg,tn.stone,r,t,s,.06,.04,.06);break;case"ruinas":for(let[r,s]of Or(i,5,.5,.66)){let o=.15+i()*.5;ln(n,tf,tn.stone,r,t+o/2,s,.14,o,.14,i()*.6)}ln(n,tf,tn.stone,.45,t+.05,.1,.5,.1,.12,1.1);break;case"vila":for(let[r,s]of Or(i,4,.5,.62)){let o=i()*6,a=.2+i()*.06;ln(n,tf,tn.wall,r,t+.1,s,a,.2,a,o),ln(n,EN,tn.roof,r,t+.29,s,a*.85,.18,a*.85,o+Math.PI/4)}break;case"planicie":for(let[r,s]of Or(i,3,.5,.75))ln(n,WM,tn.leaf,r,t+.04,s,.05,.08,.05);break}}function $M(n,e,t,i){let r=new pn,{x:s,z:o}=_r(n,e);r.position.set(s,0,o);let a=$n[t].height;return ln(r,Mg,tn.soil,0,.04,0,1,.08,1),ln(r,Mg,TN[t],0,.08+(a-.08)/2,0,.995,a-.08,.995),wN(r,t,a,L0(n,e,t.length)),i.forEach((c,l)=>{if(!c)return;let u=l*Math.PI/3,d=us/2*(c==="contorno"?.965:.985),f=c==="contorno"?.07:.03,h=ln(r,tf,c==="contorno"?tn.outline:tn.divider,Math.cos(u)*d,a+.012,Math.sin(u)*d,1,.03,f,-(u+Math.PI/2));h.castShadow=!1}),r}var IN=new Ci(.97,.97,.36,6),Sg=new gs({color:"#f5d76e",transparent:!0,opacity:.45,depthWrite:!1}),nf=new gs({color:"#e04b3a",transparent:!0,opacity:.45,depthWrite:!1});function Eg(n,e,t){let i=new en(IN,t),{x:r,z:s}=_r(n,e);return i.position.set(r,.2,s),i}function qM(n){let e=[];for(let i=-n;i<=n;i++)for(let r=Math.max(-n,-i-n);r<=Math.min(n,-i+n);r++){let{x:s,z:o}=_r(i,r);for(let a=0;a<6;a++){let c=Math.PI/6+a*Math.PI/3,l=c+Math.PI/3;e.push(s+Math.cos(c),.01,o+Math.sin(c),s+Math.cos(l),.01,o+Math.sin(l))}}let t=new an;return t.setAttribute("position",new rt(e,3)),new Wa(t,new Io({color:"#6f6248",transparent:!0,opacity:.55}))}var CN=["canvas"],XM=14,rf=class n{store=Ze(ro);host=Ze(rs);canvasRef=x0.required("canvas");renderer;scene=new Ha;camera=new hn(45,1,.1,400);controls;tilesGroup=new pn;markersGroup=new pn;ghostGroup=new pn;raycaster=new oc;ground=new fn(new O(0,1,0),0);resizeObs;frame=0;dirty=!0;hover=null;down=null;constructor(){es(()=>{this.store.state(),this.rebuild()}),es(()=>{this.store.mode(),this.store.tileId(),this.store.rotation(),this.store.state(),this.updateGhost()})}ngAfterViewInit(){let e=this.canvasRef().nativeElement,t=new Zd({canvas:e,antialias:!0});t.setPixelRatio(Math.min(window.devicePixelRatio,2)),t.shadowMap.enabled=!0,t.shadowMap.type=od,this.renderer=t,this.scene.background=new He("#1c1a17"),this.scene.fog=new Va("#1c1a17",45,110);let i=new en(new Xa(80,48),new Di({color:"#3a3225",roughness:1}));i.rotation.x=-Math.PI/2,i.position.y=-.01,i.receiveShadow=!0,this.scene.add(i,qM(XM),this.tilesGroup,this.markersGroup,this.ghostGroup),this.scene.add(new nc("#fff4dc","#3a3225",1.1));let r=new sc("#fff1d0",2.2);r.position.set(14,24,10),r.castShadow=!0,r.shadow.mapSize.set(2048,2048);let s=r.shadow.camera;s.left=s.bottom=-30,s.right=s.top=30,s.far=80,r.shadow.bias=-5e-4,this.scene.add(r),this.camera.position.set(0,16,15);let o=new ef(this.camera,e);o.enableDamping=!0,o.maxPolarAngle=Math.PI/2.15,o.minDistance=4,o.maxDistance=60,o.mouseButtons={LEFT:Yn.PAN,MIDDLE:Yn.DOLLY,RIGHT:Yn.ROTATE},o.touches={ONE:li.PAN,TWO:li.DOLLY_ROTATE},o.screenSpacePanning=!1,o.addEventListener("change",()=>this.dirty=!0),this.controls=o,e.addEventListener("pointerdown",this.onDown),e.addEventListener("pointerup",this.onUp),e.addEventListener("pointermove",this.onMove),e.addEventListener("pointerleave",this.onLeave),e.addEventListener("contextmenu",c=>c.preventDefault()),this.resizeObs=new ResizeObserver(()=>this.resize()),this.resizeObs.observe(this.host.nativeElement),this.resize(),this.rebuild();let a=()=>{this.frame=requestAnimationFrame(a),o.update(),this.dirty&&(this.dirty=!1,t.render(this.scene,this.camera))};a()}ngOnDestroy(){cancelAnimationFrame(this.frame),this.resizeObs?.disconnect(),this.controls?.dispose(),this.renderer?.dispose()}resize(){let{clientWidth:e,clientHeight:t}=this.host.nativeElement;!this.renderer||!e||!t||(this.renderer.setSize(e,t,!1),this.camera.aspect=e/t,this.camera.updateProjectionMatrix(),this.dirty=!0)}rebuild(){let e=this.store;this.tilesGroup.clear(),this.markersGroup.clear();for(let r of e.occupied().values()){let s=[0,1,2,3,4,5].map(o=>e.edgeKind(r,o));this.tilesGroup.add($M(r.q,r.r,r.terrain,s))}let t=new Map,i=new Map;for(let r of e.state().markers)i.set(jn(r.q,r.r),(i.get(jn(r.q,r.r))??0)+1);for(let r of e.state().markers){let s=jn(r.q,r.r),o=t.get(s)??0;t.set(s,o+1);let a=i.get(s)??1,c=GM(r.kind,r.color),{x:l,z:u}=_r(r.q,r.r),d=a>1?.3:0,f=o/a*Math.PI*2;c.position.set(l+Math.cos(f)*d,e.heightAt(r.q,r.r)+.02,u+Math.sin(f)*d),this.markersGroup.add(c)}this.dirty=!0}updateGhost(){this.ghostGroup.clear(),this.dirty=!0;let e=this.hover;if(!e)return;let t=this.store,i=t.mode();if(i==="peca"){let r=t.canPlace(t.tileId(),e.q,e.r,t.rotation());for(let s of t.cellsOf(t.tileId(),e.q,e.r,t.rotation()))this.ghostGroup.add(Eg(s.q,s.r,r?Sg:nf))}else{let r=t.occupied().has(jn(e.q,e.r)),s=t.state().markers.some(a=>a.q===e.q&&a.r===e.r),o=i==="marcador"||r||i==="apagar"&&s;this.ghostGroup.add(Eg(e.q,e.r,i==="apagar"?nf:o?Sg:nf))}}cellAt(e){let t=this.canvasRef().nativeElement.getBoundingClientRect(),i=new Te((e.clientX-t.left)/t.width*2-1,-((e.clientY-t.top)/t.height)*2+1);this.raycaster.setFromCamera(i,this.camera);let r=this.raycaster.ray.intersectPlane(this.ground,new O);if(!r)return null;let s=N0(r.x,r.z);return Math.max(Math.abs(s.q),Math.abs(s.r),Math.abs(s.q+s.r))<=XM?s:null}onDown=e=>{e.button===0&&(this.down={x:e.clientX,y:e.clientY,t:performance.now()})};onUp=e=>{let t=this.down;if(this.down=null,!t||Math.hypot(e.clientX-t.x,e.clientY-t.y)>8||performance.now()-t.t>600)return;let i=this.cellAt(e);i&&this.store.applyAt(i.q,i.r)};onMove=e=>{if(e.pointerType!=="mouse")return;let t=this.cellAt(e);t?.q===this.hover?.q&&t?.r===this.hover?.r||(this.hover=t,this.updateGhost())};onLeave=()=>{this.hover=null,this.updateGhost()};static \u0275fac=function(t){return new(t||n)};static \u0275cmp=ga({type:n,selectors:[["app-map-scene"]],viewQuery:function(t,i){t&1&&Xl(i.canvasRef,CN,5),t&2&&kp()},decls:2,vars:0,consts:[["canvas",""]],template:function(t,i){t&1&&ql(0,"canvas",null,0)},styles:["[_nghost-%COMP%]{display:block;position:absolute;inset:0}","canvas[_ngcontent-%COMP%]{display:block;width:100%;height:100%;touch-action:none;outline:none}"]})};var bg=(n,e)=>e.id,AN=(n,e)=>e.def.id;function DN(n,e){if(n&1){let t=Ei();Je(0,"div",6)(1,"button",5),wt("click",function(){Lt(t);let r=dt();return Ot(r.exportMap())}),Dt(2,"Exportar JSON"),tt(),Je(3,"button",5),wt("click",function(){Lt(t),dt();let r=to(13);return Ot(r.click())}),Dt(4,"Importar JSON"),tt(),Je(5,"button",14),wt("click",function(){Lt(t);let r=dt();return Ot(r.clearMap())}),Dt(6,"Limpar mapa"),tt(),vr(7,"hr"),Je(8,"button",5),wt("click",function(){Lt(t);let r=dt();return Ot(r.savePresets())}),Dt(9,"Salvar pinturas como padr\xE3o"),tt(),Je(10,"button",5),wt("click",function(){Lt(t);let r=dt();return Ot(r.exportPresets())}),Dt(11,"Exportar presets"),tt(),Je(12,"button",5),wt("click",function(){Lt(t),dt();let r=to(11);return Ot(r.click())}),Dt(13,"Importar presets"),tt(),Je(14,"button",14),wt("click",function(){Lt(t);let r=dt();return Ot(r.clearPresets())}),Dt(15,"Remover presets"),tt()()}}function RN(n,e){if(n&1&&(Je(0,"div",8),Dt(1),tt()),n&2){let t=dt();gt(),cs(t.notice())}}function NN(n,e){if(n&1){let t=Ei();Je(0,"button",5),wt("click",function(){let r=Lt(t).$implicit,s=dt();return Ot(s.store.mode.set(r.id))}),Dt(1),tt()}if(n&2){let t=e.$implicit,i=dt();bi("active",i.store.mode()===t.id),gt(),cs(t.label)}}function PN(n,e){if(n&1&&(na(),vr(0,"polygon")),n&2){let t=e.$implicit,i=e.$index,r=dt().$implicit,s=dt(2);as("fill",s.terrainColor(r.def.id,i)),mr("points",t)}}function LN(n,e){if(n&1){let t=Ei();Je(0,"button",19),wt("click",function(){let r=Lt(t).$implicit,s=dt(2);return Ot(s.store.tileId.set(r.def.id))}),na(),Je(1,"svg",20)(2,"g"),gr(3,PN,1,3,"polygon",21,jl),tt()(),ul(),Je(5,"span"),Dt(6),tt()()}if(n&2){let t=e.$implicit,i=dt(2);bi("active",i.store.tileId()===t.def.id),gt(),mr("viewBox",t.viewBox),gt(),mr("transform",i.store.tileId()===t.def.id?"rotate("+i.store.rotation()*60+")":null),gt(),yr(t.polys),gt(3),cs(t.def.name)}}function ON(n,e){if(n&1){let t=Ei();Je(0,"div",15)(1,"div",16)(2,"button",5),wt("click",function(){Lt(t);let r=dt();return Ot(r.side.set("A"))}),Dt(3,"Lado A"),tt(),Je(4,"button",5),wt("click",function(){Lt(t);let r=dt();return Ot(r.side.set("B"))}),Dt(5,"Lado B"),tt()(),Je(6,"button",17),wt("click",function(){Lt(t);let r=dt();return Ot(r.store.rotateTool())}),Dt(7),tt()(),Je(8,"div",13),gr(9,LN,7,5,"button",18,AN),tt()}if(n&2){let t=dt();gt(2),bi("active",t.side()==="A"),gt(2),bi("active",t.side()==="B"),gt(3),ls("Girar 60\xB0 (",t.store.rotation()*60,"\xB0)"),gt(2),yr(t.visibleThumbs())}}function FN(n,e){if(n&1){let t=Ei();Je(0,"button",23),wt("click",function(){let r=Lt(t).$implicit,s=dt(2);return Ot(s.store.paintTerrain.set(r.id))}),vr(1,"i"),Dt(2),tt()}if(n&2){let t=e.$implicit,i=dt(2);bi("active",i.store.paintTerrain()===t.id),gt(),as("background",t.color),gt(),ls("",t.label," ")}}function kN(n,e){if(n&1&&(Je(0,"div",13),gr(1,FN,3,5,"button",22,bg),tt()),n&2){let t=dt();gt(),yr(t.terrains)}}function UN(n,e){if(n&1){let t=Ei();Je(0,"button",23),wt("click",function(){let r=Lt(t).$implicit,s=dt(2);return Ot(s.store.markerKind.set(r.id))}),vr(1,"i",25),Dt(2),tt()}if(n&2){let t=e.$implicit,i=dt(2);bi("active",i.store.markerKind()===t.id),gt(),as("background",t.id==="heroi"?i.store.heroColor():t.color),gt(),ls("",t.label," ")}}function BN(n,e){if(n&1){let t=Ei();Je(0,"button",27),wt("click",function(){let r=Lt(t).$implicit,s=dt(3);return Ot(s.store.heroColor.set(r))}),tt()}if(n&2){let t=e.$implicit,i=dt(3);as("background",t),bi("active",i.store.heroColor()===t),mr("aria-label","Cor "+t)}}function VN(n,e){if(n&1&&(Je(0,"div",24),gr(1,BN,1,5,"button",26,jl),tt()),n&2){let t=dt(2);gt(),yr(t.heroColors)}}function HN(n,e){if(n&1&&(Je(0,"div",13),gr(1,UN,3,5,"button",22,bg),tt(),Qs(3,VN,3,0,"div",24)),n&2){let t=dt();gt(),yr(t.markers),gt(2),eo(t.store.markerKind()==="heroi"?3:-1)}}function zN(n){let e=n.cells.map(l=>_r(l.q,l.r)),t=e.map(l=>l.x),i=e.map(l=>l.z),r=Math.min(...t)-us/2-.15,s=Math.min(...i)-1.15,o=Math.max(...t)+us/2+.15-r,a=Math.max(...i)+1.15-s,c=e.map(({x:l,z:u})=>Array.from({length:6},(d,f)=>{let h=Math.PI/6+f*Math.PI/3;return`${(l+Math.cos(h)).toFixed(2)},${(u+Math.sin(h)).toFixed(2)}`}).join(" "));return{def:n,viewBox:`${r} ${s} ${o} ${a}`,polys:c}}var sf=class n{store=Ze(ro);modes=[{id:"peca",label:"Pe\xE7as"},{id:"terreno",label:"Terreno"},{id:"marcador",label:"Marcadores"},{id:"apagar",label:"Apagar"}];terrains=Object.keys($n).map(e=>Ve({id:e},$n[e]));markers=Object.keys(io).map(e=>Ve({id:e},io[e]));heroColors=tu;side=Jt("A");thumbs=wa.map(zN);visibleThumbs=no(()=>this.thumbs.filter(e=>e.def.side==="avulso"||e.def.side===this.side()));menuOpen=Jt(!1);notice=Jt("");hint=no(()=>{switch(this.store.mode()){case"peca":return"Toque no mapa para encaixar a pe\xE7a. Gire antes de colocar.";case"terreno":return"Toque num hex\xE1gono de uma pe\xE7a para pintar o terreno.";case"marcador":return"Toque num hex\xE1gono para colocar o marcador.";case"apagar":return"Toque para remover: primeiro o marcador, depois a pe\xE7a."}});onKey(e){e.target?.tagName!=="INPUT"&&(e.key==="r"||e.key==="R"?this.store.rotateTool(e.shiftKey?-1:1):(e.ctrlKey||e.metaKey)&&e.key==="z"&&this.store.undo())}exportMap(){this.download(this.store.exportJson(),"mapa.json")}terrainColor(e,t){let i=this.store.presets()[e]?.[t];return i?$n[i].color:null}savePresets(){let e=this.store.savePresetsFromMap();this.flash(e?`Padr\xE3o salvo para ${e} pe\xE7a(s).`:"Nenhuma pe\xE7a pintada no mapa."),this.menuOpen.set(!1)}exportPresets(){this.download(this.store.exportPresets(),"presets.json")}async importPresets(e){let t=e.files?.[0];if(e.value="",!t)return;let i=this.store.importPresets(await t.text());this.flash(i<0?"Arquivo inv\xE1lido: n\xE3o \xE9 um arquivo de presets.":`Presets carregados: ${i} pe\xE7a(s).`),this.menuOpen.set(!1)}clearPresets(){this.store.clearPresets(),this.flash("Presets removidos."),this.menuOpen.set(!1)}download(e,t){let i=URL.createObjectURL(new Blob([e],{type:"application/json"})),r=document.createElement("a");r.href=i,r.download=t,r.click(),URL.revokeObjectURL(i),this.menuOpen.set(!1)}async importMap(e){let t=e.files?.[0];if(e.value="",!t)return;let i=this.store.importJson(await t.text());this.flash(i?"Mapa carregado.":"Arquivo inv\xE1lido: n\xE3o \xE9 um mapa deste editor."),this.menuOpen.set(!1)}clearMap(){this.store.clear(),this.flash("Mapa limpo. Use Desfazer para recuperar."),this.menuOpen.set(!1)}flash(e){this.notice.set(e),setTimeout(()=>this.notice.set(""),3e3)}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=ga({type:n,selectors:[["app-root"]],hostBindings:function(t,i){t&1&&wt("keydown",function(s){return i.onKey(s)},mp)},decls:24,vars:6,consts:[["presetFile",""],["file",""],[1,"top"],[1,"top-actions"],["type","button",3,"click","disabled"],["type","button",3,"click"],[1,"menu"],["type","file","accept","application/json,.json","hidden","",3,"change"],["role","status",1,"notice"],[1,"panel"],[1,"tabs"],["type","button",3,"active"],[1,"hint"],[1,"strip"],["type","button",1,"danger",3,"click"],[1,"row"],[1,"seg"],["type","button",1,"rotate",3,"click"],["type","button",1,"tile",3,"active"],["type","button",1,"tile",3,"click"],["aria-hidden","true"],[3,"fill"],["type","button",1,"chip",3,"active"],["type","button",1,"chip",3,"click"],[1,"strip","colors"],[1,"round"],["type","button",1,"swatch",3,"active","background"],["type","button",1,"swatch",3,"click"]],template:function(t,i){if(t&1){let r=Ei();vr(0,"app-map-scene"),Je(1,"header",2)(2,"h1"),Dt(3,"Mapa 3D"),tt(),Je(4,"div",3)(5,"button",4),wt("click",function(){return i.store.undo()}),Dt(6,"Desfazer"),tt(),Je(7,"button",5),wt("click",function(){return i.menuOpen.set(!i.menuOpen())}),Dt(8,"Mapa"),tt()(),Qs(9,DN,16,0,"div",6),Je(10,"input",7,0),wt("change",function(){Lt(r);let o=to(11);return Ot(i.importPresets(o))}),tt(),Je(12,"input",7,1),wt("change",function(){Lt(r);let o=to(13);return Ot(i.importMap(o))}),tt()(),Qs(14,RN,2,1,"div",8),Je(15,"footer",9)(16,"nav",10),gr(17,NN,2,3,"button",11,bg),tt(),Je(19,"p",12),Dt(20),tt(),Qs(21,ON,11,5)(22,kN,3,0,"div",13)(23,HN,4,1),tt()}if(t&2){let r;gt(5),$l("disabled",!i.store.canUndo()),gt(2),mr("aria-expanded",i.menuOpen()),gt(2),eo(i.menuOpen()?9:-1),gt(5),eo(i.notice()?14:-1),gt(3),yr(i.modes),gt(3),cs(i.hint()),gt(),eo((r=i.store.mode())==="peca"?21:r==="terreno"?22:r==="marcador"?23:-1)}},dependencies:[rf],styles:["[_nghost-%COMP%]{position:fixed;inset:0;display:block;color:#efe6d2;font:14px/1.35 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}button[_ngcontent-%COMP%]{font:inherit;color:inherit;background:#3a3225;border:1px solid #5a4e3a;border-radius:8px;padding:8px 12px;min-height:40px;cursor:pointer;-webkit-tap-highlight-color:transparent}button[_ngcontent-%COMP%]:disabled{opacity:.4;cursor:default}button.active[_ngcontent-%COMP%]{background:#c9a24a;border-color:#e6c877;color:#1c1a17;font-weight:600}button[_ngcontent-%COMP%]:focus-visible{outline:2px solid #e6c877;outline-offset:2px}.top[_ngcontent-%COMP%]{position:absolute;top:0;left:0;right:0;display:flex;align-items:center;justify-content:space-between;gap:8px;padding:calc(8px + env(safe-area-inset-top)) 12px 8px;background:linear-gradient(#1c1a17e6,#1c1a1700);pointer-events:none}.top[_ngcontent-%COMP%] > *[_ngcontent-%COMP%]{pointer-events:auto}.top[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{margin:0;font:600 17px/1 Georgia,Times New Roman,serif;letter-spacing:.04em}.top-actions[_ngcontent-%COMP%]{display:flex;gap:8px}.menu[_ngcontent-%COMP%]{position:absolute;top:calc(100% - 2px);right:12px;display:flex;flex-direction:column;gap:6px;padding:8px;background:#26221c;border:1px solid #5a4e3a;border-radius:10px;box-shadow:0 8px 24px #0008}.menu[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{text-align:left}.menu[_ngcontent-%COMP%]   hr[_ngcontent-%COMP%]{width:100%;margin:2px 0;border:0;border-top:1px solid #5a4e3a}.danger[_ngcontent-%COMP%]{color:#ff9c8c}.notice[_ngcontent-%COMP%]{position:absolute;top:calc(64px + env(safe-area-inset-top));left:50%;transform:translate(-50%);padding:8px 14px;background:#26221c;border:1px solid #c9a24a;border-radius:999px;white-space:nowrap}.panel[_ngcontent-%COMP%]{position:absolute;left:0;right:0;bottom:0;padding:8px 12px calc(10px + env(safe-area-inset-bottom));background:#1c1a17f0;border-top:1px solid #5a4e3a;display:flex;flex-direction:column;gap:8px}.tabs[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(4,1fr);gap:6px}.tabs[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{padding:8px 4px}.hint[_ngcontent-%COMP%]{margin:0;color:#b8ad95;font-size:12.5px}.row[_ngcontent-%COMP%]{display:flex;gap:8px;align-items:center;justify-content:space-between}.seg[_ngcontent-%COMP%]{display:flex}.seg[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{border-radius:0}.seg[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]:first-child{border-radius:8px 0 0 8px}.seg[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]:last-child{border-radius:0 8px 8px 0;border-left:0}.strip[_ngcontent-%COMP%]{display:flex;gap:8px;overflow-x:auto;padding:3px 3px 5px;scrollbar-width:thin}.strip[_ngcontent-%COMP%] > *[_ngcontent-%COMP%]{flex:0 0 auto}.tile[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;gap:4px;width:76px;padding:6px}.tile[_ngcontent-%COMP%]   svg[_ngcontent-%COMP%]{width:56px;height:48px;overflow:visible}.tile[_ngcontent-%COMP%]   g[_ngcontent-%COMP%]{transform-box:fill-box;transform-origin:center}.tile[_ngcontent-%COMP%]   polygon[_ngcontent-%COMP%]{fill:#7fa650;stroke:#1c1a17;stroke-width:.12}.tile.active[_ngcontent-%COMP%]   polygon[_ngcontent-%COMP%]{fill:#2f5a26}.tile[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{font-size:12px}.chip[_ngcontent-%COMP%]{display:flex;align-items:center;gap:8px}.chip[_ngcontent-%COMP%]   i[_ngcontent-%COMP%]{width:16px;height:16px;border-radius:4px;border:1px solid #0006}.chip[_ngcontent-%COMP%]   i.round[_ngcontent-%COMP%]{border-radius:50%}.swatch[_ngcontent-%COMP%]{width:40px;height:40px;padding:0;border-radius:50%;border:2px solid #5a4e3a}.swatch.active[_ngcontent-%COMP%]{border-color:#fff;box-shadow:0 0 0 2px #c9a24a}@media(min-width:720px){.panel[_ngcontent-%COMP%]{left:12px;right:auto;bottom:12px;width:420px;border:1px solid #5a4e3a;border-radius:12px}}"]})};Yp(sf,R0).catch(n=>console.error(n));
