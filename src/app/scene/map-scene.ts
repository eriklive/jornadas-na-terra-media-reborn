import { AfterViewInit, Component, ElementRef, OnDestroy, effect, inject, viewChild } from '@angular/core';
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { axialToWorld, key, worldToAxial } from '../core/hex';
import { MapStore } from '../core/map-store';
import { buildMarker } from './marker-factory';
import { EdgeKind, buildCell, buildGhostCell, buildGrid, ghostBad, ghostOk } from './terrain-factory';

const GRID_RADIUS = 14;

@Component({
  selector: 'app-map-scene',
  template: '<canvas #canvas></canvas>',
  styles: [
    ':host{display:block;position:absolute;inset:0}',
    'canvas{display:block;width:100%;height:100%;touch-action:none;outline:none}',
  ],
})
export class MapScene implements AfterViewInit, OnDestroy {
  private readonly store = inject(MapStore);
  private readonly host = inject<ElementRef<HTMLElement>>(ElementRef);
  private readonly canvasRef = viewChild.required<ElementRef<HTMLCanvasElement>>('canvas');

  private renderer?: THREE.WebGLRenderer;
  private readonly scene = new THREE.Scene();
  private readonly camera = new THREE.PerspectiveCamera(45, 1, 0.1, 400);
  private controls?: OrbitControls;
  private readonly tilesGroup = new THREE.Group();
  private readonly markersGroup = new THREE.Group();
  private readonly ghostGroup = new THREE.Group();
  private readonly raycaster = new THREE.Raycaster();
  private readonly ground = new THREE.Plane(new THREE.Vector3(0, 1, 0), 0);
  private resizeObs?: ResizeObserver;
  private frame = 0;
  private dirty = true;
  private hover: { q: number; r: number } | null = null;
  private down: { x: number; y: number; t: number } | null = null;

  constructor() {
    // Reconstrói a cena sempre que o mapa muda.
    effect(() => {
      this.store.state();
      this.rebuild();
    });
    // Atualiza a pré-visualização quando a ferramenta muda.
    effect(() => {
      this.store.mode();
      this.store.tileId();
      this.store.rotation();
      this.store.state();
      this.updateGhost();
    });
  }

  ngAfterViewInit(): void {
    const canvas = this.canvasRef().nativeElement;
    const renderer = new THREE.WebGLRenderer({ canvas, antialias: true });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.shadowMap.enabled = true;
    renderer.shadowMap.type = THREE.PCFSoftShadowMap;
    this.renderer = renderer;

    this.scene.background = new THREE.Color('#1c1a17');
    this.scene.fog = new THREE.Fog('#1c1a17', 45, 110);

    const table = new THREE.Mesh(
      new THREE.CircleGeometry(80, 48),
      new THREE.MeshStandardMaterial({ color: '#3a3225', roughness: 1 }),
    );
    table.rotation.x = -Math.PI / 2;
    table.position.y = -0.01;
    table.receiveShadow = true;
    this.scene.add(table, buildGrid(GRID_RADIUS), this.tilesGroup, this.markersGroup, this.ghostGroup);

    this.scene.add(new THREE.HemisphereLight('#fff4dc', '#3a3225', 1.1));
    const sun = new THREE.DirectionalLight('#fff1d0', 2.2);
    sun.position.set(14, 24, 10);
    sun.castShadow = true;
    sun.shadow.mapSize.set(2048, 2048);
    const sc = sun.shadow.camera;
    sc.left = sc.bottom = -30;
    sc.right = sc.top = 30;
    sc.far = 80;
    sun.shadow.bias = -0.0005;
    this.scene.add(sun);

    this.camera.position.set(0, 16, 15);
    const controls = new OrbitControls(this.camera, canvas);
    controls.enableDamping = true;
    controls.maxPolarAngle = Math.PI / 2.15;
    controls.minDistance = 4;
    controls.maxDistance = 60;
    // Arrastar move o mapa; botão direito ou dois dedos giram e aproximam.
    controls.mouseButtons = { LEFT: THREE.MOUSE.PAN, MIDDLE: THREE.MOUSE.DOLLY, RIGHT: THREE.MOUSE.ROTATE };
    controls.touches = { ONE: THREE.TOUCH.PAN, TWO: THREE.TOUCH.DOLLY_ROTATE };
    controls.screenSpacePanning = false;
    controls.addEventListener('change', () => (this.dirty = true));
    this.controls = controls;

    canvas.addEventListener('pointerdown', this.onDown);
    canvas.addEventListener('pointerup', this.onUp);
    canvas.addEventListener('pointermove', this.onMove);
    canvas.addEventListener('pointerleave', this.onLeave);
    canvas.addEventListener('contextmenu', (e) => e.preventDefault());

    this.resizeObs = new ResizeObserver(() => this.resize());
    this.resizeObs.observe(this.host.nativeElement);
    this.resize();
    this.rebuild();

    const loop = () => {
      this.frame = requestAnimationFrame(loop);
      controls.update();
      if (this.dirty) {
        this.dirty = false;
        renderer.render(this.scene, this.camera);
      }
    };
    loop();
  }

  ngOnDestroy(): void {
    cancelAnimationFrame(this.frame);
    this.resizeObs?.disconnect();
    this.controls?.dispose();
    this.renderer?.dispose();
  }

  private resize(): void {
    const { clientWidth: w, clientHeight: h } = this.host.nativeElement;
    if (!this.renderer || !w || !h) return;
    this.renderer.setSize(w, h, false);
    this.camera.aspect = w / h;
    this.camera.updateProjectionMatrix();
    this.dirty = true;
  }

  private rebuild(): void {
    const store = this.store;
    this.tilesGroup.clear();
    this.markersGroup.clear();

    for (const oc of store.occupied().values()) {
      const edges: EdgeKind[] = [0, 1, 2, 3, 4, 5].map((i) => store.edgeKind(oc, i));
      this.tilesGroup.add(buildCell(oc.q, oc.r, oc.terrain, edges));
    }

    // Vários marcadores no mesmo hexágono se espalham em círculo.
    const perCell = new Map<string, number>();
    const totals = new Map<string, number>();
    for (const m of store.state().markers) totals.set(key(m.q, m.r), (totals.get(key(m.q, m.r)) ?? 0) + 1);
    for (const m of store.state().markers) {
      const k = key(m.q, m.r);
      const i = perCell.get(k) ?? 0;
      perCell.set(k, i + 1);
      const total = totals.get(k) ?? 1;
      const obj = buildMarker(m.kind, m.color);
      const { x, z } = axialToWorld(m.q, m.r);
      const spread = total > 1 ? 0.3 : 0;
      const a = (i / total) * Math.PI * 2;
      obj.position.set(x + Math.cos(a) * spread, store.heightAt(m.q, m.r) + 0.02, z + Math.sin(a) * spread);
      this.markersGroup.add(obj);
    }
    this.dirty = true;
  }

  private updateGhost(): void {
    this.ghostGroup.clear();
    this.dirty = true;
    const h = this.hover;
    if (!h) return;
    const store = this.store;
    const mode = store.mode();
    if (mode === 'peca') {
      const ok = store.canPlace(store.tileId(), h.q, h.r, store.rotation());
      for (const c of store.cellsOf(store.tileId(), h.q, h.r, store.rotation())) {
        this.ghostGroup.add(buildGhostCell(c.q, c.r, ok ? ghostOk : ghostBad));
      }
    } else {
      const occupied = store.occupied().has(key(h.q, h.r));
      const hasMarker = store.state().markers.some((m) => m.q === h.q && m.r === h.r);
      const ok = mode === 'marcador' || occupied || (mode === 'apagar' && hasMarker);
      this.ghostGroup.add(buildGhostCell(h.q, h.r, mode === 'apagar' ? ghostBad : ok ? ghostOk : ghostBad));
    }
  }

  private cellAt(e: PointerEvent): { q: number; r: number } | null {
    const rect = this.canvasRef().nativeElement.getBoundingClientRect();
    const ndc = new THREE.Vector2(
      ((e.clientX - rect.left) / rect.width) * 2 - 1,
      -((e.clientY - rect.top) / rect.height) * 2 + 1,
    );
    this.raycaster.setFromCamera(ndc, this.camera);
    const p = this.raycaster.ray.intersectPlane(this.ground, new THREE.Vector3());
    if (!p) return null;
    const c = worldToAxial(p.x, p.z);
    return Math.max(Math.abs(c.q), Math.abs(c.r), Math.abs(c.q + c.r)) <= GRID_RADIUS ? c : null;
  }

  private readonly onDown = (e: PointerEvent) => {
    if (e.button === 0) this.down = { x: e.clientX, y: e.clientY, t: performance.now() };
  };

  private readonly onUp = (e: PointerEvent) => {
    const d = this.down;
    this.down = null;
    if (!d) return;
    // Só conta como toque se o ponteiro quase não se moveu (senão foi arrasto de câmera).
    if (Math.hypot(e.clientX - d.x, e.clientY - d.y) > 8 || performance.now() - d.t > 600) return;
    const c = this.cellAt(e);
    if (c) this.store.applyAt(c.q, c.r);
  };

  private readonly onMove = (e: PointerEvent) => {
    if (e.pointerType !== 'mouse') return;
    const c = this.cellAt(e);
    if (c?.q === this.hover?.q && c?.r === this.hover?.r) return;
    this.hover = c;
    this.updateGhost();
  };

  private readonly onLeave = () => {
    this.hover = null;
    this.updateGhost();
  };
}
