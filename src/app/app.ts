import { Component, HostListener, computed, inject, signal } from '@angular/core';
import { SQRT3, axialToWorld } from './core/hex';
import { MapStore } from './core/map-store';
import { HERO_COLORS, MARKERS, MarkerKind, Mode, TERRAINS, Terrain, TileDef } from './core/models';
import { TILE_LIBRARY } from './core/tile-library';
import { MapScene } from './scene/map-scene';

interface Thumb {
  def: TileDef;
  viewBox: string;
  polys: string[];
}

function thumb(def: TileDef): Thumb {
  const centers = def.cells.map((c) => axialToWorld(c.q, c.r));
  const xs = centers.map((c) => c.x);
  const zs = centers.map((c) => c.z);
  const minX = Math.min(...xs) - SQRT3 / 2 - 0.15;
  const minZ = Math.min(...zs) - 1.15;
  const w = Math.max(...xs) + SQRT3 / 2 + 0.15 - minX;
  const h = Math.max(...zs) + 1.15 - minZ;
  const polys = centers.map(({ x, z }) =>
    Array.from({ length: 6 }, (_, i) => {
      const a = Math.PI / 6 + (i * Math.PI) / 3;
      return `${(x + Math.cos(a)).toFixed(2)},${(z + Math.sin(a)).toFixed(2)}`;
    }).join(' '),
  );
  return { def, viewBox: `${minX} ${minZ} ${w} ${h}`, polys };
}

@Component({
  selector: 'app-root',
  imports: [MapScene],
  templateUrl: './app.html',
  styleUrl: './app.css',
})
export class App {
  protected readonly store = inject(MapStore);

  protected readonly modes: { id: Mode; label: string }[] = [
    { id: 'peca', label: 'Peças' },
    { id: 'terreno', label: 'Terreno' },
    { id: 'marcador', label: 'Marcadores' },
    { id: 'apagar', label: 'Apagar' },
  ];
  protected readonly terrains = (Object.keys(TERRAINS) as Terrain[]).map((id) => ({ id, ...TERRAINS[id] }));
  protected readonly markers = (Object.keys(MARKERS) as MarkerKind[]).map((id) => ({ id, ...MARKERS[id] }));
  protected readonly heroColors = HERO_COLORS;

  protected readonly side = signal<'A' | 'B'>('A');
  private readonly thumbs = TILE_LIBRARY.map(thumb);
  protected readonly visibleThumbs = computed(() =>
    this.thumbs.filter((t) => t.def.side === 'avulso' || t.def.side === this.side()),
  );

  protected readonly menuOpen = signal(false);
  protected readonly notice = signal('');

  protected readonly hint = computed(() => {
    switch (this.store.mode()) {
      case 'peca':
        return 'Toque no mapa para encaixar a peça. Gire antes de colocar.';
      case 'terreno':
        return 'Toque num hexágono de uma peça para pintar o terreno.';
      case 'marcador':
        return 'Toque num hexágono para colocar o marcador.';
      case 'apagar':
        return 'Toque para remover: primeiro o marcador, depois a peça.';
    }
  });

  @HostListener('window:keydown', ['$event'])
  protected onKey(e: KeyboardEvent): void {
    if ((e.target as HTMLElement)?.tagName === 'INPUT') return;
    if (e.key === 'r' || e.key === 'R') this.store.rotateTool(e.shiftKey ? -1 : 1);
    else if ((e.ctrlKey || e.metaKey) && e.key === 'z') this.store.undo();
  }

  protected exportMap(): void {
    const url = URL.createObjectURL(new Blob([this.store.exportJson()], { type: 'application/json' }));
    const a = document.createElement('a');
    a.href = url;
    a.download = 'mapa.json';
    a.click();
    URL.revokeObjectURL(url);
    this.menuOpen.set(false);
  }

  protected async importMap(input: HTMLInputElement): Promise<void> {
    const file = input.files?.[0];
    input.value = '';
    if (!file) return;
    const ok = this.store.importJson(await file.text());
    this.flash(ok ? 'Mapa carregado.' : 'Arquivo inválido: não é um mapa deste editor.');
    this.menuOpen.set(false);
  }

  protected clearMap(): void {
    this.store.clear();
    this.flash('Mapa limpo. Use Desfazer para recuperar.');
    this.menuOpen.set(false);
  }

  private flash(msg: string): void {
    this.notice.set(msg);
    setTimeout(() => this.notice.set(''), 3000);
  }
}
