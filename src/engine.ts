// Loads the WebAssembly engine and recovers from traps.
//
// All sessions share one WebAssembly instance. If that instance traps (the
// heap limit is reached or Rust panics), its memory can no longer be trusted.
// The engine is then marked crashed, every query running on it fails with
// EngineCrashedError, and the next query loads a new instance from a fresh
// copy of the glue module.

import type * as Glue from './wasm/datafusion_vtab.js';

export type GlueModule = typeof Glue;

/** Thrown when the WebAssembly instance traps. The engine restarts on the next query. */
export class EngineCrashedError extends Error {
  override readonly name = 'EngineCrashedError';
}

export interface InitOptions {
  /**
   * The compiled engine, its bytes, or where to fetch them. Defaults to the
   * `.wasm` file shipped next to this module, read from disk in Node and
   * fetched elsewhere.
   */
  readonly module?: WebAssembly.Module | BufferSource | URL | string;
  /**
   * Maximum bytes the engine may hold on its WebAssembly heap, across all
   * sessions. Reaching it crashes the engine (see EngineCrashedError).
   * Default: 1 GiB.
   */
  readonly maxHeapBytes?: number;
}

export const DEFAULT_MAX_HEAP_BYTES = 1024 * 1024 * 1024;

export class Engine {
  crashed: EngineCrashedError | undefined;
  private readonly crashListeners = new Set<(error: EngineCrashedError) => void>();

  constructor(
    readonly glue: GlueModule,
    readonly generation: number,
  ) {}

  /** Runs `fn`, converting a WebAssembly trap into EngineCrashedError. */
  call<T>(fn: () => T): T {
    if (this.crashed) throw this.crashed;
    try {
      return fn();
    } catch (error) {
      if (error instanceof WebAssembly.RuntimeError) throw this.crash(error);
      throw error;
    }
  }

  /** Calls `listener` if the engine crashes. Returns a function that removes it. */
  onCrash(listener: (error: EngineCrashedError) => void): () => void {
    this.crashListeners.add(listener);
    return () => this.crashListeners.delete(listener);
  }

  private crash(trap: WebAssembly.RuntimeError): EngineCrashedError {
    if (this.crashed) return this.crashed;
    let reason = `WebAssembly trap: ${trap.message}`;
    try {
      const failed = this.glue.heapLimitFailure();
      if (failed > 0) {
        const limit = this.glue.heapStats()[2];
        reason = `heap limit of ${limit} bytes reached while allocating ${failed} bytes`;
      } else {
        const panic = this.glue.takePanicMessage();
        if (panic) reason = `DataFusion panicked: ${panic}`;
      }
    } catch {
      // The instance is unusable; keep the trap message.
    }
    this.crashed = new EngineCrashedError(`${reason}. The engine will restart on the next query.`, {
      cause: trap,
    });
    if (current === this) current = undefined;
    try {
      this.glue.__wbg_dispose();
    } catch {}
    for (const listener of this.crashListeners) listener(this.crashed);
    this.crashListeners.clear();
    return this.crashed;
  }
}

let options: InitOptions = {};
let compiled: Promise<WebAssembly.Module> | undefined;
let current: Engine | undefined;
let loading: Promise<Engine> | undefined;
let generation = 0;

const wasmUrl = new URL('./wasm/datafusion_vtab_bg.wasm', import.meta.url);

async function compile(source: InitOptions['module']): Promise<WebAssembly.Module> {
  if (source instanceof WebAssembly.Module) return source;
  if (source !== undefined && !(source instanceof URL) && typeof source !== 'string') {
    return WebAssembly.compile(source);
  }
  const url = source === undefined ? wasmUrl : new URL(source, import.meta.url);
  if (url.protocol === 'file:') {
    const fs = (globalThis as { process?: NodeJS.Process }).process?.getBuiltinModule?.(
      'node:fs/promises',
    ) as typeof import('node:fs/promises') | undefined;
    if (!fs) throw new Error('Reading the engine from a file: URL requires Node.js 20.16 or later');
    return WebAssembly.compile(await fs.readFile(url));
  }
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch ${url}: ${response.status}`);
  return WebAssembly.compile(await response.arrayBuffer());
}

async function load(): Promise<Engine> {
  compiled ??= compile(options.module);
  const module = await compiled;
  const gen = generation++;
  // A new URL gives a new module instance with its own `wasm` binding.
  const glue: GlueModule =
    gen === 0
      ? await import('./wasm/datafusion_vtab.js')
      : await import(`./wasm/datafusion_vtab.js?generation=${gen}`);
  glue.initSync({ module });
  glue.setHeapLimit(options.maxHeapBytes ?? DEFAULT_MAX_HEAP_BYTES);
  return new Engine(glue, gen);
}

/** Returns the running engine, starting one if needed. */
export async function engine(): Promise<Engine> {
  if (current) return current;
  loading ??= load().then(
    (e) => {
      current = e;
      loading = undefined;
      return e;
    },
    (error: unknown) => {
      loading = undefined;
      compiled = undefined;
      throw error;
    },
  );
  return loading;
}

/**
 * Configures and starts the engine. Optional: queries start it on first use
 * with default options. Options apply to engines started after this call.
 */
export async function init(initOptions: InitOptions = {}): Promise<void> {
  if (initOptions.module !== undefined && initOptions.module !== options.module) compiled = undefined;
  options = { ...options, ...initOptions };
  const e = await engine();
  if (initOptions.maxHeapBytes !== undefined) e.call(() => e.glue.setHeapLimit(initOptions.maxHeapBytes!));
}

export interface HeapStats {
  /** Bytes currently allocated on the WebAssembly heap. */
  readonly used: number;
  /** Highest `used` value since the engine started. */
  readonly peak: number;
  /** The heap limit in bytes, or null for none. */
  readonly limit: number | null;
}

/** Heap usage of the running engine, or null if no engine is running. */
export function heapStats(): HeapStats | null {
  const e = current;
  if (!e) return null;
  const [used, peak, limit] = e.call(() => e.glue.heapStats());
  return { used: used!, peak: peak!, limit: limit! < 0 ? null : limit! };
}

/** Drives a native task to completion from JavaScript. See crate/src/task.rs. */
export async function drive(e: Engine, task: Glue.Task): Promise<unknown> {
  let rejectCrash!: (error: EngineCrashedError) => void;
  const crashed = new Promise<never>((_, reject) => (rejectCrash = reject));
  crashed.catch(() => {});
  const unsubscribe = e.onCrash(rejectCrash);
  try {
    for (;;) {
      let wake!: () => void;
      const woken = new Promise<void>((resolve) => (wake = resolve));
      if (e.call(() => task.poll(wake))) return e.call(() => task.takeResult());
      await Promise.race([woken, crashed]);
    }
  } finally {
    unsubscribe();
    if (!e.crashed) e.call(() => task.free());
  }
}
