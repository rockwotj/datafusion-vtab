// Builds the Rust crate to WebAssembly and writes the bindings to src/wasm/.
//
// Requires: the wasm32-unknown-unknown Rust target, wasm-bindgen-cli at the
// version pinned in crate/Cargo.toml, and an llvm-ar that can index wasm
// object files (zstd-sys compiles C code). Set WASM_OPT=0 to skip wasm-opt.

import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const crate = join(root, 'crate');
const out = join(root, 'src', 'wasm');
const name = 'datafusion_vtab';

function run(cmd, args, options = {}) {
  console.log(`> ${cmd} ${args.join(' ')}`);
  return execFileSync(cmd, args, { stdio: 'inherit', ...options });
}

function capture(cmd, args) {
  return execFileSync(cmd, args, { encoding: 'utf8' }).trim();
}

function findLlvmAr() {
  for (const candidate of ['llvm-ar', ...Array.from({ length: 12 }, (_, i) => `llvm-ar-${22 - i}`)]) {
    try {
      capture(candidate, ['--version']);
      return candidate;
    } catch {}
  }
  const sysroot = capture('rustc', ['--print', 'sysroot']);
  const host = capture('rustc', ['-vV'])
    .split('\n')
    .find((line) => line.startsWith('host: '))
    .slice('host: '.length);
  const bundled = join(sysroot, 'lib', 'rustlib', host, 'bin', 'llvm-ar');
  if (existsSync(bundled)) return bundled;
  throw new Error('llvm-ar not found. Install LLVM or run `rustup component add llvm-tools`.');
}

const env = { ...process.env };
env.AR_wasm32_unknown_unknown ??= findLlvmAr();

run('cargo', ['build', '--release', '--target', 'wasm32-unknown-unknown'], { cwd: crate, env });

const lock = readFileSync(join(crate, 'Cargo.lock'), 'utf8');
const pinned = /name = "wasm-bindgen"\nversion = "([^"]+)"/.exec(lock)?.[1];
const installed = capture('wasm-bindgen', ['--version']).split(' ')[1];
if (pinned !== installed) {
  throw new Error(
    `wasm-bindgen-cli ${installed} does not match the crate's wasm-bindgen ${pinned}. ` +
      `Run: cargo install wasm-bindgen-cli --version ${pinned} --locked`,
  );
}

rmSync(out, { recursive: true, force: true });
mkdirSync(out, { recursive: true });
run('wasm-bindgen', [
  '--target',
  'web',
  '--out-dir',
  out,
  join(crate, 'target', 'wasm32-unknown-unknown', 'release', `${name}.wasm`),
]);

// Each engine generation imports its own copy of the glue module. After a
// trap, `__wbg_dispose` points the dead copy's `wasm` binding at stubs, so
// late finalizers and promise callbacks do nothing and the old instance's
// memory can be collected.
const gluePath = join(out, `${name}.js`);
const glue = readFileSync(gluePath, 'utf8');
if (!glue.includes('let wasmModule, wasmInstance, wasm;')) {
  throw new Error('wasm-bindgen glue layout changed; update the dispose patch in scripts/build-wasm.mjs');
}
writeFileSync(
  gluePath,
  glue +
    `
export function __wbg_dispose() {
    wasm = new Proxy({}, { get: () => () => 0 });
    wasmInstance = undefined;
    wasmModule = undefined;
}
`,
);
writeFileSync(
  join(out, `${name}.d.ts`),
  readFileSync(join(out, `${name}.d.ts`), 'utf8') + '\nexport function __wbg_dispose(): void;\n',
);

const wasmPath = join(out, `${name}_bg.wasm`);
if (process.env.WASM_OPT !== '0') {
  const before = statSync(wasmPath).size;
  run(join(root, 'node_modules', '.bin', 'wasm-opt'), [
    '-Oz',
    '--enable-bulk-memory',
    '--enable-nontrapping-float-to-int',
    '--enable-sign-ext',
    '--enable-mutable-globals',
    '--enable-reference-types',
    '--enable-multivalue',
    '--strip-debug',
    '--strip-producers',
    wasmPath,
    '-o',
    wasmPath,
  ]);
  console.log(`wasm-opt: ${before} -> ${statSync(wasmPath).size} bytes`);
}
console.log(`${wasmPath}: ${statSync(wasmPath).size} bytes`);
