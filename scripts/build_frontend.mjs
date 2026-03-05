import fs from 'node:fs';
import path from 'node:path';

const root = process.cwd();
const frontendDir = path.join(root, 'frontend');
const distDir = path.join(root, 'dist');
const distStaticDir = path.join(distDir, 'static');

const apiBaseUrl = String(process.env.PUBLIC_API_BASE_URL || process.env.API_BASE_URL || '').replace(/\/+$/, '');

fs.rmSync(distDir, { recursive: true, force: true });
fs.mkdirSync(distStaticDir, { recursive: true });

const copy = (from, to) => fs.copyFileSync(path.join(frontendDir, from), path.join(to));

copy('index.html', path.join(distDir, 'index.html'));
copy('app.js', path.join(distStaticDir, 'app.js'));
copy('styles.css', path.join(distStaticDir, 'styles.css'));

const configJs = `window.__APP_CONFIG__ = Object.assign({}, window.__APP_CONFIG__ || {}, { API_BASE_URL: ${JSON.stringify(apiBaseUrl)} });\n`;
fs.writeFileSync(path.join(distStaticDir, 'config.js'), configJs, 'utf8');

console.log(`Built frontend into ${distDir}`);
console.log(`PUBLIC_API_BASE_URL=${apiBaseUrl || '(same-origin)'}`);
