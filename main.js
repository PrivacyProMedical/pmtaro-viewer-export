import os from 'node:os';
import child_process from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

import { createApiLoader } from '../../../utils/apiLoader.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const platform = process.platform || os.platform();
const isMac = platform === 'darwin';
// const isWin = platform === 'win32';

const apiDir = resolve(__dirname, 'api');
const apiLoader = createApiLoader(apiDir, 'index.js');
const apiReady = apiLoader.load();

const contextMenus = [
  {
    slots: ['patient', 'study', 'series', 'instance'],
    label: 'Export',
    click: async (ctx, ...args) => {},
    ui: true,
  },
  /*
  isMac && {
    slots: ['folder', 'file'],
    label: 'Export & Anonymize'.replace(/&/g, '&&'),
    submenu: [
      {
        slots: ['folder'],
        label: 'Copy Name',
        click: async (ctx, ...args) => {
          const data = ctx?.selection?.name ?? '';
          if (data) {
            pbcopy(data);
            return data;
          }
        },
        ui: false,
      },
      {
        slots: ['file'],
        label: 'Copy Path',
        click: async (ctx, ...args) => {
          const data = ctx?.selection?.path ?? '';
          if (data) {
            pbcopy(data);
            return data;
          }
        },
        ui: false,
      },
    ],
  },
  */
].filter(Boolean);

export default {
  meta: {
    name: 'Export & Anonymize',
    version: '1.0.0',
    // ...
  },
  async setup(ctx = {/* __file__, __name__, __author__, __version__ */}, electronApp) {
    return electronApp.whenReady().then(() => {
      // ...
    });
  },
  ui: {
    entry: 'ui/index.html',
    windowOptions: {
      width: 720,
      height: 640,
      minWidth: 400,
      minHeight: 300,
    },
  },
  api: {
    test: (...args) => ({ args }),

    exportParsedStandardDirectory: async (...args) => apiReady.then(api => apiLoader.run(api.exportParsedStandardDirectory)(...args)),
    // deidentify2DDicom: async (...args) => apiReady.then(api => apiLoader.run(api.deidentify2DDicom)(...args)),

    // ...

    // ipcRenderer.invoke(`@pmt/export/@contextmenu`, ...
    '@contextmenu': (indexes, ctx, ...args) => {
      let item = null;
      for (let i = 0; i < indexes.length; i++) {
        const index = indexes[i];
        item = i === 0 ? contextMenus[index] : item.submenu?.[index];
      }
      if (item && typeof item.click === 'function') {
        return item.click(ctx, ...args);
      }
    },
  },
  contextMenus,
};

function pbcopy(data) {
  if (!isMac) {
    return;
  }
  try {
    const p = child_process.spawn('pbcopy');
    p.stdin.write(data);
    p.stdin.end();
  } catch (err) {}
}
