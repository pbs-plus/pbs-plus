const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const definitions = {};
let viewport = { width: 800, height: 600 };
const Ext = {
  define: (name, config) => {
    definitions[name] = config;
  },
  Element: {
    getViewportWidth: () => viewport.width,
    getViewportHeight: () => viewport.height,
  },
  GlobalEvents: {},
};
vm.runInNewContext(fs.readFileSync(process.argv[2], "utf8"), { Ext });
const plugin = definitions["PBS.plugin.Responsive"];
assert.equal(definitions["PBS.plusWindow.Window"].extend, "Ext.window.Window");
assert.equal(
  definitions["PBS.plusWindow.Window"].plugins[0],
  "pbsPlusResponsive",
);

function component(type, children = []) {
  return {
    isContainer: children.length > 0,
    events: {},
    isXType: (name) => type === name,
    on(name, fn) {
      this.events[name] = fn;
    },
    getRefItems: () => children,
  };
}

const toolbar = component("toolbar");
let overflow;
toolbar.getLayout = () => ({
  setOverflowHandler: (value) => {
    overflow = value;
  },
});
const root = component("panel", [toolbar]);
plugin.prepare(root);
assert.equal(overflow, "scroller");
const added = component("toolbar");
added.getLayout = toolbar.getLayout;
root.events.add(root, added);
assert.equal(added.pbsPlusResponsiveReady, true);
const docked = component("toolbar");
docked.getLayout = toolbar.getLayout;
root.events.dockedadd(root, docked);
assert.equal(docked.pbsPlusResponsiveReady, true);
const untouched = component("toolbar");
assert.equal(untouched.pbsPlusResponsiveReady, undefined);
plugin.prepare(root);

const column = component("gridcolumn");
Object.assign(column, { isColumn: true, flex: 1, initialConfig: {} });
plugin.prepare(column);
assert.equal(column.minWidth, 100);
const explicitColumn = component("gridcolumn");
Object.assign(explicitColumn, {
  isColumn: true,
  flex: 1,
  minWidth: 75,
  initialConfig: { minWidth: 75 },
});
plugin.prepare(explicitColumn);
assert.equal(explicitColumn.minWidth, 75);

function makeWindow(extra = {}) {
  return Object.assign(
    {
      width: 960,
      height: 640,
      minWidth: 500,
      minHeight: 400,
      events: {},
      mon(target, name, fn) {
        this.events[name] = fn;
      },
      on(name, fn) {
        this.events[name] = fn;
      },
      setSize(width, height) {
        if (width !== undefined) this.width = width;
        if (height !== undefined) this.height = height;
      },
      setScrollable(value) {
        this.scrollable = value;
      },
      query: () => [],
      updateLayout() {},
      doConstrain() {
        this.constrained = true;
      },
    },
    extra,
  );
}
const win = makeWindow();
plugin.window(win);
assert.equal(win.width, 784);
assert.equal(win.height, 584);
assert.equal(win.constrain, true);
win.rendered = true;
viewport = { width: 360, height: 240 };
win.events.resize();
assert.equal(win.width, 344);
assert.equal(win.height, 224);
assert.equal(win.minWidth, 344);
assert.equal(win.minHeight, 224);
assert.equal(win.constrained, true);
viewport = { width: 1600, height: 1000 };
win.events.resize();
assert.equal(win.width, 960);
assert.equal(win.height, 640);
assert.equal(win.minWidth, 500);
assert.equal(win.minHeight, 400);
win.resizer = {};
win.events.afterrender();
win.events.resize(win.resizer, 700, 500);
win.events.beforeshow();
assert.equal(win.width, 700);
assert.equal(win.height, 500);
win.destroyed = true;
viewport = { width: 320, height: 200 };
win.events.beforeshow();
assert.equal(win.width, 700);

let formScrollable;
let formLayout;
const edit = makeWindow({
  height: undefined,
  formPanel: {
    setScrollable: (value) => {
      formScrollable = value;
    },
  },
  setLayout: (value) => {
    formLayout = value;
  },
});
plugin.window(edit);
assert.equal(formLayout, "fit");
assert.equal(formScrollable, true);
assert.equal(edit.height, undefined);
const form = {
  setScrollable: (value) => {
    formScrollable = value;
  },
};
formScrollable = undefined;
plugin.window(makeWindow({ query: () => [form] }));
assert.equal(formScrollable, true);

const columns = Array.from({ length: 2 }, () => ({
  flex: 1,
  padding: 10,
  setMargin(value) {
    this.margin = value;
  },
}));
let rowWidth = 800;
let layout;
const row = component("container");
row.items = { items: columns };
row.setLayout = (value) => {
  layout = value;
};
row.getWidth = () => {
  assert.equal(row.rendered, true);
  return rowWidth;
};
row.updateLayout = () => {};
viewport = { width: 800, height: 600 };
plugin.columns(row);
assert.equal(layout, "column");
assert.equal(columns[0].columnWidth, 0.5);
assert.equal(columns[0].flex, undefined);
assert.equal(columns[0].padding, 0);
row.rendered = true;
rowWidth = 440;
row.events.resize();
assert.equal(columns[0].columnWidth, 1);
assert.equal(columns[1].columnWidth, 1);
rowWidth = 800;
row.events.resize();
assert.equal(columns[0].columnWidth, 0.5);
assert.equal(columns[1].columnWidth, 0.5);

const combo = component("combobox");
let pickerWidth = 720;
let pickerHeight = 300;
let aligned = false;
const picker = {
  minWidth: 600,
  minHeight: 200,
  initialConfig: { width: 720, height: 300 },
  getWidth: () => pickerWidth,
  getHeight: () => pickerHeight,
  setSize: (width, height) => {
    pickerWidth = width;
    pickerHeight = height;
  },
};
combo.el = { dom: { getBoundingClientRect: () => ({ top: 80, bottom: 110 }) } };
combo.getPicker = () => picker;
combo.alignPicker = () => {
  aligned = true;
};
viewport = { width: 360, height: 240 };
plugin.prepare(combo);
combo.events.expand(combo);
assert.equal(pickerWidth, 344);
assert.equal(picker.minWidth, 344);
assert.equal(picker.maxWidth, 344);
assert.equal(pickerHeight, 122);
assert.equal(picker.minHeight, 122);
assert.equal(aligned, true);
viewport = { width: 1280, height: 800 };
combo.events.expand(combo);
assert.equal(pickerWidth, 720);
assert.equal(pickerHeight, 300);
assert.equal(picker.minWidth, 600);
assert.equal(picker.minHeight, 200);
console.log("Responsive runtime checks passed");
