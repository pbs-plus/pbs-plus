const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
let viewportHeight = 800;
let editors = [];
const callbacks = [];
const sandbox = {
  Ext: {
    GlobalEvents: {},
    Element: { getViewportHeight: () => viewportHeight },
  },
  PBS: {
    PlusUtils: { LoadCodeMirror: (callback) => callbacks.push(callback) },
  },
  Proxmox: { Utils: { theme: "dark" } },
  window: {},
  gettext: (value) => value,
  CodeMirror: {
    fromTextArea(textarea, options) {
      const editor = {
        value: textarea.value,
        options,
        events: {},
        on(name, callback) {
          this.events[name] = callback;
        },
        getValue() {
          return this.value;
        },
        setValue(value) {
          this.value = value;
          this.events.change?.();
        },
        setSize(width, height) {
          this.size = [width, height];
        },
        refresh() {
          this.refreshed = true;
        },
        toTextArea() {
          this.disposed = true;
        },
        wrapper: { style: {}, title: "" },
        getWrapperElement() {
          return this.wrapper;
        },
      };
      editors.push(editor);
      return editor;
    },
  },
};
const attach = vm.runInNewContext(
  "(" + fs.readFileSync(process.argv[2], "utf8") + ")",
  sandbox,
);
function field(value) {
  return {
    value,
    originalValue: value,
    events: {},
    inputEl: {
      dom: {
        value,
        style: {},
        setAttribute(name, value) {
          this[name] = value;
        },
      },
    },
    getValue() {
      return this.value;
    },
    setValue(value) {
      if (this.value === value) return;
      this.value = value;
      this.inputEl.dom.value = value;
      this.events.change?.(this, value);
    },
    isDirty() {
      return this.value !== this.originalValue;
    },
    isValid() {
      return this.value !== "";
    },
    setHeight(value) {
      this.height = value;
    },
    updateLayout() {},
    on(name, callback) {
      this.events[name] = callback;
    },
    mon(target, name, callback) {
      this.viewportResize = callback;
    },
  };
}
const component = field("#!/bin/sh\necho before\n");
attach(component);
assert.equal(component.height, 400);
assert.equal(component.inputEl.dom.spellcheck, false);
assert.equal(component.inputEl.dom["aria-label"], "Script Content");
component.setValue("#!/bin/sh\necho typed while loading\n");
assert.equal(component.isDirty(), true);
callbacks.shift()();
const editor = component.codeMirror;
assert.equal(editor.getValue(), component.getValue());
assert.equal(editor.options.lineWrapping, true);
assert.equal(editor.options.mode, "shell");
assert.equal(editor.options.theme, "material-darker");
assert.equal(editor.options.screenReaderLabel, "Script Content");
editor.setValue("#!/bin/sh\necho changed\n");
assert.equal(component.getValue(), editor.getValue());
assert.equal(component.isDirty(), true);
component.setValue(component.originalValue);
assert.equal(component.isDirty(), false);
assert.equal(editor.getValue(), component.originalValue);
assert.equal(editor.wrapper.style.outline, "");
component.setValue("");
assert.equal(editor.getValue(), "");
component.events.validitychange(component, component.isValid());
assert.equal(editor.wrapper.style.outline, "1px solid #cf4c35");
assert.equal(editor.wrapper.title, "This field is required");
component.setValue("#!/bin/sh\n");
component.events.validitychange(component, component.isValid());
assert.equal(editor.wrapper.style.outline, "");
assert.equal(editor.wrapper.title, "");
component.setValue("");
viewportHeight = 360;
component.viewportResize();
assert.equal(component.height, 160);
assert.equal(editor.size[1], 160);
component.events.beforedestroy();
assert.equal(editor.disposed, true);
assert.equal(component.codeMirror, null);
const disposed = field("unchanged");
attach(disposed);
disposed.destroyed = true;
const count = editors.length;
callbacks.shift()();
assert.equal(editors.length, count);
assert.equal(disposed.getValue(), "unchanged");

const utilsSource = fs.readFileSync(process.argv[3], "utf8");
function loader() {
  const definitions = {};
  const assets = [];
  const window = {};
  vm.runInNewContext(utilsSource, {
    Ext: {
      define: (name, config) => {
        definitions[name] = config;
      },
    },
    window,
    document: {
      createElement: () => ({}),
      head: { appendChild: (asset) => assets.push(asset) },
    },
    gettext: (value) => value,
    console: { warn() {} },
    Promise,
  });
  return { utils: definitions["PBS.PlusUtils"], assets, window };
}
(async () => {
  const loaded = loader();
  let first = 0;
  let second = 0;
  loaded.utils.LoadCodeMirror(() => first++);
  assert.equal(loaded.assets.length, 3);
  loaded.window.CodeMirror = {};
  loaded.utils.LoadCodeMirror(() => second++);
  assert.equal(loaded.assets.length, 3);
  assert.equal(first, 0);
  assert.equal(second, 0);
  loaded.assets.slice().forEach((asset) => asset.onload());
  await new Promise(setImmediate);
  assert.equal(loaded.assets.length, 4);
  loaded.assets[3].onload();
  await new Promise(setImmediate);
  assert.equal(first, 1);
  assert.equal(second, 1);
  const failed = loader();
  let enhanced = false;
  failed.utils.LoadCodeMirror(() => {
    enhanced = true;
  });
  failed.assets[0].onerror();
  await new Promise(setImmediate);
  assert.equal(enhanced, false);
  console.log(
    "Code editor synchronization, reset, sizing, cleanup and loader checks passed",
  );
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
