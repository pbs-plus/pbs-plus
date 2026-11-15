const fs = require("node:fs");
const vm = require("node:vm");
const assert = require("node:assert/strict");
let definition;
let windowConfig;
let request;
let previewText;
const formValues = {
  datastore: "archive",
  ns: "Media",
  mode: "ro",
  outpost: "export",
  "share-name": "collection",
  "layout-template": "{resolve}/{ns.leaf}",
  "layout-selection": "namespace",
  "layout-resolver": "https://resolver.example/paths",
  "layout-token-env": "LAYOUT_TOKEN",
  "layout-fallback": "Other/{ns.leaf}",
  "layout-windows": "1",
};
const fieldset = { collapsed: false };
const form = {
  getValues: () => formValues,
  isValid: () => true,
  down: (id) =>
    id === "#pathLayout"
      ? fieldset
      : {
          setValue: (v) => {
            previewText = v;
          },
          show: () => {},
        },
};
const win = {
  down: () => form,
  show: () => {},
  close: () => {},
  destroyed: false,
};
const encode = (s) =>
  s.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
const ctx = {
  gettext: (s) => s,
  pbsPlusBaseUrl: "",
  encodePathValue: (s) => s,
  console,
  Ext: {
    define: (_name, config) => {
      definition = config;
    },
    create: (_name, config) => {
      windowConfig = config;
      return win;
    },
    String: { htmlEncode: encode, format: (s) => s },
    Msg: {
      alert: (_title, text) => {
        throw new Error(text);
      },
    },
  },
  PBS: {
    PlusUtils: {
      API2Request: (config) => {
        request = config;
      },
    },
  },
};
vm.runInNewContext(fs.readFileSync(process.argv[2], "utf8"), ctx);
const controller = definition.controller;
assert.equal(
  JSON.parse(controller.profileParams(form).layout).selection,
  "namespace",
);
assert.equal(
  JSON.parse(controller.profileParams(form).layout).resolver_token_env,
  "LAYOUT_TOKEN",
);
fieldset.collapsed = true;
assert.equal(controller.profileParams(form).layout, "");
fieldset.collapsed = false;
controller.openEdit({}, null);
const buttons = windowConfig.buttons;
const preview = buttons.find((b) => b.text === "Preview paths");
preview.handler({ up: () => win });
assert.equal(request.url, "/api2/extjs/config/d2d-mount-profiles/preview");
assert.equal(request.method, "POST");
const previewParams = request.params;
request.success({
  result: { data: [{ paths: ["<unsafe>/D-100"], backup_time: "2026-01-01" }] },
});
assert.ok(previewText.includes("&lt;unsafe&gt;"));
assert.ok(!previewText.includes("<unsafe>"));
buttons.find((b) => b.text === "Create").handler({ up: () => win });
assert.equal(JSON.stringify(request.params), JSON.stringify(previewParams));
assert.equal(request.url, "/api2/extjs/config/d2d-mount-profiles");
